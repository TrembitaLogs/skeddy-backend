import contextlib
import logging
from uuid import UUID

from fastapi import HTTPException
from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import select, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.credit_balance import CreditBalance
from app.models.credit_transaction import CreditTransaction, TransactionType
from app.models.paired_device import PairedDevice
from app.services.cluster_service import remove_device_from_cluster
from app.services.config_service import (
    get_registration_bonus_credits,
    get_ride_credit_tiers,
)

logger = logging.getLogger(__name__)

BALANCE_CACHE_KEY = "user_balance:{user_id}"
BALANCE_CACHE_TTL = 300  # 5 minutes

LOW_BALANCE_NOTIFIED_KEY = "low_balance_notified:{user_id}"


def _balance_key(user_id: UUID) -> str:
    return BALANCE_CACHE_KEY.format(user_id=user_id)


def low_balance_notified_key(user_id: UUID) -> str:
    return LOW_BALANCE_NOTIFIED_KEY.format(user_id=user_id)


async def get_balance(user_id: UUID, db: AsyncSession, redis: Redis) -> int:
    """Return the current credit balance for a user.

    Resolution order: Redis cache -> DB -> 0.
    Redis failures are handled gracefully (falls back to DB).
    """
    cache_key = _balance_key(user_id)

    # 1. Try Redis cache
    try:
        cached = await redis.get(cache_key)
        if cached is not None:
            return int(cached)
    except RedisError:
        logger.warning("Redis unavailable when reading %s, falling back to DB", cache_key)

    # 2. Try DB
    result = await db.execute(
        select(CreditBalance.balance).where(CreditBalance.user_id == user_id)
    )
    balance = result.scalar_one_or_none()

    if balance is not None:
        # Cache the value for next time
        try:
            await redis.setex(cache_key, BALANCE_CACHE_TTL, str(balance))
        except RedisError:
            logger.warning("Redis unavailable when caching %s", cache_key)
        return balance

    # 3. No balance row found — return 0 without creating a record
    return 0


async def create_balance_with_bonus(
    user_id: UUID, db: AsyncSession, redis: Redis
) -> CreditBalance:
    """Create CreditBalance with registration bonus for a new user.

    Reads bonus from AppConfig 'registration_bonus_credits' (fallback: 10).
    Uses flush() only — caller must commit the outer transaction and
    call cache_balance() after successful commit for Redis write-through.
    """
    bonus_amount = await get_registration_bonus_credits(db, redis)

    credit_balance = CreditBalance(user_id=user_id, balance=bonus_amount)
    db.add(credit_balance)

    transaction = CreditTransaction(
        user_id=user_id,
        type=TransactionType.REGISTRATION_BONUS,
        amount=bonus_amount,
        balance_after=bonus_amount,
    )
    db.add(transaction)

    await db.flush()

    return credit_balance


async def charge_credits(
    user_id: UUID,
    amount: int,
    reference_id: UUID,
    db: AsyncSession,
    redis: Redis,
) -> tuple[int, int]:
    """Charge credits for a ride acceptance (hot path).

    Uses SELECT FOR UPDATE NOWAIT for minimal latency.
    Supports partial charging: charges min(amount, current_balance).
    If balance is 0, returns (0, 0) without creating a transaction.

    Uses flush() instead of commit() — the caller is responsible for
    committing the transaction and calling cache_balance() afterwards.
    This allows atomic commits with other operations (e.g. ride creation).

    Returns (charged_amount, new_balance).
    Raises HTTPException(503) on row lock conflict.
    """
    stmt = (
        select(CreditBalance).where(CreditBalance.user_id == user_id).with_for_update(nowait=True)
    )
    try:
        result = await db.execute(stmt)
    except OperationalError as e:
        if "could not obtain lock" in str(e):
            raise HTTPException(status_code=503, detail="Balance locked, retry later")
        raise

    balance_row = result.scalar_one_or_none()
    if balance_row is None:
        return (0, 0)

    current_balance = balance_row.balance
    charged_amount = min(amount, current_balance)
    if charged_amount == 0:
        return (0, current_balance)

    new_balance = current_balance - charged_amount
    balance_row.balance = new_balance

    db.add(
        CreditTransaction(
            user_id=user_id,
            type=TransactionType.RIDE_CHARGE,
            amount=-charged_amount,
            balance_after=new_balance,
            reference_id=reference_id,
        )
    )

    await db.flush()

    if new_balance <= 0:
        try:
            device_result = await db.execute(
                select(PairedDevice.device_id).where(PairedDevice.user_id == user_id)
            )
            device_id = device_result.scalar_one_or_none()
            if device_id:
                await remove_device_from_cluster(device_id, redis)
        except Exception:
            logger.warning(
                "Failed to remove device from cluster after balance depletion for user %s",
                user_id,
                exc_info=True,
            )

    return (charged_amount, new_balance)


async def cache_balance(user_id: UUID, balance: int, redis: Redis) -> None:
    """Write-through cache for user credit balance.

    Called after successful DB commit. On SET failure, deletes the key
    to prevent stale data (PRD section 7).
    """
    cache_key = _balance_key(user_id)
    try:
        await redis.setex(cache_key, BALANCE_CACHE_TTL, str(balance))
    except RedisError:
        logger.warning(
            "Redis SET failed for %s, deleting to prevent stale cache",
            cache_key,
        )
        with contextlib.suppress(RedisError):
            await redis.delete(cache_key)


async def get_ride_credit_cost(price: float, db: AsyncSession, redis: Redis) -> int:
    """Calculate the credit cost for a ride based on its price.

    Loads tier configuration from AppConfig (via config_service) and applies
    tier matching using ``RideCreditTiersConfig.get_credits_for_price()``.
    """
    tiers = await get_ride_credit_tiers(db, redis)
    return tiers.get_credits_for_price(price)


async def get_max_ride_credits(db: AsyncSession, redis: Redis) -> int:
    """Return the maximum credits value from ride_credit_tiers AppConfig.

    Used as threshold for low_balance_notified cleanup.
    """
    tiers = await get_ride_credit_tiers(db, redis)
    return max(t.credits for t in tiers.root)


async def add_credits(
    user_id: UUID,
    amount: int,
    tx_type: TransactionType,
    reference_id: UUID | None,
    db: AsyncSession,
    redis: Redis,
    description: str | None = None,
) -> int:
    """Add credits to user balance (purchases, admin adjustments, refunds).

    Uses SELECT FOR UPDATE with 5s statement timeout (not NOWAIT) —
    acceptable latency for infrequent operations like purchases and
    admin adjustments.

    Writes through to Redis cache after successful DB commit.
    Clears low_balance_notified flag when new balance reaches threshold.

    Returns new_balance.
    Raises HTTPException(503) on statement timeout.
    """
    await db.execute(text("SET LOCAL statement_timeout = '5s'"))

    stmt = select(CreditBalance).where(CreditBalance.user_id == user_id).with_for_update()

    try:
        result = await db.execute(stmt)
    except OperationalError as e:
        if "canceling statement due to statement timeout" in str(e):
            raise HTTPException(status_code=503, detail="Operation timed out, retry later")
        raise

    balance_row = result.scalar_one_or_none()
    if balance_row is None:
        raise ValueError(f"CreditBalance not found for user {user_id}")

    new_balance = balance_row.balance + amount
    balance_row.balance = new_balance

    db.add(
        CreditTransaction(
            user_id=user_id,
            type=tx_type,
            amount=amount,
            balance_after=new_balance,
            reference_id=reference_id,
            description=description,
        )
    )

    await db.commit()
    await cache_balance(user_id, new_balance, redis)

    # Clear low_balance_notified ONLY if new_balance >= threshold (PRD section 8)
    try:
        threshold = await get_max_ride_credits(db, redis)
        if new_balance >= threshold:
            await redis.delete(low_balance_notified_key(user_id))
    except (RedisError, OperationalError):
        logger.warning(
            "Failed to clear low_balance_notified for user %s",
            user_id,
            exc_info=True,
        )

    return new_balance


async def refund_credits(
    user_id: UUID,
    amount: int,
    reference_id: UUID,
    db: AsyncSession,
    redis: Redis,
) -> int:
    """Refund credits for a cancelled ride.

    Wrapper around add_credits with tx_type=RIDE_REFUND.

    Args:
        user_id: User to refund.
        amount: Credits to refund (must be positive).
        reference_id: Original ride ID (must not be None).
        db: Database session.
        redis: Redis client.

    Returns:
        New balance after refund.

    Raises:
        ValueError: If amount <= 0 or reference_id is None.
    """
    if amount <= 0:
        raise ValueError(f"Refund amount must be positive, got {amount}")
    if reference_id is None:
        raise ValueError("reference_id is required for refund")

    return await add_credits(
        user_id=user_id,
        amount=amount,
        tx_type=TransactionType.RIDE_REFUND,
        reference_id=reference_id,
        db=db,
        redis=redis,
    )


async def refund_credits_in_txn(
    user_id: UUID,
    amount: int,
    reference_id: UUID,
    db: AsyncSession,
) -> int:
    """Refund credits within an existing transaction (flush only, no commit).

    Uses SELECT FOR UPDATE NOWAIT — suitable for ping handler hot path.
    Caller must commit the transaction and call cache_balance() afterwards
    for Redis write-through.

    Args:
        user_id: User to refund.
        amount: Credits to refund (must be positive).
        reference_id: Original ride ID.
        db: Database session (caller controls commit).

    Returns:
        New balance after refund.

    Raises:
        ValueError: If amount <= 0 or CreditBalance not found.
        OperationalError: If row lock cannot be obtained (NOWAIT).
    """
    if amount <= 0:
        raise ValueError(f"Refund amount must be positive, got {amount}")

    stmt = (
        select(CreditBalance).where(CreditBalance.user_id == user_id).with_for_update(nowait=True)
    )
    result = await db.execute(stmt)
    balance_row = result.scalar_one_or_none()

    if balance_row is None:
        raise ValueError(f"CreditBalance not found for user {user_id}")

    new_balance = balance_row.balance + amount
    balance_row.balance = new_balance

    db.add(
        CreditTransaction(
            user_id=user_id,
            type=TransactionType.RIDE_REFUND,
            amount=amount,
            balance_after=new_balance,
            reference_id=reference_id,
        )
    )

    await db.flush()

    return new_balance
