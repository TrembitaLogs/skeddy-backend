import logging
from datetime import UTC, datetime
from uuid import UUID

from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.credit_balance import CreditBalance
from app.models.credit_transaction import CreditTransaction, TransactionType
from app.models.legacy_credit import LegacyCredit

logger = logging.getLogger(__name__)

CLAIM_ATTEMPTS_KEY = "legacy_claim_attempts:{user_id}"
CLAIM_ATTEMPTS_LIMIT = 3
CLAIM_ATTEMPTS_WINDOW = 3600  # 1 hour


async def _check_rate_limit(user_id: UUID, redis: Redis) -> bool:
    """Return True if user is within the rate limit for legacy credit claims."""
    key = CLAIM_ATTEMPTS_KEY.format(user_id=user_id)
    try:
        current = await redis.get(key)
        if current is not None and int(current) >= CLAIM_ATTEMPTS_LIMIT:
            return False
    except RedisError:
        logger.warning("Redis unavailable for legacy claim rate limit, allowing attempt")
    return True


async def _increment_attempts(user_id: UUID, redis: Redis) -> None:
    """Increment the claim attempt counter for the user."""
    key = CLAIM_ATTEMPTS_KEY.format(user_id=user_id)
    try:
        await redis.incr(key)
        await redis.expire(key, CLAIM_ATTEMPTS_WINDOW)
    except RedisError:
        logger.warning("Redis unavailable for legacy claim rate limit increment")


async def try_claim_legacy_credits(
    user_id: UUID,
    phone_number: str | None,
    license_number: str | None,
    db: AsyncSession,
    redis: Redis,
) -> int | None:
    """Attempt to claim legacy credits by phone_number + license_number.

    Check order:
    1. User already claimed legacy credits → skip
    2. Both phone_number and license_number present → proceed
    3. Rate limit not exceeded → search legacy_credits

    Returns the claimed amount, or None if no match / already claimed / rate limited.
    """
    # 1. Already claimed? Check by existing LEGACY_IMPORT transaction
    existing_claim = await db.execute(
        select(CreditTransaction.id)
        .where(
            CreditTransaction.user_id == user_id,
            CreditTransaction.type == TransactionType.LEGACY_IMPORT,
        )
        .limit(1)
    )
    if existing_claim.scalar_one_or_none() is not None:
        return None

    # 2. Both fields required
    if not phone_number or not license_number:
        return None

    # 3. Rate limit
    if not await _check_rate_limit(user_id, redis):
        logger.info("Legacy claim rate limited for user %s", user_id)
        return None

    await _increment_attempts(user_id, redis)

    # Look up legacy record
    result = await db.execute(
        select(LegacyCredit).where(
            LegacyCredit.phone_number == phone_number,
            LegacyCredit.license_number == license_number,
            LegacyCredit.claimed_at.is_(None),
        )
    )
    legacy = result.scalar_one_or_none()

    if legacy is None or legacy.balance <= 0:
        return None

    # Transfer credits
    amount = legacy.balance

    balance_row = await db.execute(
        select(CreditBalance).where(CreditBalance.user_id == user_id).with_for_update()
    )
    credit_balance = balance_row.scalar_one_or_none()
    if credit_balance is None:
        logger.error("CreditBalance not found for user %s during legacy claim", user_id)
        return None

    new_balance = credit_balance.balance + amount
    credit_balance.balance = new_balance

    db.add(
        CreditTransaction(
            user_id=user_id,
            type=TransactionType.LEGACY_IMPORT,
            amount=amount,
            balance_after=new_balance,
            description=f"Legacy credit transfer from old user #{legacy.old_user_id}",
        )
    )

    legacy.balance = 0
    legacy.claimed_at = datetime.now(UTC)

    await db.flush()

    logger.info(
        "Legacy credits claimed: user=%s amount=%d old_user_id=%d",
        user_id,
        amount,
        legacy.old_user_id,
    )

    return amount
