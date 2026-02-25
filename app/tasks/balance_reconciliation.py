"""Background task: daily balance reconciliation.

Runs once every 24 hours and verifies that each user's denormalized
credit balance matches the sum of their credit transactions (PRD section 12).

Uses incremental reconciliation with Redis checkpoints to avoid
re-processing all transactions every run.  Full reconciliation runs
once per week or when a mismatch is detected for a specific user.
"""

import asyncio
import contextlib
import json
import logging
from datetime import UTC, datetime, timedelta
from uuid import UUID

from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import AsyncSessionLocal
from app.models.credit_balance import CreditBalance
from app.models.credit_transaction import CreditTransaction, TransactionType
from app.models.ride import Ride
from app.redis import redis_client

logger = logging.getLogger(__name__)

RECONCILIATION_INTERVAL_SECONDS = 86400  # 24 hours
INITIAL_DELAY_SECONDS = 14400  # 4 h stagger from other daily tasks

CHECKPOINT_KEY_PREFIX = "reconciliation_checkpoint:"
CHECKPOINT_TTL = 691200  # 8 days

LAST_FULL_RUN_KEY = "reconciliation_last_full_run"
LAST_FULL_RUN_TTL = 691200  # 8 days
FULL_RUN_INTERVAL_DAYS = 7

BATCH_SIZE = 50
BATCH_PAUSE_SECONDS = 0.1  # 100 ms between batches

EIGHT_WEEKS = timedelta(weeks=8)


# ---------------------------------------------------------------------------
# Redis helpers
# ---------------------------------------------------------------------------


def checkpoint_key(user_id: UUID) -> str:
    """Return Redis key for a user's reconciliation checkpoint."""
    return f"{CHECKPOINT_KEY_PREFIX}{user_id}"


async def needs_full_run(redis: Redis) -> bool:
    """Return True if a full (non-incremental) reconciliation is due."""
    try:
        last_full = await redis.get(LAST_FULL_RUN_KEY)
        if last_full is None:
            return True
        last_dt = datetime.fromisoformat(last_full)
        return datetime.now(UTC) - last_dt > timedelta(days=FULL_RUN_INTERVAL_DAYS)
    except (RedisError, ValueError):
        # Redis unavailable or corrupt value -> do full run
        return True


async def mark_full_run(redis: Redis) -> None:
    """Record that a full reconciliation has been completed."""
    try:
        await redis.setex(
            LAST_FULL_RUN_KEY,
            LAST_FULL_RUN_TTL,
            datetime.now(UTC).isoformat(),
        )
    except RedisError:
        logger.warning("Redis unavailable setting %s", LAST_FULL_RUN_KEY)


async def get_checkpoint(user_id: UUID, redis: Redis) -> dict | None:
    """Retrieve reconciliation checkpoint for a user from Redis.

    Returns ``None`` when the key is missing, Redis is unavailable,
    or the stored value is corrupt.
    """
    try:
        raw = await redis.get(checkpoint_key(user_id))
        if raw is None:
            return None
        data = json.loads(raw)
        # Validate required fields
        if not all(
            k in data for k in ("last_tx_id", "last_tx_created_at", "balance_at_checkpoint")
        ):
            return None
        return dict(data)
    except (RedisError, json.JSONDecodeError, KeyError, TypeError):
        return None


async def save_checkpoint(
    user_id: UUID,
    last_tx_id: str,
    last_tx_created_at: str,
    balance_at_checkpoint: int,
    redis: Redis,
) -> None:
    """Persist reconciliation checkpoint for a user to Redis."""
    data = json.dumps(
        {
            "last_tx_id": last_tx_id,
            "last_tx_created_at": last_tx_created_at,
            "balance_at_checkpoint": balance_at_checkpoint,
        }
    )
    try:
        await redis.setex(checkpoint_key(user_id), CHECKPOINT_TTL, data)
    except RedisError:
        logger.warning("Redis unavailable saving checkpoint for user %s", user_id)


async def delete_checkpoint(user_id: UUID, redis: Redis) -> None:
    """Delete checkpoint for a user (forces full reconciliation next run)."""
    with contextlib.suppress(RedisError):
        await redis.delete(checkpoint_key(user_id))


# ---------------------------------------------------------------------------
# Core reconciliation logic
# ---------------------------------------------------------------------------


async def get_all_user_ids(db: AsyncSession) -> list[UUID]:
    """Return every user_id that has a credit balance row."""
    result = await db.execute(select(CreditBalance.user_id))
    return [row[0] for row in result.all()]


async def reconcile_user_balance(
    user_id: UUID,
    force_full: bool,
    db: AsyncSession,
    redis: Redis,
) -> bool:
    """Reconcile a single user's balance vs transaction log.

    Returns ``True`` when the balance matches (or user has no balance row),
    ``False`` when a mismatch is detected.
    """
    # Get actual denormalized balance
    result = await db.execute(
        select(CreditBalance.balance).where(CreditBalance.user_id == user_id)
    )
    actual_balance = result.scalar_one_or_none()
    if actual_balance is None:
        return True  # No balance row — nothing to reconcile

    cp = None if force_full else await get_checkpoint(user_id, redis)

    if cp is not None:
        # --- Incremental reconciliation ---
        cp_time = datetime.fromisoformat(cp["last_tx_created_at"])
        cp_balance: int = cp["balance_at_checkpoint"]

        result = await db.execute(
            select(func.coalesce(func.sum(CreditTransaction.amount), 0)).where(
                CreditTransaction.user_id == user_id,
                CreditTransaction.created_at > cp_time,
            )
        )
        incremental_sum: int = result.scalar_one()
        expected_balance = cp_balance + incremental_sum
    else:
        # --- Full reconciliation ---
        result = await db.execute(
            select(func.coalesce(func.sum(CreditTransaction.amount), 0)).where(
                CreditTransaction.user_id == user_id,
            )
        )
        expected_balance = result.scalar_one()

    if expected_balance != actual_balance:
        logger.warning(
            "BALANCE_MISMATCH: user_id=%s, expected=%d, actual=%d, diff=%d",
            user_id,
            expected_balance,
            actual_balance,
            actual_balance - expected_balance,
        )
        await delete_checkpoint(user_id, redis)
        return False

    # Balance matches — update checkpoint with latest transaction
    result = await db.execute(
        select(CreditTransaction.id, CreditTransaction.created_at)
        .where(CreditTransaction.user_id == user_id)
        .order_by(CreditTransaction.created_at.desc())
        .limit(1)
    )
    latest_tx = result.first()
    if latest_tx is not None:
        await save_checkpoint(
            user_id,
            str(latest_tx.id),
            latest_tx.created_at.isoformat(),
            actual_balance,
            redis,
        )

    return True


async def reconcile_ride_credits(user_id: UUID, db: AsyncSession) -> None:
    """Verify ride credits_charged / credits_refunded vs transaction log.

    Only checks rides younger than 8 weeks (older rides are deleted by the
    cleanup task and their ``reference_id`` in transactions is already NULL).
    """
    cutoff = datetime.now(UTC) - EIGHT_WEEKS

    result = await db.execute(
        select(Ride.id, Ride.credits_charged, Ride.credits_refunded).where(
            Ride.user_id == user_id,
            Ride.created_at > cutoff,
        )
    )
    rides = result.all()
    if not rides:
        return

    ride_ids = [r.id for r in rides]

    # Batch-fetch RIDE_CHARGE transactions for these rides
    charge_result = await db.execute(
        select(CreditTransaction.reference_id, CreditTransaction.amount).where(
            CreditTransaction.reference_id.in_(ride_ids),
            CreditTransaction.type == TransactionType.RIDE_CHARGE.value,
        )
    )
    charge_map: dict[UUID, int] = {
        row.reference_id: abs(row.amount) for row in charge_result.all()
    }

    # Batch-fetch RIDE_REFUND transactions for these rides
    refund_result = await db.execute(
        select(CreditTransaction.reference_id, CreditTransaction.amount).where(
            CreditTransaction.reference_id.in_(ride_ids),
            CreditTransaction.type == TransactionType.RIDE_REFUND.value,
        )
    )
    refund_map: dict[UUID, int] = {row.reference_id: row.amount for row in refund_result.all()}

    for ride in rides:
        tx_charged = charge_map.get(ride.id, 0)
        if ride.credits_charged != tx_charged:
            logger.warning(
                "RIDE_CREDIT_MISMATCH: ride_id=%s, field=credits_charged, expected=%d, actual=%d",
                ride.id,
                tx_charged,
                ride.credits_charged,
            )

        tx_refunded = refund_map.get(ride.id, 0)
        if ride.credits_refunded != tx_refunded:
            logger.warning(
                "RIDE_CREDIT_MISMATCH: ride_id=%s, field=credits_refunded, expected=%d, actual=%d",
                ride.id,
                tx_refunded,
                ride.credits_refunded,
            )


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


async def run_balance_reconciliation() -> None:
    """Background task: reconcile credit balances once every 24 hours."""
    logger.info(
        "Balance reconciliation task started (interval=%d seconds)",
        RECONCILIATION_INTERVAL_SECONDS,
    )
    await asyncio.sleep(INITIAL_DELAY_SECONDS)

    while True:
        try:
            force_full = await needs_full_run(redis_client)
            if force_full:
                logger.info("Balance reconciliation: performing full run")

            async with AsyncSessionLocal() as db:
                user_ids = await get_all_user_ids(db)

            if not user_ids:
                logger.debug("Balance reconciliation: no users found")
            else:
                logger.info(
                    "Balance reconciliation: processing %d user(s)%s",
                    len(user_ids),
                    " (full)" if force_full else " (incremental)",
                )

                mismatch_count = 0
                for i in range(0, len(user_ids), BATCH_SIZE):
                    batch = user_ids[i : i + BATCH_SIZE]
                    for user_id in batch:
                        try:
                            async with AsyncSessionLocal() as db:
                                matched = await reconcile_user_balance(
                                    user_id, force_full, db, redis_client
                                )
                                if not matched:
                                    mismatch_count += 1
                                await reconcile_ride_credits(user_id, db)
                        except Exception:
                            logger.exception(
                                "Balance reconciliation error for user %s",
                                user_id,
                            )

                    # Pause between batches to spread DB load
                    if i + BATCH_SIZE < len(user_ids):
                        await asyncio.sleep(BATCH_PAUSE_SECONDS)

                logger.info(
                    "Balance reconciliation complete: %d mismatch(es) out of %d user(s)",
                    mismatch_count,
                    len(user_ids),
                )

            if force_full:
                await mark_full_run(redis_client)

        except Exception:
            logger.exception("Balance reconciliation task error")

        await asyncio.sleep(RECONCILIATION_INTERVAL_SECONDS)
