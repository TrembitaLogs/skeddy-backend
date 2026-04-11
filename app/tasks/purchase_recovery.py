"""Background task: recovery of CONSUMED purchase orders.

Runs every 5 minutes and finalizes orders stuck in CONSUMED state —
i.e. Google Play consume succeeded but credit application failed
(crash, timeout, etc. between steps 7 and 8-9 in PRD section 4).

Recovery uses the same atomic CONSUMED -> VERIFIED transition with
rowcount check as the purchase endpoint to prevent double crediting.
"""

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from uuid import UUID

from redis.asyncio import Redis
from sqlalchemy import func, select, update
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import AsyncSessionLocal
from app.models.credit_transaction import TransactionType
from app.models.purchase_order import PurchaseOrder, PurchaseStatus
from app.redis import redis_client
from app.services.credit_service import add_credits

logger = logging.getLogger(__name__)

RECOVERY_INTERVAL_SECONDS = 5 * 60  # 5 minutes
INITIAL_DELAY_SECONDS = 40  # Stagger startup relative to other tasks
MIN_AGE_MINUTES = 2  # Skip orders younger than 2 min (still in active request)
MAX_AGE_HOURS = 24  # Orders older than 24h are anomalous


async def get_recoverable_order_ids(db: AsyncSession) -> list[UUID]:
    """Return IDs of CONSUMED orders eligible for automatic recovery.

    Eligible: status = CONSUMED, created_at between 2 minutes and 24 hours ago.
    Uses the partial index ``idx_purchase_orders_consumed`` for efficient lookup.
    """
    now = datetime.now(UTC)
    result = await db.execute(
        select(PurchaseOrder.id).where(
            PurchaseOrder.status == PurchaseStatus.CONSUMED.value,
            PurchaseOrder.created_at < now - timedelta(minutes=MIN_AGE_MINUTES),
            PurchaseOrder.created_at > now - timedelta(hours=MAX_AGE_HOURS),
        )
    )
    return list(result.scalars().all())


async def get_stuck_order_ids(db: AsyncSession) -> list[UUID]:
    """Return IDs of CONSUMED orders older than 24 hours (anomalous).

    These are NOT auto-processed — only logged as PURCHASE_STUCK warning
    for manual intervention by admin.
    """
    now = datetime.now(UTC)
    result = await db.execute(
        select(PurchaseOrder.id).where(
            PurchaseOrder.status == PurchaseStatus.CONSUMED.value,
            PurchaseOrder.created_at <= now - timedelta(hours=MAX_AGE_HOURS),
        )
    )
    return list(result.scalars().all())


async def recover_order(order_id: UUID, db: AsyncSession, redis: Redis) -> bool:
    """Attempt to finalize a CONSUMED order (PRD steps 8-9).

    Atomically transitions CONSUMED -> VERIFIED and applies credits.
    If another process already finalized this order (rowcount=0),
    logs success and returns False.

    Returns True if credits were applied by this call, False if already handled.
    """
    result = await db.execute(select(PurchaseOrder).where(PurchaseOrder.id == order_id))
    order = result.scalar_one_or_none()

    if order is None or order.status != PurchaseStatus.CONSUMED.value:
        return False

    order_user_id = order.user_id
    order_credits = order.credits_amount
    order_product_id = order.product_id

    if order_user_id is None:
        logger.warning(
            "PURCHASE_RECOVERY_SKIP: order_id=%s has no user_id (soft-deleted)", order_id
        )
        return False

    # Atomic claim: CONSUMED -> VERIFIED (prevents double crediting)
    claim = await db.execute(
        update(PurchaseOrder)
        .where(
            PurchaseOrder.id == order_id,
            PurchaseOrder.status == PurchaseStatus.CONSUMED.value,
        )
        .values(
            status=PurchaseStatus.VERIFIED.value,
            verified_at=func.now(),
        )
    )

    if claim.rowcount == 0:  # type: ignore[attr-defined]
        # Another process (client retry) already finalized — treat as success
        logger.info(
            "PURCHASE_RECOVERED (by other process): order_id=%s, user_id=%s",
            order_id,
            order_user_id,
        )
        return False

    # Apply credits — commits the transaction atomically with status change.
    # add_credits may raise HTTPException(503) on lock timeout; the caller's
    # except-handler lets the session context manager rollback, keeping the
    # order CONSUMED for the next recovery cycle.
    new_balance = await add_credits(
        user_id=order_user_id,
        amount=order_credits,
        tx_type=TransactionType.PURCHASE,
        reference_id=order_id,
        db=db,
        redis=redis,
    )

    logger.info(
        "PURCHASE_RECOVERED: order_id=%s, user_id=%s, product_id=%s, credits=%d, new_balance=%d",
        order_id,
        order_user_id,
        order_product_id,
        order_credits,
        new_balance,
    )

    return True


async def run_purchase_recovery() -> None:
    """Background task: recover CONSUMED orders every 5 minutes.

    Recovery path for orders where Google Play consume() succeeded but
    credit application failed (PRD section 4).  Also logs PURCHASE_STUCK
    warnings for anomalous orders older than 24 hours.

    Each order is processed in an isolated DB session so that a failure
    for one order does not affect others.
    """
    logger.info(
        "Purchase recovery task started (interval=%d seconds)",
        RECOVERY_INTERVAL_SECONDS,
    )
    await asyncio.sleep(INITIAL_DELAY_SECONDS)

    while True:
        try:
            # Phase 1: Find and recover eligible orders (2 min - 24 hours old)
            async with AsyncSessionLocal() as db:
                order_ids = await get_recoverable_order_ids(db)

            if not order_ids:
                logger.debug("Purchase recovery: no CONSUMED orders to recover")
            else:
                logger.info(
                    "Purchase recovery: processing %d CONSUMED order(s)",
                    len(order_ids),
                )
                for order_id in order_ids:
                    try:
                        async with AsyncSessionLocal() as db:
                            await recover_order(order_id, db, redis_client)
                    except (OperationalError, OSError, ValueError):
                        logger.exception("Purchase recovery error for order %s", order_id)

            # Phase 2: Log stuck orders (> 24 hours old)
            async with AsyncSessionLocal() as db:
                stuck_ids = await get_stuck_order_ids(db)

            if stuck_ids:
                logger.warning(
                    "PURCHASE_STUCK: %d order(s) older than %d hours "
                    "require manual intervention: %s",
                    len(stuck_ids),
                    MAX_AGE_HOURS,
                    [str(sid) for sid in stuck_ids],
                )

        except (OperationalError, OSError):
            logger.exception("Purchase recovery task error")

        await asyncio.sleep(RECOVERY_INTERVAL_SECONDS)
