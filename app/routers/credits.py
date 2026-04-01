"""Credit purchase endpoint for Google Play in-app purchases."""

import logging
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from redis.asyncio import Redis
from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.dependencies.auth import get_current_user
from app.middleware.rate_limiter import get_user_key, limiter
from app.models.credit_balance import CreditBalance
from app.models.credit_transaction import TransactionType
from app.models.purchase_order import PurchaseOrder, PurchaseStatus
from app.models.user import User
from app.redis import get_redis
from app.schemas.credits import (
    PurchaseRequest,
    PurchaseResponse,
    RestoreCreditsRequest,
    RestoreCreditsResponse,
)
from app.services.config_service import get_credit_products
from app.services.credit_service import add_credits, cache_balance, get_balance
from app.services.google_play_service import (
    GooglePlayService,
    GooglePlayVerificationError,
)
from app.services.legacy_credit_service import RestoreStatus, try_restore_legacy_credits

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/credits", tags=["credits"])

_google_play_service: GooglePlayService | None = None


def _get_google_play_service() -> GooglePlayService:
    """Return lazy singleton GooglePlayService instance."""
    global _google_play_service
    if _google_play_service is None:
        _google_play_service = GooglePlayService()
    return _google_play_service


async def _handle_order_id_conflict(
    google_order_id: str | None,
    user_id: UUID,
    db: AsyncSession,
    redis: Redis,
    response: Response,
) -> PurchaseResponse:
    """Handle IntegrityError from google_order_id unique constraint.

    Another order already claimed this Google order — return idempotent success.
    """
    if google_order_id:
        result = await db.execute(
            select(PurchaseOrder).where(
                PurchaseOrder.google_order_id == google_order_id,
            )
        )
        existing = result.scalar_one_or_none()
        if existing:
            current_balance = await get_balance(user_id, db, redis)
            response.status_code = 200
            return PurchaseResponse(
                credits_added=existing.credits_amount,
                new_balance=current_balance,
            )
    raise HTTPException(status_code=503, detail="SERVICE_UNAVAILABLE")


async def _finalize_purchase(
    order: PurchaseOrder,
    user_id: UUID,
    db: AsyncSession,
    redis: Redis,
    response: Response,
) -> PurchaseResponse:
    """Atomic CONSUMED -> VERIFIED transition with credit application.

    Prevents double crediting via optimistic locking: only the process that
    successfully transitions CONSUMED -> VERIFIED proceeds to add credits.
    If another process already finalized this order, returns idempotent
    success with HTTP 200.
    """
    # Capture before any commit/rollback may expire ORM attributes
    order_id = order.id
    order_credits = order.credits_amount
    order_product_id = order.product_id

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
        # Another process already finalized — return idempotent success
        await db.rollback()
        current_balance = await get_balance(user_id, db, redis)
        response.status_code = 200
        return PurchaseResponse(
            credits_added=order_credits,
            new_balance=current_balance,
        )

    # add_credits commits the transaction (atomic with the status change above)
    new_balance = await add_credits(
        user_id=user_id,
        amount=order_credits,
        tx_type=TransactionType.PURCHASE,
        reference_id=order_id,
        db=db,
        redis=redis,
    )

    logger.info(
        "Purchase verified: user_id=%s, product_id=%s, credits=%d, new_balance=%d",
        user_id,
        order_product_id,
        order_credits,
        new_balance,
    )

    return PurchaseResponse(credits_added=order_credits, new_balance=new_balance)


@router.post("/purchase", response_model=PurchaseResponse, status_code=201)
@limiter.limit("10/minute", key_func=get_user_key)
async def purchase_credits(
    request: Request,
    response: Response,
    body: PurchaseRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    """Verify a Google Play purchase and credit the user's balance.

    Idempotent: repeated requests with the same purchase_token return
    the existing result without double-crediting. Supports recovery for
    orders stuck in CONSUMED state (credits not yet applied).
    """
    # 1. Map product_id -> credits from AppConfig catalog
    products = await get_credit_products(db, redis)
    product = products.get_product_by_id(body.product_id)
    if product is None:
        raise HTTPException(status_code=400, detail="UNKNOWN_PRODUCT")
    credits_amount = product.credits

    # 2. Lookup existing PurchaseOrder by purchase_token (idempotency)
    result = await db.execute(
        select(PurchaseOrder).where(PurchaseOrder.purchase_token == body.purchase_token)
    )
    order = result.scalar_one_or_none()

    if order is not None:
        # 2a. VERIFIED — idempotent replay (credits already applied)
        if order.status == PurchaseStatus.VERIFIED.value:
            current_balance = await get_balance(current_user.id, db, redis)
            response.status_code = 200
            return PurchaseResponse(
                credits_added=order.credits_amount,
                new_balance=current_balance,
            )

        # 2b. CONSUMED — recovery (consume done, credits not yet applied)
        if order.status == PurchaseStatus.CONSUMED.value:
            return await _finalize_purchase(order, current_user.id, db, redis, response)

        # 2c. FAILED or PENDING — reuse record for fresh verification attempt
        order.credits_amount = credits_amount
        order.product_id = body.product_id
    else:
        # 2d. Not found — create new PurchaseOrder(PENDING)
        order = PurchaseOrder(
            user_id=current_user.id,
            product_id=body.product_id,
            purchase_token=body.purchase_token,
            credits_amount=credits_amount,
            status=PurchaseStatus.PENDING.value,
        )
        db.add(order)
        try:
            await db.commit()
        except IntegrityError:
            # Race: concurrent request already inserted this purchase_token
            await db.rollback()
            logger.info(
                "Purchase token race condition: user_id=%s, token=%s",
                current_user.id,
                body.purchase_token[:16],
            )
            result = await db.execute(
                select(PurchaseOrder).where(PurchaseOrder.purchase_token == body.purchase_token)
            )
            order = result.scalar_one_or_none()
            if order is None:
                raise
            if order.status == PurchaseStatus.VERIFIED.value:
                current_balance = await get_balance(current_user.id, db, redis)
                response.status_code = 200
                return PurchaseResponse(
                    credits_added=order.credits_amount,
                    new_balance=current_balance,
                )
            if order.status == PurchaseStatus.CONSUMED.value:
                return await _finalize_purchase(order, current_user.id, db, redis, response)
            # PENDING or FAILED — reuse existing order for verification

    # 3. Verify with Google Play Developer API
    gp_service = _get_google_play_service()
    try:
        gp_result = await gp_service.verify_purchase(body.product_id, body.purchase_token)
    except GooglePlayVerificationError:
        order.status = PurchaseStatus.FAILED.value
        await db.commit()
        raise HTTPException(status_code=400, detail="INVALID_PURCHASE_TOKEN")
    except Exception:
        logger.error(
            "Google Play verify failed: user_id=%s, product_id=%s",
            current_user.id,
            body.product_id,
            exc_info=True,
        )
        order.status = PurchaseStatus.FAILED.value
        await db.commit()
        raise HTTPException(status_code=503, detail="SERVICE_UNAVAILABLE")

    # 4. Check google_order_id deduplication BEFORE recording it on the order.
    #    Prevents double-crediting when two different purchase_tokens resolve
    #    to the same Google order (UNIQUE constraint is the second line of defense).
    if gp_result.order_id:
        dup_result = await db.execute(
            select(PurchaseOrder).where(
                PurchaseOrder.google_order_id == gp_result.order_id,
                PurchaseOrder.status == PurchaseStatus.VERIFIED.value,
                PurchaseOrder.id != order.id,
            )
        )
        dup_order = dup_result.scalar_one_or_none()
        if dup_order is not None:
            current_balance = await get_balance(current_user.id, db, redis)
            response.status_code = 200
            return PurchaseResponse(
                credits_added=dup_order.credits_amount,
                new_balance=current_balance,
            )

    # 4b. Record google_order_id (safe — dedup check passed)
    order.google_order_id = gp_result.order_id

    # 5. Handle already_consumed from Google (crash recovery: our consume()
    #    succeeded in Google but status=CONSUMED was never persisted in our DB)
    if gp_result.already_consumed:
        order.status = PurchaseStatus.CONSUMED.value
        try:
            await db.commit()
        except IntegrityError:
            await db.rollback()
            logger.info(
                "google_order_id race (already_consumed): order_id=%s",
                gp_result.order_id,
            )
            return await _handle_order_id_conflict(
                gp_result.order_id, current_user.id, db, redis, response
            )
        return await _finalize_purchase(order, current_user.id, db, redis, response)

    # 6. Consume purchase (allows repurchase of same SKU)
    consumed = await gp_service.consume_purchase(body.product_id, body.purchase_token)
    if not consumed:
        order.status = PurchaseStatus.FAILED.value
        await db.commit()
        raise HTTPException(status_code=503, detail="SERVICE_UNAVAILABLE")

    # 7. Persist CONSUMED status (records the irreversible consume action)
    order.status = PurchaseStatus.CONSUMED.value
    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        logger.info(
            "google_order_id race (consume): order_id=%s",
            gp_result.order_id,
        )
        return await _handle_order_id_conflict(
            gp_result.order_id, current_user.id, db, redis, response
        )

    # 8. Finalize: atomic CONSUMED -> VERIFIED + credit balance + transaction
    return await _finalize_purchase(order, current_user.id, db, redis, response)


@router.post("/restore", response_model=RestoreCreditsResponse)
@limiter.limit("3/hour", key_func=get_user_key)
async def restore_legacy_credits(
    request: Request,
    response: Response,
    body: RestoreCreditsRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    """Restore legacy credits from the old system.

    Accepts phone_number and license_number in the request body
    to look up and transfer credits from the legacy_credits table.
    """
    result = await try_restore_legacy_credits(
        user_id=current_user.id,
        phone_number=body.phone_number,
        license_number=body.license_number,
        db=db,
        redis=redis,
    )

    if result.status == RestoreStatus.ALREADY_RESTORED:
        raise HTTPException(status_code=409, detail="ALREADY_RESTORED")

    if result.status == RestoreStatus.RATE_LIMITED:
        response.headers["Retry-After"] = str(result.retry_after_seconds)
        raise HTTPException(
            status_code=429,
            detail={
                "error": "RATE_LIMIT_EXCEEDED",
                "retry_after_seconds": result.retry_after_seconds,
            },
        )

    if result.status == RestoreStatus.NO_MATCH:
        raise HTTPException(status_code=404, detail="NO_MATCH")

    # SUCCESS — commit and update cache
    await db.commit()

    balance_row = await db.execute(
        select(CreditBalance.balance).where(CreditBalance.user_id == current_user.id)
    )
    balance = balance_row.scalar_one_or_none()
    if balance is not None:
        await cache_balance(current_user.id, balance, redis)

    return RestoreCreditsResponse(restored_credits=result.restored_credits)
