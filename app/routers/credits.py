"""Credit purchase endpoint for Google Play in-app purchases."""

import logging
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from googleapiclient.errors import HttpError
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
    GooglePurchaseResult,
)
from app.services.legacy_credit_service import RestoreStatus, try_restore_legacy_credits

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/credits", tags=["credits"])


def _create_google_play_service() -> GooglePlayService:
    """Create a GooglePlayService instance (separated for testability)."""
    return GooglePlayService()


def get_google_play_service() -> GooglePlayService:
    """FastAPI dependency that provides GooglePlayService via DI."""
    return _create_google_play_service()


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

    Uses a savepoint (begin_nested) so a failed optimistic-lock attempt
    only rolls back this operation, consistent with the savepoint policy
    used in ping_service.py for concurrent state transitions.
    """
    # Capture before any savepoint rollback may expire ORM attributes
    order_id = order.id
    order_credits = order.credits_amount
    order_product_id = order.product_id

    try:
        async with db.begin_nested():
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
                raise IntegrityError("Optimistic lock failed", params=None, orig=Exception())
    except IntegrityError:
        # Savepoint rolled back — verify actual order state
        refreshed = await db.execute(
            select(PurchaseOrder.status).where(PurchaseOrder.id == order_id)
        )
        actual_status = refreshed.scalar_one_or_none()
        if actual_status != PurchaseStatus.VERIFIED.value:
            logger.error(
                "Finalize race: order %s expected VERIFIED but found %s",
                order_id,
                actual_status,
            )
            raise HTTPException(status_code=409, detail="ORDER_STATE_CONFLICT")
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


async def _resolve_or_create_order(
    purchase_token: str,
    product_id: str,
    credits_amount: int,
    user_id: UUID,
    db: AsyncSession,
    redis: Redis,
    response: Response,
) -> PurchaseOrder | PurchaseResponse:
    """Lookup existing order by purchase_token or create a new PENDING one.

    Returns a PurchaseResponse early for VERIFIED (idempotent replay) or
    CONSUMED (recovery) orders. Otherwise returns the PurchaseOrder for
    fresh verification.
    """
    result = await db.execute(
        select(PurchaseOrder).where(PurchaseOrder.purchase_token == purchase_token)
    )
    order = result.scalar_one_or_none()

    if order is not None:
        if order.status == PurchaseStatus.VERIFIED.value:
            current_balance = await get_balance(user_id, db, redis)
            response.status_code = 200
            return PurchaseResponse(
                credits_added=order.credits_amount,
                new_balance=current_balance,
            )
        if order.status == PurchaseStatus.CONSUMED.value:
            return await _finalize_purchase(order, user_id, db, redis, response)
        order.credits_amount = credits_amount
        order.product_id = product_id
        return order

    order = PurchaseOrder(
        user_id=user_id,
        product_id=product_id,
        purchase_token=purchase_token,
        credits_amount=credits_amount,
        status=PurchaseStatus.PENDING.value,
    )
    db.add(order)
    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        logger.info(
            "Purchase token race condition: user_id=%s, token=%s",
            user_id,
            purchase_token[:16],
        )
        result = await db.execute(
            select(PurchaseOrder).where(PurchaseOrder.purchase_token == purchase_token)
        )
        order = result.scalar_one_or_none()
        if order is None:
            raise
        if order.status == PurchaseStatus.VERIFIED.value:
            current_balance = await get_balance(user_id, db, redis)
            response.status_code = 200
            return PurchaseResponse(
                credits_added=order.credits_amount,
                new_balance=current_balance,
            )
        if order.status == PurchaseStatus.CONSUMED.value:
            return await _finalize_purchase(order, user_id, db, redis, response)
    return order


async def _verify_with_google_play(
    order: PurchaseOrder,
    product_id: str,
    purchase_token: str,
    user_id: UUID,
    db: AsyncSession,
    gp_service: GooglePlayService,
) -> GooglePurchaseResult:
    """Verify purchase token with Google Play. Marks order FAILED on error."""
    try:
        return await gp_service.verify_purchase(product_id, purchase_token)
    except GooglePlayVerificationError:
        order.status = PurchaseStatus.FAILED.value
        await db.commit()
        raise HTTPException(status_code=400, detail="INVALID_PURCHASE_TOKEN")
    except (OSError, TimeoutError, ValueError, HttpError) as exc:
        logger.error(
            "Google Play verify failed: user_id=%s, product_id=%s: %s",
            user_id,
            product_id,
            exc,
            exc_info=True,
        )
        order.status = PurchaseStatus.FAILED.value
        await db.commit()
        raise HTTPException(status_code=503, detail="SERVICE_UNAVAILABLE")


async def _consume_and_persist(
    order: PurchaseOrder,
    gp_result: "GooglePurchaseResult",
    product_id: str,
    purchase_token: str,
    user_id: UUID,
    db: AsyncSession,
    redis: Redis,
    response: Response,
    gp_service: GooglePlayService,
) -> PurchaseResponse:
    """Handle Google consume flow: already_consumed recovery, consume API, persist CONSUMED, finalize."""
    # Already consumed by Google (crash recovery path)
    if gp_result.already_consumed:
        order.status = PurchaseStatus.CONSUMED.value
        try:
            await db.commit()
        except IntegrityError:
            await db.rollback()
            logger.info("google_order_id race (already_consumed): order_id=%s", gp_result.order_id)
            return await _handle_order_id_conflict(
                gp_result.order_id, user_id, db, redis, response
            )
        return await _finalize_purchase(order, user_id, db, redis, response)

    # Consume purchase via Google Play API
    try:
        consumed = await gp_service.consume_purchase(product_id, purchase_token)
    except (OSError, TimeoutError, HttpError) as exc:
        logger.error(
            "Google Play consume failed: user_id=%s, product_id=%s: %s",
            user_id,
            product_id,
            exc,
            exc_info=True,
        )
        order.status = PurchaseStatus.FAILED.value
        await db.commit()
        raise HTTPException(status_code=503, detail="SERVICE_UNAVAILABLE")
    if not consumed:
        order.status = PurchaseStatus.FAILED.value
        await db.commit()
        raise HTTPException(status_code=503, detail="SERVICE_UNAVAILABLE")

    # Persist CONSUMED status
    order.status = PurchaseStatus.CONSUMED.value
    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        logger.info("google_order_id race (consume): order_id=%s", gp_result.order_id)
        return await _handle_order_id_conflict(gp_result.order_id, user_id, db, redis, response)

    return await _finalize_purchase(order, user_id, db, redis, response)


@router.post("/purchase", response_model=PurchaseResponse, status_code=201)
@limiter.limit("10/minute", key_func=get_user_key)
async def purchase_credits(
    request: Request,
    response: Response,
    body: PurchaseRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis),
    gp_service: GooglePlayService = Depends(get_google_play_service),
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
    if product.credits <= 0:
        logger.error("Product %s has non-positive credits: %d", body.product_id, product.credits)
        raise HTTPException(status_code=400, detail="INVALID_PRODUCT_CONFIG")

    # 2. Resolve existing order or create a new PENDING one
    order_or_response = await _resolve_or_create_order(
        body.purchase_token,
        body.product_id,
        product.credits,
        current_user.id,
        db,
        redis,
        response,
    )
    if isinstance(order_or_response, PurchaseResponse):
        return order_or_response
    order = order_or_response

    # 3. Verify with Google Play Developer API
    gp_result = await _verify_with_google_play(
        order,
        body.product_id,
        body.purchase_token,
        current_user.id,
        db,
        gp_service,
    )

    # 4. Check google_order_id deduplication
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
    order.google_order_id = gp_result.order_id

    # 5-8. Consume, persist, and finalize
    return await _consume_and_persist(
        order,
        gp_result,
        body.product_id,
        body.purchase_token,
        current_user.id,
        db,
        redis,
        response,
        gp_service,
    )


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
