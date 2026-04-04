import logging
from datetime import datetime

from fastapi import APIRouter, Depends, Query, Request, Response
from redis.asyncio import Redis
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.dependencies.auth import get_current_user
from app.dependencies.device_auth import verify_device
from app.middleware.rate_limiter import get_device_key, get_user_key, limiter
from app.models.paired_device import PairedDevice
from app.models.user import User
from app.redis import get_redis
from app.schemas.rides import (
    CreateRideRequest,
    CreateRideResponse,
    CreditEventResponse,
    EventsListResponse,
    RideEventResponse,
)
from app.services.config_service import get_verification_deadline_minutes
from app.services.ride_service import (
    calculate_verification_deadline,
    create_ride_with_charge,
    get_ride_by_idempotency,
    get_unified_events,
    parse_pickup_time,
    resolve_ride_timezone,
    send_ride_notifications,
)
from app.utils.pagination import decode_cursor, encode_cursor

logger = logging.getLogger(__name__)

router = APIRouter(tags=["rides"])


@router.post("/rides", response_model=CreateRideResponse, status_code=201)
@limiter.limit("30/minute", key_func=get_device_key)
async def create_ride_endpoint(
    request: Request,
    body: CreateRideRequest,
    response: Response,
    device: PairedDevice = Depends(verify_device),
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    """Create a ride event from search device.

    Idempotency: if a ride with the same idempotency_key already exists
    for this user, returns 200 with the existing ride_id (no FCM push).

    New rides return 201 and trigger an FCM push to the main app.
    FCM failures are logged but do not block the response.
    """
    # 1. Check idempotency — return existing ride if found
    existing = await get_ride_by_idempotency(db, device.user_id, body.idempotency_key)
    if existing:
        response.status_code = 200
        return CreateRideResponse(ride_id=existing.id)

    # 2. Parse timezone, pickup time, and verification deadline
    tz, timezone_fallback = resolve_ride_timezone(body.timezone)
    pickup_dt = parse_pickup_time(body.ride_data.pickup_time, tz)
    if pickup_dt is None:
        logger.warning(
            "RIDE_PICKUP_PARSE_FAILED: pickup_time=%r, timezone=%s",
            body.ride_data.pickup_time,
            body.timezone,
        )

    deadline_minutes = await get_verification_deadline_minutes(db, redis)
    verification_deadline = calculate_verification_deadline(pickup_dt, deadline_minutes)

    # 3. Create ride, charge credits, and commit atomically
    ride_data_dict = body.ride_data.model_dump()
    try:
        ride, charged, new_balance = await create_ride_with_charge(
            db,
            redis,
            user_id=device.user_id,
            idempotency_key=body.idempotency_key,
            event_type=body.event_type,
            ride_data=ride_data_dict,
            ride_hash=body.ride_hash,
            price=body.ride_data.price,
            verification_deadline=verification_deadline,
        )
    except IntegrityError:
        await db.rollback()
        existing = await get_ride_by_idempotency(db, device.user_id, body.idempotency_key)
        if existing:
            response.status_code = 200
            return CreateRideResponse(ride_id=existing.id)
        raise

    # 4. Log warnings for edge cases
    if charged == 0:
        logger.warning(
            "RIDE_NOT_CHARGED: user_id=%s, ride_id=%s, reason=zero_balance",
            device.user_id,
            ride.id,
        )
    if timezone_fallback:
        logger.warning(
            "RIDE_TIMEZONE_FALLBACK: ride_id=%s, received_timezone=%s",
            ride.id,
            body.timezone,
        )

    # 5. Send FCM notifications (best-effort)
    await send_ride_notifications(db, device.user_id, ride, charged, new_balance, ride_data_dict)

    return CreateRideResponse(ride_id=ride.id)


@router.get("/rides/events", response_model=EventsListResponse)
@limiter.limit("60/minute", key_func=get_user_key)
async def get_ride_events(
    request: Request,
    response: Response,
    limit: int = Query(default=20, ge=1, le=100),
    cursor: str | None = Query(default=None),
    since: str | None = Query(default=None),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get unified event feed for the authenticated user.

    Returns a mixed chronological feed of ride events (with billing
    info) and credit events (purchases, bonuses, adjustments).
    Uses cursor-based pagination ordered by created_at descending.
    """
    # Parse cursor
    decoded_cursor = None
    if cursor is not None:
        try:
            decoded_cursor = decode_cursor(cursor)
        except ValueError:
            return Response(
                content='{"error":{"code":"INVALID_CURSOR","message":"Invalid cursor format"}}',
                status_code=400,
                media_type="application/json",
            )

    # Parse since
    since_dt = None
    if since is not None:
        try:
            since_dt = datetime.fromisoformat(since)
        except ValueError:
            return Response(
                content='{"error":{"code":"INVALID_SINCE","message":"Invalid since format, expected ISO 8601 datetime"}}',
                status_code=400,
                media_type="application/json",
            )

    rows, has_more = await get_unified_events(
        db,
        current_user.id,
        limit,
        cursor=decoded_cursor,
        since=since_dt,
    )

    # Build typed event responses from raw rows
    events: list[RideEventResponse | CreditEventResponse] = []
    for row in rows:
        if row.event_kind == "ride":
            events.append(
                RideEventResponse(
                    id=row.id,
                    event_type=row.event_type,
                    ride_data=row.ride_data,
                    credits_charged=row.credits_charged,
                    credits_refunded=row.credits_refunded,
                    verification_status=row.verification_status,
                    created_at=row.created_at,
                )
            )
        else:
            events.append(
                CreditEventResponse(
                    id=row.id,
                    credit_type=row.credit_type,
                    amount=row.amount,
                    balance_after=row.balance_after,
                    description=row.description,
                    created_at=row.created_at,
                )
            )

    # Build next_cursor from the last event
    next_cursor = None
    if has_more and events:
        last = events[-1]
        next_cursor = encode_cursor(last.created_at, last.event_kind, last.id)

    return EventsListResponse(
        events=events,
        next_cursor=next_cursor,
        has_more=has_more,
    )
