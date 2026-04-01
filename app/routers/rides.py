import logging
from datetime import datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import APIRouter, Depends, Query, Request, Response
from firebase_admin import exceptions as firebase_exceptions
from redis.asyncio import Redis
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.dependencies.auth import get_current_user
from app.dependencies.device_auth import verify_device
from app.middleware.rate_limiter import get_device_key, get_user_key, limiter
from app.models.paired_device import PairedDevice
from app.models.user import User
from app.redis import get_redis
from app.schemas.fcm import NotificationType, create_ride_accepted_payload
from app.schemas.rides import (
    CreateRideRequest,
    CreateRideResponse,
    CreditEventResponse,
    EventsListResponse,
    RideEventResponse,
)
from app.services.config_service import get_verification_deadline_minutes
from app.services.credit_service import (
    cache_balance,
    charge_credits,
    get_ride_credit_cost,
)
from app.services.fcm_service import send_credits_depleted, send_push
from app.services.ride_service import (
    calculate_verification_deadline,
    create_ride,
    get_ride_by_idempotency,
    get_unified_events,
    get_user_fcm_token,
    parse_pickup_time,
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

    # 2. Parse timezone (fallback to UTC with deferred warning)
    timezone_fallback = False
    try:
        tz = ZoneInfo(body.timezone)
    except (ZoneInfoNotFoundError, KeyError, ValueError):
        tz = ZoneInfo("UTC")
        timezone_fallback = True

    # 3. Parse pickup_time and calculate verification_deadline
    pickup_dt = parse_pickup_time(body.ride_data.pickup_time, tz)
    if pickup_dt is None:
        logger.warning(
            "RIDE_PICKUP_PARSE_FAILED: pickup_time=%r, timezone=%s",
            body.ride_data.pickup_time,
            body.timezone,
        )

    deadline_minutes = await get_verification_deadline_minutes(db, redis)
    verification_deadline = calculate_verification_deadline(pickup_dt, deadline_minutes)

    # 4. Create new ride (flush only — commit after credit charging)
    try:
        ride = await create_ride(
            db,
            user_id=device.user_id,
            idempotency_key=body.idempotency_key,
            event_type=body.event_type,
            ride_data=body.ride_data.model_dump(),
            ride_hash=body.ride_hash,
            verification_deadline=verification_deadline,
        )
    except IntegrityError:
        # Race condition: concurrent request already inserted this ride
        await db.rollback()
        existing = await get_ride_by_idempotency(db, device.user_id, body.idempotency_key)
        if existing:
            response.status_code = 200
            return CreateRideResponse(ride_id=existing.id)
        raise

    # 5. Charge credits based on ride price (PRD section 5)
    credits_cost = await get_ride_credit_cost(body.ride_data.price, db, redis)
    charged, new_balance = await charge_credits(device.user_id, credits_cost, ride.id, db, redis)
    ride.credits_charged = charged

    # 6. Commit ride + credit changes atomically
    await db.commit()

    # 7. Write-through Redis cache after successful commit (PRD section 7)
    if charged > 0:
        await cache_balance(device.user_id, new_balance, redis)

    # 8. FCM push CREDITS_DEPLETED if balance reached zero (PRD section 5)
    if charged > 0 and new_balance == 0:
        try:
            await send_credits_depleted(db, device.user_id)
        except (firebase_exceptions.FirebaseError, OperationalError) as exc:
            logger.warning("FCM CREDITS_DEPLETED push failed for user %s: %s", device.user_id, exc)

    # 9. Log if ride was not charged due to zero balance (PRD section 5)
    if charged == 0:
        logger.warning(
            "RIDE_NOT_CHARGED: user_id=%s, ride_id=%s, cost=%d, reason=zero_balance",
            device.user_id,
            ride.id,
            credits_cost,
        )

    # 10. Log timezone fallback with ride_id (PRD section 6)
    if timezone_fallback:
        logger.warning(
            "RIDE_TIMEZONE_FALLBACK: ride_id=%s, received_timezone=%s",
            ride.id,
            body.timezone,
        )

    # 11. FCM push notification (graceful — failure does not block response)
    try:
        fcm_token = await get_user_fcm_token(db, device.user_id)
        if fcm_token:
            payload = create_ride_accepted_payload(
                ride_id=ride.id,
                price=body.ride_data.price,
                pickup_time=body.ride_data.pickup_time,
                pickup_location=body.ride_data.pickup_location,
                dropoff_location=body.ride_data.dropoff_location,
            )
            await send_push(
                db,
                fcm_token,
                NotificationType.RIDE_ACCEPTED,
                payload,
                device.user_id,
            )
    except (firebase_exceptions.FirebaseError, OperationalError) as exc:
        logger.warning("FCM push failed for ride %s: %s", ride.id, exc)

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
