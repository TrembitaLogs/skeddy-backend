import logging
from datetime import datetime

from fastapi import APIRouter, Depends, Query, Request, Response
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.dependencies.auth import get_current_user
from app.dependencies.device_auth import verify_device
from app.middleware.rate_limiter import get_device_key, get_user_key, limiter
from app.models.paired_device import PairedDevice
from app.models.user import User
from app.schemas.fcm import NotificationType, create_ride_accepted_payload
from app.schemas.rides import (
    CreateRideRequest,
    CreateRideResponse,
    RideEventResponse,
    RideEventsListResponse,
)
from app.services.fcm_service import send_push
from app.services.ride_service import (
    create_ride,
    get_ride_by_idempotency,
    get_user_fcm_token,
    get_user_ride_events,
)

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

    # 2. Create new ride
    try:
        ride = await create_ride(
            db,
            user_id=device.user_id,
            idempotency_key=body.idempotency_key,
            event_type=body.event_type,
            ride_data=body.ride_data.model_dump(),
        )
        await db.commit()
    except IntegrityError:
        # Race condition: concurrent request already inserted this ride
        await db.rollback()
        existing = await get_ride_by_idempotency(db, device.user_id, body.idempotency_key)
        if existing:
            response.status_code = 200
            return CreateRideResponse(ride_id=existing.id)
        raise

    # 3. FCM push notification (graceful — failure does not block response)
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
    except Exception as exc:
        logger.warning("FCM push failed for ride %s: %s", ride.id, exc)

    return CreateRideResponse(ride_id=ride.id)


@router.get("/rides/events", response_model=RideEventsListResponse)
@limiter.limit("60/minute", key_func=get_user_key)
async def get_ride_events(
    request: Request,
    response: Response,
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    since: datetime | None = Query(default=None),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get paginated ride events for the authenticated user.

    Returns events ordered by created_at descending (newest first).
    Uses offset-based pagination with total count.
    Optionally filters by created_at >= since (ISO 8601 datetime).
    """
    events, total = await get_user_ride_events(db, current_user.id, limit, offset, since=since)

    return RideEventsListResponse(
        events=[
            RideEventResponse(
                id=e.id,
                event_type=e.event_type,
                ride_data=e.ride_data,
                created_at=e.created_at,
            )
            for e in events
        ],
        total=total,
        limit=limit,
        offset=offset,
    )
