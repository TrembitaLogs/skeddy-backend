import logging
from datetime import datetime
from uuid import UUID

from firebase_admin import exceptions as firebase_exceptions
from redis.asyncio import Redis
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.ride import Ride
from app.schemas.fcm import NotificationType, create_ride_accepted_payload
from app.services.cluster_service import penalize_device_in_cluster
from app.services.credit_service import cache_balance, charge_credits, get_ride_credit_cost
from app.services.fcm_service import send_credits_depleted, send_push
from app.services.ride_service.crud import create_ride, get_user_fcm_token

logger = logging.getLogger(__name__)


async def create_ride_with_charge(
    db: AsyncSession,
    redis: Redis,
    user_id: UUID,
    idempotency_key: str,
    event_type: str,
    ride_data: dict,
    ride_hash: str,
    price: float,
    verification_deadline: datetime | None,
    device_id: str | None = None,
) -> tuple[Ride, int, int]:
    """Create a ride, charge credits, and commit atomically.

    When ``device_id`` is provided, penalizes the device in its cluster
    after a successful commit so that other cluster members get priority
    in the next search cycle.

    Returns:
        Tuple of (ride, credits_charged, new_balance).

    Raises:
        IntegrityError: If idempotency race condition occurs (caller handles).
    """
    ride = await create_ride(
        db,
        user_id=user_id,
        idempotency_key=idempotency_key,
        event_type=event_type,
        ride_data=ride_data,
        ride_hash=ride_hash,
        verification_deadline=verification_deadline,
    )

    credits_cost = await get_ride_credit_cost(price, db, redis)
    charged, new_balance = await charge_credits(user_id, credits_cost, ride.id, db, redis)
    ride.credits_charged = charged

    await db.commit()

    if charged > 0:
        await cache_balance(user_id, new_balance, redis)

    if device_id:
        try:
            await penalize_device_in_cluster(device_id, redis)
        except Exception:
            logger.warning(
                "Failed to penalize device %s in cluster after ride creation",
                device_id,
                exc_info=True,
            )

    return ride, charged, new_balance


async def send_ride_notifications(
    db: AsyncSession,
    user_id: UUID,
    ride: Ride,
    charged: int,
    new_balance: int,
    ride_data: dict,
) -> None:
    """Send post-commit FCM notifications for a new ride (best-effort)."""
    if charged > 0 and new_balance == 0:
        try:
            await send_credits_depleted(db, user_id)
        except (firebase_exceptions.FirebaseError, OperationalError) as exc:
            logger.warning("FCM CREDITS_DEPLETED push failed for user %s: %s", user_id, exc)

    try:
        fcm_token = await get_user_fcm_token(db, user_id)
        if fcm_token:
            payload = create_ride_accepted_payload(
                ride_id=ride.id,
                price=ride_data.get("price", 0),
                pickup_time=ride_data.get("pickup_time", ""),
                pickup_location=ride_data.get("pickup_location", ""),
                dropoff_location=ride_data.get("dropoff_location", ""),
            )
            await send_push(
                db,
                fcm_token,
                NotificationType.RIDE_ACCEPTED,
                payload,
                user_id,
            )
    except (firebase_exceptions.FirebaseError, OperationalError) as exc:
        logger.warning("FCM push failed for ride %s: %s", ride.id, exc)
