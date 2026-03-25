import asyncio
import json
import logging
from uuid import UUID

import firebase_admin
from firebase_admin import credentials, exceptions, messaging
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.user import User
from app.schemas.fcm import (
    NotificationType,
    create_balance_adjusted_payload,
    create_credits_depleted_payload,
    create_credits_low_payload,
    create_ride_credit_refunded_payload,
    create_search_update_required_payload,
)
from app.services.config_service import get_push_templates

logger = logging.getLogger(__name__)


def initialize_firebase() -> None:
    """Initialize Firebase Admin SDK with service account credentials.

    Supports two credential sources (checked in order):
    1. FIREBASE_CREDENTIALS_PATH - path to a service account JSON file
    2. FIREBASE_CREDENTIALS_JSON - JSON string with service account credentials

    Raises ValueError if neither credential source is configured.
    Skips initialization if Firebase app is already initialized.
    """
    if firebase_admin._apps:
        logger.debug("Firebase already initialized, skipping")
        return

    if settings.FIREBASE_CREDENTIALS_PATH:
        cred = credentials.Certificate(settings.FIREBASE_CREDENTIALS_PATH)
        logger.info("Firebase initialized with credentials file")
    elif settings.FIREBASE_CREDENTIALS_JSON:
        cred_dict = json.loads(settings.FIREBASE_CREDENTIALS_JSON)
        cred = credentials.Certificate(cred_dict)
        logger.info("Firebase initialized with JSON credentials")
    else:
        raise ValueError(
            "Firebase credentials not configured. "
            "Set FIREBASE_CREDENTIALS_PATH or FIREBASE_CREDENTIALS_JSON."
        )

    firebase_admin.initialize_app(cred)


async def update_user_fcm_token(db: AsyncSession, user_id: UUID, fcm_token: str) -> None:
    """Update the FCM token for a user."""
    await db.execute(update(User).where(User.id == user_id).values(fcm_token=fcm_token))
    await db.commit()
    logger.info("Updated FCM token for user %s", user_id)


async def clear_fcm_token(db: AsyncSession, user_id: UUID) -> None:
    """Clear the FCM token for a user when the token is no longer valid."""
    await db.execute(update(User).where(User.id == user_id).values(fcm_token=None))
    await db.commit()
    logger.info("Cleared FCM token for user %s", user_id)


async def send_push(
    db: AsyncSession,
    fcm_token: str,
    notification_type: str,
    data: dict,
    user_id: UUID,
    language: str = "en",
) -> bool:
    """Send a data-only FCM push notification with retry and exponential backoff.

    Resolves title/body from push notification templates in AppConfig,
    substitutes placeholders with values from the data payload.

    Handles specific FCM errors:
    - UnregisteredError: Token expired/invalid, clears from DB, no retry.
    - InvalidArgumentError: Bad token format, clears from DB, no retry.
    - Other errors: Retries up to 3 times with exponential backoff (1s, 3s, 9s).

    Args:
        db: Async database session for token cleanup on permanent errors.
        fcm_token: The device's FCM registration token.
        notification_type: Type string added to data payload as 'type' field.
        data: Notification payload. All values are converted to strings.
        user_id: The user's ID (for logging and token cleanup).
        language: User's language code for template resolution (default: "en").

    Returns:
        True if sent successfully, False on permanent error or after all retries.
    """
    string_data = {k: str(v) for k, v in data.items()}
    string_data["type"] = notification_type

    # Resolve title/body from templates
    try:
        from app.redis import redis_client

        templates = await get_push_templates(db, redis_client)
        template = templates.get_template(notification_type, language)
        if template:
            string_data["title"] = template.title.format_map(
                {k: v for k, v in string_data.items()}
            )
            string_data["body"] = template.body.format_map({k: v for k, v in string_data.items()})
    except Exception:
        logger.warning("Failed to resolve push template for %s", notification_type, exc_info=True)

    message = messaging.Message(data=string_data, token=fcm_token)

    loop = asyncio.get_running_loop()

    for attempt in range(3):
        try:
            await loop.run_in_executor(None, messaging.send, message)
            logger.info(
                "FCM push sent on attempt %d for user %s, type=%s",
                attempt + 1,
                user_id,
                notification_type,
            )
            return True
        except messaging.UnregisteredError:
            logger.warning(
                "FCM token unregistered for user %s, clearing token",
                user_id,
            )
            await clear_fcm_token(db, user_id)
            return False
        except exceptions.InvalidArgumentError:
            logger.warning(
                "FCM invalid argument for user %s, clearing token",
                user_id,
            )
            await clear_fcm_token(db, user_id)
            return False
        except Exception as e:
            if attempt < 2:
                wait_time = 3**attempt
                logger.warning(
                    "FCM push attempt %d failed for user %s: %s. Retrying in %ds",
                    attempt + 1,
                    user_id,
                    e,
                    wait_time,
                )
                await asyncio.sleep(wait_time)
            else:
                logger.error(
                    "FCM push failed after 3 attempts for user %s: %s",
                    user_id,
                    e,
                )
                return False

    return False  # Unreachable; satisfies type checker


async def _get_user_push_info(db: AsyncSession, user_id: UUID) -> tuple[str | None, str]:
    """Fetch user's FCM token and language for push notifications.

    Returns (fcm_token, language). fcm_token may be None.
    """
    result = await db.execute(select(User.fcm_token, User.language).where(User.id == user_id))
    row = result.one_or_none()
    if row is None:
        return None, "en"
    return row.fcm_token, row.language or "en"


async def send_ride_credit_refunded(
    db: AsyncSession,
    user_id: UUID,
    ride_id: UUID,
    credits_refunded: int,
    new_balance: int,
) -> None:
    """Send RIDE_CREDIT_REFUNDED push when a ride is verified as CANCELLED.

    Fire-and-forget: exceptions are caught and logged, never propagated.
    """
    try:
        fcm_token, language = await _get_user_push_info(db, user_id)
        if not fcm_token:
            logger.debug(
                "No FCM token for user %s, skipping RIDE_CREDIT_REFUNDED push",
                user_id,
            )
            return

        payload = create_ride_credit_refunded_payload(
            ride_id=ride_id,
            credits_refunded=credits_refunded,
            new_balance=new_balance,
        )
        await send_push(
            db, fcm_token, NotificationType.RIDE_CREDIT_REFUNDED, payload, user_id, language
        )
        logger.info("FCM_RIDE_CREDIT_REFUNDED_SENT: user_id=%s, ride_id=%s", user_id, ride_id)
    except Exception:
        logger.warning(
            "FCM RIDE_CREDIT_REFUNDED failed for user %s, ride_id=%s",
            user_id,
            ride_id,
            exc_info=True,
        )


async def send_credits_depleted(db: AsyncSession, user_id: UUID) -> None:
    """Send CREDITS_DEPLETED push notification when user balance reaches zero.

    Fire-and-forget: exceptions are caught and logged, never propagated.
    """
    try:
        fcm_token, language = await _get_user_push_info(db, user_id)
        if not fcm_token:
            logger.debug("No FCM token for user %s, skipping CREDITS_DEPLETED push", user_id)
            return

        payload = create_credits_depleted_payload()
        await send_push(
            db, fcm_token, NotificationType.CREDITS_DEPLETED, payload, user_id, language
        )
        logger.info("FCM_CREDITS_DEPLETED_SENT: user_id=%s", user_id)
    except Exception:
        logger.warning("FCM CREDITS_DEPLETED failed for user %s", user_id, exc_info=True)


async def send_credits_low(
    db: AsyncSession,
    user_id: UUID,
    balance: int,
    threshold: int,
) -> None:
    """Send CREDITS_LOW push notification when user balance is below threshold.

    Fire-and-forget: exceptions are caught and logged, never propagated.
    """
    try:
        fcm_token, language = await _get_user_push_info(db, user_id)
        if not fcm_token:
            logger.debug("No FCM token for user %s, skipping CREDITS_LOW push", user_id)
            return

        payload = create_credits_low_payload(balance=balance, threshold=threshold)
        await send_push(db, fcm_token, NotificationType.CREDITS_LOW, payload, user_id, language)
        logger.info(
            "FCM_CREDITS_LOW_SENT: user_id=%s, balance=%d, threshold=%d",
            user_id,
            balance,
            threshold,
        )
    except Exception:
        logger.warning("FCM CREDITS_LOW failed for user %s", user_id, exc_info=True)


async def send_balance_adjusted(
    db: AsyncSession,
    user_id: UUID,
    amount: int,
    new_balance: int,
) -> None:
    """Send BALANCE_ADJUSTED push notification after admin balance adjustment.

    Fire-and-forget: exceptions are caught and logged, never propagated.
    """
    try:
        fcm_token, language = await _get_user_push_info(db, user_id)
        if not fcm_token:
            logger.debug("No FCM token for user %s, skipping BALANCE_ADJUSTED push", user_id)
            return

        payload = create_balance_adjusted_payload(amount=amount, new_balance=new_balance)
        await send_push(
            db, fcm_token, NotificationType.BALANCE_ADJUSTED, payload, user_id, language
        )
        logger.info(
            "FCM_BALANCE_ADJUSTED_SENT: user_id=%s, amount=%+d, new_balance=%d",
            user_id,
            amount,
            new_balance,
        )
    except Exception:
        logger.warning("FCM BALANCE_ADJUSTED failed for user %s", user_id, exc_info=True)


async def send_search_update_required(
    db: AsyncSession,
    user_id: UUID,
    min_version: str,
) -> None:
    """Send SEARCH_UPDATE_REQUIRED push to main app when search app is outdated.

    Fire-and-forget: exceptions are caught and logged, never propagated.
    """
    try:
        fcm_token, language = await _get_user_push_info(db, user_id)
        if not fcm_token:
            logger.debug("No FCM token for user %s, skipping SEARCH_UPDATE_REQUIRED push", user_id)
            return

        payload = create_search_update_required_payload(min_version=min_version)
        await send_push(
            db, fcm_token, NotificationType.SEARCH_UPDATE_REQUIRED, payload, user_id, language
        )
        logger.info("FCM_SEARCH_UPDATE_REQUIRED_SENT: user_id=%s", user_id)
    except Exception:
        logger.warning("FCM SEARCH_UPDATE_REQUIRED failed for user %s", user_id, exc_info=True)
