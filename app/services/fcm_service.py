import asyncio
import json
import logging
from uuid import UUID

import firebase_admin
from firebase_admin import credentials, exceptions, messaging
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.user import User

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
) -> bool:
    """Send a data-only FCM push notification with retry and exponential backoff.

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

    Returns:
        True if sent successfully, False on permanent error or after all retries.
    """
    string_data = {k: str(v) for k, v in data.items()}
    string_data["type"] = notification_type
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
