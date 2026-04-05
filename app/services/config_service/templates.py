"""Push notification and email template config."""

import json
import logging

from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.email_template import EmailTemplate as EmailTemplateModel
from app.models.push_template import PushTemplate as PushTemplateModel
from app.schemas.push_templates import PushNotificationTemplatesConfig
from app.services.config_service.cache import (
    CACHE_KEY_EMAIL_TEMPLATES,
    CACHE_KEY_PUSH_TEMPLATES,
    CACHE_TTL,
    _memory_cache,
)

logger = logging.getLogger(__name__)

DEFAULT_PUSH_TEMPLATES: dict[str, dict[str, dict[str, str]]] = {
    "RIDE_ACCEPTED": {
        "en": {
            "title": "New Ride",
            "body": "Ride from {pickup_location} to {dropoff_location}, ${price}",
        },
    },
    "SEARCH_OFFLINE": {
        "en": {
            "title": "Device Offline",
            "body": "Your search device has been offline since {last_ping_at}",
        },
    },
    "CREDITS_DEPLETED": {
        "en": {
            "title": "Credits Depleted",
            "body": "Your credit balance is empty. Top up to continue.",
        },
    },
    "CREDITS_LOW": {
        "en": {
            "title": "Low Credits",
            "body": "Your balance is {balance} credits. Minimum for a ride is {threshold}.",
        },
    },
    "RIDE_CREDIT_REFUNDED": {
        "en": {
            "title": "Credit Refunded",
            "body": "{credits_refunded} credit(s) refunded. New balance: {new_balance}",
        },
    },
    "BALANCE_ADJUSTED": {
        "en": {
            "title": "Balance Updated",
            "body": "Your balance was adjusted by {amount}. New balance: {new_balance}",
        },
    },
    "SEARCH_UPDATE_REQUIRED": {
        "en": {
            "title": "Update Required",
            "body": "Your search device needs an update to version {min_version}.",
        },
    },
}


async def get_push_templates(db: AsyncSession, redis: Redis) -> PushNotificationTemplatesConfig:
    """Return push notification templates from push_templates table.

    Resolution order: Redis cache -> DB -> DEFAULT_PUSH_TEMPLATES.
    Returns a validated ``PushNotificationTemplatesConfig``.
    """
    raw_json: str | None = None

    # 1. Try Redis cache
    try:
        cached = await redis.get(CACHE_KEY_PUSH_TEMPLATES)
        if cached is not None:
            raw_json = cached
    except RedisError:
        logger.warning(
            "Redis unavailable when reading %s, falling back to DB",
            CACHE_KEY_PUSH_TEMPLATES,
        )
        mem_value = _memory_cache.get(CACHE_KEY_PUSH_TEMPLATES)
        if mem_value is not None:
            return mem_value  # type: ignore[no-any-return]

    # 2. Try DB (only when Redis miss)
    if raw_json is None:
        result = await db.execute(select(PushTemplateModel))
        rows = result.scalars().all()

        if rows:
            data = {}
            for row in rows:
                data[row.notification_type] = {
                    "en": {"title": row.title_en, "body": row.body_en},
                    "es": {"title": row.title_es, "body": row.body_es},
                }
            raw_json = json.dumps(data)
            try:
                await redis.setex(CACHE_KEY_PUSH_TEMPLATES, CACHE_TTL, raw_json)
            except RedisError:
                logger.warning(
                    "Redis unavailable when caching %s",
                    CACHE_KEY_PUSH_TEMPLATES,
                )

    # 3. Parse and validate
    if raw_json is not None:
        try:
            parsed = json.loads(raw_json)
            value = PushNotificationTemplatesConfig.model_validate(parsed)
            _memory_cache[CACHE_KEY_PUSH_TEMPLATES] = value
            return value
        except (json.JSONDecodeError, ValueError, TypeError) as exc:
            logger.warning("Invalid push templates, using defaults: %s", exc)

    # 4. Fallback to defaults
    return PushNotificationTemplatesConfig.model_validate(DEFAULT_PUSH_TEMPLATES)


async def invalidate_push_templates(redis: Redis) -> None:
    """Invalidate push templates cache after admin edit."""
    _memory_cache.pop(CACHE_KEY_PUSH_TEMPLATES, None)
    try:
        await redis.delete(CACHE_KEY_PUSH_TEMPLATES)
    except RedisError:
        logger.warning("Redis unavailable when invalidating %s", CACHE_KEY_PUSH_TEMPLATES)


async def get_email_templates(
    db: AsyncSession, redis: Redis
) -> dict[str, dict[str, dict[str, str]]]:
    """Return email templates from email_templates table.

    Resolution order: Redis cache -> DB.
    Returns: {email_type: {lang: {subject, body}}}
    """
    raw_json: str | None = None

    # 1. Try Redis cache
    try:
        cached = await redis.get(CACHE_KEY_EMAIL_TEMPLATES)
        if cached is not None:
            raw_json = cached
    except RedisError:
        logger.warning("Redis unavailable when reading %s", CACHE_KEY_EMAIL_TEMPLATES)
        mem_value = _memory_cache.get(CACHE_KEY_EMAIL_TEMPLATES)
        if mem_value is not None:
            return mem_value  # type: ignore[no-any-return]

    # 2. Try DB
    if raw_json is None:
        result = await db.execute(select(EmailTemplateModel))
        rows = result.scalars().all()

        if rows:
            data = {}
            for row in rows:
                data[row.email_type] = {
                    "en": {"subject": row.subject_en, "body": row.body_en},
                    "es": {"subject": row.subject_es, "body": row.body_es},
                }
            raw_json = json.dumps(data)
            try:
                await redis.setex(CACHE_KEY_EMAIL_TEMPLATES, CACHE_TTL, raw_json)
            except RedisError:
                logger.warning("Redis unavailable when caching %s", CACHE_KEY_EMAIL_TEMPLATES)

    # 3. Parse
    if raw_json is not None:
        try:
            parsed = json.loads(raw_json)
            _memory_cache[CACHE_KEY_EMAIL_TEMPLATES] = parsed
            return dict(parsed)
        except (json.JSONDecodeError, ValueError) as exc:
            logger.warning("Invalid email templates JSON: %s", exc)

    return {}


async def invalidate_email_templates(redis: Redis) -> None:
    """Invalidate email templates cache after admin edit."""
    _memory_cache.pop(CACHE_KEY_EMAIL_TEMPLATES, None)
    try:
        await redis.delete(CACHE_KEY_EMAIL_TEMPLATES)
    except RedisError:
        logger.warning("Redis unavailable when invalidating %s", CACHE_KEY_EMAIL_TEMPLATES)
