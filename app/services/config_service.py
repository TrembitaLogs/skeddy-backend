import contextlib
import json
import logging
from dataclasses import dataclass
from typing import Any

from cachetools import TTLCache
from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.app_config import AppConfig
from app.models.push_template import PushTemplate as PushTemplateModel
from app.schemas.billing_config import (
    CreditProductsConfig,
    RideCreditTiersConfig,
)
from app.schemas.push_templates import PushNotificationTemplatesConfig

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cache keys & TTL
# ---------------------------------------------------------------------------

CACHE_KEY = "app_config:min_search_app_version"
CACHE_KEY_INTERVAL = "app_config:search_interval"
CACHE_KEY_VERIFICATION_DEADLINE = "app_config:verification_deadline"
CACHE_KEY_VERIFICATION_CHECK_INTERVAL = "app_config:verification_check_interval"
CACHE_KEY_CREDIT_PRODUCTS = "app_config:credit_products"
CACHE_KEY_RIDE_CREDIT_TIERS = "app_config:ride_credit_tiers"
CACHE_KEY_REGISTRATION_BONUS = "app_config:registration_bonus"
CACHE_KEY_PUSH_TEMPLATES = "app_config:push_templates"
CACHE_TTL = 300  # 5 minutes (Redis)
IN_MEMORY_TTL = 600  # 10 minutes (fallback when Redis is unavailable)

# ---------------------------------------------------------------------------
# Default values (hardcoded fallbacks when key absent from Redis and DB)
# ---------------------------------------------------------------------------

DEFAULT_VERIFICATION_DEADLINE_MINUTES = 30
DEFAULT_VERIFICATION_CHECK_INTERVAL_MINUTES = 60
DEFAULT_REGISTRATION_BONUS_CREDITS = 10

DEFAULT_CREDIT_PRODUCTS: list[dict] = [
    {"product_id": "credits_10", "credits": 10, "price_usd": 10.00},
    {"product_id": "credits_25", "credits": 25, "price_usd": 22.00},
    {"product_id": "credits_50", "credits": 50, "price_usd": 40.00},
    {"product_id": "credits_100", "credits": 100, "price_usd": 80.00},
]

DEFAULT_RIDE_CREDIT_TIERS: list[dict] = [
    {"max_price": 20.0, "credits": 1},
    {"max_price": 50.0, "credits": 2},
    {"max_price": None, "credits": 3},
]

# ---------------------------------------------------------------------------
# In-memory fallback cache (safety net when Redis is unavailable)
# ---------------------------------------------------------------------------

_memory_cache: TTLCache[str, Any] = TTLCache(maxsize=64, ttl=IN_MEMORY_TTL)

# Mapping: AppConfig DB key → Redis cache key(s) to invalidate.
# Some DB keys share a composite Redis key (e.g. requests_per_day/hour → search_interval).
_DB_KEY_TO_CACHE_KEYS: dict[str, list[str]] = {
    "min_search_app_version": [CACHE_KEY],
    "requests_per_day": [CACHE_KEY_INTERVAL],
    "requests_per_hour": [CACHE_KEY_INTERVAL],
    "verification_deadline_minutes": [CACHE_KEY_VERIFICATION_DEADLINE],
    "verification_check_interval_minutes": [CACHE_KEY_VERIFICATION_CHECK_INTERVAL],
    "credit_products": [CACHE_KEY_CREDIT_PRODUCTS],
    "ride_credit_tiers": [CACHE_KEY_RIDE_CREDIT_TIERS],
    "registration_bonus_credits": [CACHE_KEY_REGISTRATION_BONUS],
    "push_notification_templates": [CACHE_KEY_PUSH_TEMPLATES],
}


async def invalidate_config(db_key: str, redis: Redis) -> None:
    """Remove a config value from both Redis and in-memory cache.

    Called from SQLAdmin ``after_model_change`` when an AppConfig entry is
    created or updated.  ``db_key`` is the ``AppConfig.key`` column value
    (e.g. ``"credit_products"``).
    """
    cache_keys = _DB_KEY_TO_CACHE_KEYS.get(db_key, [])
    for cache_key in cache_keys:
        _memory_cache.pop(cache_key, None)
        try:
            await redis.delete(cache_key)
        except RedisError:
            logger.warning("Redis unavailable when invalidating %s", cache_key)


async def get_min_search_version(db: AsyncSession, redis: Redis) -> str:
    """Return the minimum search app version.

    Resolution order: Redis cache -> DB -> settings fallback.
    Redis failures are handled gracefully (falls back to DB).
    """
    # 1. Try Redis cache
    try:
        cached = await redis.get(CACHE_KEY)
        if cached is not None:
            value = str(cached)
            _memory_cache[CACHE_KEY] = value
            return value
    except RedisError:
        logger.warning("Redis unavailable when reading %s, falling back to DB", CACHE_KEY)
        mem_value = _memory_cache.get(CACHE_KEY)
        if mem_value is not None:
            return str(mem_value)

    # 2. Try DB
    result = await db.execute(
        select(AppConfig.value).where(AppConfig.key == "min_search_app_version")
    )
    row = result.scalar_one_or_none()

    if row is not None:
        _memory_cache[CACHE_KEY] = row
        # Cache the value for next time
        try:
            await redis.setex(CACHE_KEY, CACHE_TTL, row)
        except RedisError:
            logger.warning("Redis unavailable when caching %s", CACHE_KEY)
        return row

    # 3. Fallback to settings
    return settings.MIN_SEARCH_APP_VERSION


async def set_min_search_version(db: AsyncSession, redis: Redis, version: str) -> None:
    """Upsert the minimum search app version and invalidate cache."""
    stmt = (
        insert(AppConfig)
        .values(key="min_search_app_version", value=version)
        .on_conflict_do_update(index_elements=["key"], set_={"value": version})
    )
    await db.execute(stmt)
    await db.commit()

    _memory_cache.pop(CACHE_KEY, None)
    try:
        await redis.delete(CACHE_KEY)
    except RedisError:
        logger.warning("Redis unavailable when invalidating %s", CACHE_KEY)


async def get_search_interval_config(
    db: AsyncSession, redis: Redis
) -> tuple[int, list[float]] | None:
    """Return (requests_per_day, requests_per_hour) for dynamic interval calculation.

    Resolution order: Redis cache -> DB -> None (caller uses flat default).
    Both values are cached together as a single JSON blob to avoid two DB queries.
    Redis failures are handled gracefully (falls back to DB).
    """
    # 1. Try Redis cache
    try:
        cached = await redis.get(CACHE_KEY_INTERVAL)
        if cached is not None:
            data = json.loads(cached)
            value = (data["rpd"], data["rph"])
            _memory_cache[CACHE_KEY_INTERVAL] = value
            return value
    except RedisError:
        logger.warning(
            "Redis unavailable when reading %s, falling back to DB",
            CACHE_KEY_INTERVAL,
        )
        mem_value = _memory_cache.get(CACHE_KEY_INTERVAL)
        if mem_value is not None:
            return mem_value  # type: ignore[no-any-return]

    # 2. Try DB — fetch both keys in one query
    result = await db.execute(
        select(AppConfig.key, AppConfig.value).where(
            AppConfig.key.in_(["requests_per_day", "requests_per_hour"])
        )
    )
    rows = {row.key: row.value for row in result.all()}

    if "requests_per_day" not in rows or "requests_per_hour" not in rows:
        return None

    rpd = int(rows["requests_per_day"])
    rph = json.loads(rows["requests_per_hour"])
    value = (rpd, rph)
    _memory_cache[CACHE_KEY_INTERVAL] = value

    # Cache for next time
    cache_blob = json.dumps({"rpd": rpd, "rph": rph})
    try:
        await redis.setex(CACHE_KEY_INTERVAL, CACHE_TTL, cache_blob)
    except RedisError:
        logger.warning("Redis unavailable when caching %s", CACHE_KEY_INTERVAL)

    return value


async def get_verification_deadline_minutes(db: AsyncSession, redis: Redis) -> int:
    """Return verification deadline minutes from AppConfig.

    The verification deadline is calculated as pickup_time minus N minutes.
    Resolution order: Redis cache -> DB -> DEFAULT_VERIFICATION_DEADLINE_MINUTES (30).
    """
    # 1. Try Redis cache
    try:
        cached = await redis.get(CACHE_KEY_VERIFICATION_DEADLINE)
        if cached is not None:
            value = int(cached)
            _memory_cache[CACHE_KEY_VERIFICATION_DEADLINE] = value
            return value
    except (RedisError, ValueError):
        logger.warning(
            "Redis read failed for %s, falling back to DB",
            CACHE_KEY_VERIFICATION_DEADLINE,
        )
        mem_value = _memory_cache.get(CACHE_KEY_VERIFICATION_DEADLINE)
        if mem_value is not None:
            return int(mem_value)

    # 2. Try DB
    result = await db.execute(
        select(AppConfig.value).where(AppConfig.key == "verification_deadline_minutes")
    )
    config_value = result.scalar_one_or_none()

    if config_value is not None:
        try:
            minutes = int(config_value)
        except (ValueError, TypeError):
            logger.warning(
                "Invalid verification_deadline_minutes value: %r, using default %d",
                config_value,
                DEFAULT_VERIFICATION_DEADLINE_MINUTES,
            )
            return DEFAULT_VERIFICATION_DEADLINE_MINUTES

        _memory_cache[CACHE_KEY_VERIFICATION_DEADLINE] = minutes
        try:
            await redis.setex(CACHE_KEY_VERIFICATION_DEADLINE, CACHE_TTL, str(minutes))
        except RedisError:
            logger.warning(
                "Redis unavailable when caching %s",
                CACHE_KEY_VERIFICATION_DEADLINE,
            )
        return minutes

    return DEFAULT_VERIFICATION_DEADLINE_MINUTES


async def get_verification_check_interval_minutes(db: AsyncSession, redis: Redis) -> int:
    """Return verification check interval minutes from AppConfig.

    Controls how often each ride is included in verify_rides for the Search App.
    Special value 0 means verification is requested only right before the deadline.

    Resolution order: Redis cache -> DB -> DEFAULT (60 minutes).
    """
    # 1. Try Redis cache
    try:
        cached = await redis.get(CACHE_KEY_VERIFICATION_CHECK_INTERVAL)
        if cached is not None:
            value = int(cached)
            _memory_cache[CACHE_KEY_VERIFICATION_CHECK_INTERVAL] = value
            return value
    except (RedisError, ValueError):
        logger.warning(
            "Redis read failed for %s, falling back to DB",
            CACHE_KEY_VERIFICATION_CHECK_INTERVAL,
        )
        mem_value = _memory_cache.get(CACHE_KEY_VERIFICATION_CHECK_INTERVAL)
        if mem_value is not None:
            return int(mem_value)

    # 2. Try DB
    result = await db.execute(
        select(AppConfig.value).where(AppConfig.key == "verification_check_interval_minutes")
    )
    config_value = result.scalar_one_or_none()

    if config_value is not None:
        try:
            minutes = int(config_value)
        except (ValueError, TypeError):
            logger.warning(
                "Invalid verification_check_interval_minutes value: %r, using default %d",
                config_value,
                DEFAULT_VERIFICATION_CHECK_INTERVAL_MINUTES,
            )
            return DEFAULT_VERIFICATION_CHECK_INTERVAL_MINUTES

        _memory_cache[CACHE_KEY_VERIFICATION_CHECK_INTERVAL] = minutes
        try:
            await redis.setex(CACHE_KEY_VERIFICATION_CHECK_INTERVAL, CACHE_TTL, str(minutes))
        except RedisError:
            logger.warning(
                "Redis unavailable when caching %s",
                CACHE_KEY_VERIFICATION_CHECK_INTERVAL,
            )
        return minutes

    return DEFAULT_VERIFICATION_CHECK_INTERVAL_MINUTES


# ---------------------------------------------------------------------------
# Billing config getters (typed, with Pydantic validation)
# ---------------------------------------------------------------------------


async def get_registration_bonus_credits(db: AsyncSession, redis: Redis) -> int:
    """Return registration bonus credits from AppConfig.

    Resolution order: Redis cache -> DB -> DEFAULT_REGISTRATION_BONUS_CREDITS (10).
    """
    # 1. Try Redis cache
    try:
        cached = await redis.get(CACHE_KEY_REGISTRATION_BONUS)
        if cached is not None:
            value = int(cached)
            _memory_cache[CACHE_KEY_REGISTRATION_BONUS] = value
            return value
    except (RedisError, ValueError):
        logger.warning(
            "Redis read failed for %s, falling back to DB",
            CACHE_KEY_REGISTRATION_BONUS,
        )
        mem_value = _memory_cache.get(CACHE_KEY_REGISTRATION_BONUS)
        if mem_value is not None:
            return int(mem_value)

    # 2. Try DB
    result = await db.execute(
        select(AppConfig.value).where(AppConfig.key == "registration_bonus_credits")
    )
    config_value = result.scalar_one_or_none()

    if config_value is not None:
        try:
            bonus = int(config_value)
        except (ValueError, TypeError):
            logger.warning(
                "Invalid registration_bonus_credits value: %r, using default %d",
                config_value,
                DEFAULT_REGISTRATION_BONUS_CREDITS,
            )
            return DEFAULT_REGISTRATION_BONUS_CREDITS

        _memory_cache[CACHE_KEY_REGISTRATION_BONUS] = bonus
        try:
            await redis.setex(CACHE_KEY_REGISTRATION_BONUS, CACHE_TTL, str(bonus))
        except RedisError:
            logger.warning(
                "Redis unavailable when caching %s",
                CACHE_KEY_REGISTRATION_BONUS,
            )
        return bonus

    return DEFAULT_REGISTRATION_BONUS_CREDITS


async def get_credit_products(db: AsyncSession, redis: Redis) -> CreditProductsConfig:
    """Return credit products catalog from AppConfig.

    Resolution order: Redis cache -> DB -> DEFAULT_CREDIT_PRODUCTS.
    Returns a validated ``CreditProductsConfig`` (Pydantic RootModel).
    """
    raw_json: str | None = None

    # 1. Try Redis cache
    try:
        cached = await redis.get(CACHE_KEY_CREDIT_PRODUCTS)
        if cached is not None:
            raw_json = cached
    except RedisError:
        logger.warning(
            "Redis unavailable when reading %s, falling back to DB",
            CACHE_KEY_CREDIT_PRODUCTS,
        )
        mem_value = _memory_cache.get(CACHE_KEY_CREDIT_PRODUCTS)
        if mem_value is not None:
            return mem_value  # type: ignore[no-any-return]

    # 2. Try DB (only when Redis miss)
    if raw_json is None:
        result = await db.execute(
            select(AppConfig.value).where(AppConfig.key == "credit_products")
        )
        config_value = result.scalar_one_or_none()

        if config_value is not None:
            raw_json = config_value
            # Cache DB value for next time
            try:
                await redis.setex(CACHE_KEY_CREDIT_PRODUCTS, CACHE_TTL, config_value)
            except RedisError:
                logger.warning(
                    "Redis unavailable when caching %s",
                    CACHE_KEY_CREDIT_PRODUCTS,
                )

    # 3. Parse and validate
    if raw_json is not None:
        try:
            data = json.loads(raw_json)
            value = CreditProductsConfig.model_validate(data)
            _memory_cache[CACHE_KEY_CREDIT_PRODUCTS] = value
            return value
        except (json.JSONDecodeError, ValueError, TypeError) as exc:
            logger.warning("Invalid credit_products config, using defaults: %s", exc)

    # 4. Fallback to defaults
    return CreditProductsConfig.model_validate(DEFAULT_CREDIT_PRODUCTS)


async def get_ride_credit_tiers(db: AsyncSession, redis: Redis) -> RideCreditTiersConfig:
    """Return ride credit tiers from AppConfig.

    Resolution order: Redis cache -> DB -> DEFAULT_RIDE_CREDIT_TIERS.
    Returns a validated ``RideCreditTiersConfig`` (Pydantic RootModel).
    """
    raw_json: str | None = None

    # 1. Try Redis cache
    try:
        cached = await redis.get(CACHE_KEY_RIDE_CREDIT_TIERS)
        if cached is not None:
            raw_json = cached
    except RedisError:
        logger.warning(
            "Redis unavailable when reading %s, falling back to DB",
            CACHE_KEY_RIDE_CREDIT_TIERS,
        )
        mem_value = _memory_cache.get(CACHE_KEY_RIDE_CREDIT_TIERS)
        if mem_value is not None:
            return mem_value  # type: ignore[no-any-return]

    # 2. Try DB (only when Redis miss)
    if raw_json is None:
        result = await db.execute(
            select(AppConfig.value).where(AppConfig.key == "ride_credit_tiers")
        )
        config_value = result.scalar_one_or_none()

        if config_value is not None:
            raw_json = config_value
            # Cache DB value for next time
            try:
                await redis.setex(CACHE_KEY_RIDE_CREDIT_TIERS, CACHE_TTL, config_value)
            except RedisError:
                logger.warning(
                    "Redis unavailable when caching %s",
                    CACHE_KEY_RIDE_CREDIT_TIERS,
                )

    # 3. Parse and validate
    if raw_json is not None:
        try:
            data = json.loads(raw_json)
            value = RideCreditTiersConfig.model_validate(data)
            _memory_cache[CACHE_KEY_RIDE_CREDIT_TIERS] = value
            return value
        except (json.JSONDecodeError, ValueError, TypeError) as exc:
            logger.warning("Invalid ride_credit_tiers config, using defaults: %s", exc)

    # 4. Fallback to defaults
    return RideCreditTiersConfig.model_validate(DEFAULT_RIDE_CREDIT_TIERS)


# ---------------------------------------------------------------------------
# Push notification templates
# ---------------------------------------------------------------------------

DEFAULT_PUSH_TEMPLATES: dict[str, dict[str, dict[str, str]]] = {
    "RIDE_ACCEPTED": {
        "en": {
            "title": "New Ride",
            "body": "Ride from {pickup_location} to {dropoff_location}, ${price}",
        },
        "es": {
            "title": "Nuevo viaje",
            "body": "Viaje de {pickup_location} a {dropoff_location}, ${price}",
        },
    },
    "SEARCH_OFFLINE": {
        "en": {
            "title": "Device Offline",
            "body": "Your search device has been offline since {last_ping_at}",
        },
        "es": {
            "title": "Dispositivo fuera de línea",
            "body": "Su dispositivo de búsqueda está fuera de línea desde {last_ping_at}",
        },
    },
    "CREDITS_DEPLETED": {
        "en": {
            "title": "Credits Depleted",
            "body": "Your credit balance is empty. Top up to continue.",
        },
        "es": {
            "title": "Créditos agotados",
            "body": "Su saldo de créditos está vacío. Recargue para continuar.",
        },
    },
    "CREDITS_LOW": {
        "en": {
            "title": "Low Credits",
            "body": "Your balance is {balance} credits. Minimum for a ride is {threshold}.",
        },
        "es": {
            "title": "Créditos bajos",
            "body": "Su saldo es de {balance} créditos. Mínimo para un viaje es {threshold}.",
        },
    },
    "RIDE_CREDIT_REFUNDED": {
        "en": {
            "title": "Credit Refunded",
            "body": "{credits_refunded} credit(s) refunded. New balance: {new_balance}",
        },
        "es": {
            "title": "Crédito reembolsado",
            "body": "{credits_refunded} crédito(s) reembolsado(s). Nuevo saldo: {new_balance}",
        },
    },
    "BALANCE_ADJUSTED": {
        "en": {
            "title": "Balance Updated",
            "body": "Your balance was adjusted by {amount}. New balance: {new_balance}",
        },
        "es": {
            "title": "Saldo actualizado",
            "body": "Su saldo fue ajustado en {amount}. Nuevo saldo: {new_balance}",
        },
    },
    "SEARCH_UPDATE_REQUIRED": {
        "en": {
            "title": "Update Required",
            "body": "Your search device needs an update to version {min_version}.",
        },
        "es": {
            "title": "Actualización requerida",
            "body": "Su dispositivo de búsqueda necesita una actualización a la versión {min_version}.",
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


# ---------------------------------------------------------------------------
# Batch loading for ping handler
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PingConfigs:
    """All AppConfig values needed by the ping handler."""

    min_search_version: str
    verification_check_interval_minutes: int
    search_interval_config: tuple[int, list[float]] | None


# DB keys that map to each Redis cache entry.
# search_interval is a composite of two DB keys cached as one JSON blob.
_DB_KEY_MIN_VERSION = "min_search_app_version"
_DB_KEY_CHECK_INTERVAL = "verification_check_interval_minutes"
_DB_KEY_RPD = "requests_per_day"
_DB_KEY_RPH = "requests_per_hour"


async def batch_get_ping_configs(db: AsyncSession, redis: Redis) -> PingConfigs:
    """Load all AppConfig values needed by ping in a single Redis MGET.

    Falls back to a single DB query (IN clause) for any keys missing from
    the Redis cache, then writes them back for future requests.

    Reduces Redis round-trips from 3 separate GETs to 1 MGET.
    """
    redis_keys = [CACHE_KEY, CACHE_KEY_VERIFICATION_CHECK_INTERVAL, CACHE_KEY_INTERVAL]

    # 1. Try Redis MGET
    cached: list[str | None] = [None, None, None]
    redis_failed = False
    try:
        cached = await redis.mget(redis_keys)
    except RedisError:
        redis_failed = True
        logger.warning("Redis MGET failed for ping configs, falling back to DB")

    # 2. Parse cached values
    min_version: str | None = None
    check_interval: int | None = None
    interval_config: tuple[int, list[float]] | None = None
    # Track whether interval was resolved (None is a valid "not configured" state)
    interval_resolved = False

    if cached[0] is not None:
        min_version = str(cached[0])
        _memory_cache[CACHE_KEY] = min_version

    if cached[1] is not None:
        try:
            check_interval = int(cached[1])
            _memory_cache[CACHE_KEY_VERIFICATION_CHECK_INTERVAL] = check_interval
        except (ValueError, TypeError):
            logger.warning("Invalid cached verification_check_interval: %r", cached[1])

    if cached[2] is not None:
        try:
            data = json.loads(cached[2])
            interval_config = (data["rpd"], data["rph"])
            interval_resolved = True
            _memory_cache[CACHE_KEY_INTERVAL] = interval_config
        except (json.JSONDecodeError, KeyError, TypeError):
            logger.warning("Invalid cached search_interval: %r", cached[2])

    # 2b. In-memory fallback for values still missing after Redis failure
    if redis_failed:
        if min_version is None:
            mem = _memory_cache.get(CACHE_KEY)
            if mem is not None:
                min_version = mem
        if check_interval is None:
            mem = _memory_cache.get(CACHE_KEY_VERIFICATION_CHECK_INTERVAL)
            if mem is not None:
                check_interval = mem
        if not interval_resolved:
            mem = _memory_cache.get(CACHE_KEY_INTERVAL)
            if mem is not None:
                interval_config = mem
                interval_resolved = True

    # 3. Determine which DB keys are needed
    need_db_keys: list[str] = []
    if min_version is None:
        need_db_keys.append(_DB_KEY_MIN_VERSION)
    if check_interval is None:
        need_db_keys.append(_DB_KEY_CHECK_INTERVAL)
    if not interval_resolved:
        need_db_keys.extend([_DB_KEY_RPD, _DB_KEY_RPH])

    # 4. Single DB query for all missing keys
    if need_db_keys:
        result = await db.execute(
            select(AppConfig.key, AppConfig.value).where(AppConfig.key.in_(need_db_keys))
        )
        db_rows = {row.key: row.value for row in result.all()}

        # Parse and cache min_version
        if min_version is None:
            db_val = db_rows.get(_DB_KEY_MIN_VERSION)
            if db_val is not None:
                min_version = db_val
                _memory_cache[CACHE_KEY] = min_version
                with contextlib.suppress(RedisError):
                    await redis.setex(CACHE_KEY, CACHE_TTL, db_val)

        # Parse and cache check_interval
        if check_interval is None:
            db_val = db_rows.get(_DB_KEY_CHECK_INTERVAL)
            if db_val is not None:
                try:
                    check_interval = int(db_val)
                    _memory_cache[CACHE_KEY_VERIFICATION_CHECK_INTERVAL] = check_interval
                    with contextlib.suppress(RedisError):
                        await redis.setex(
                            CACHE_KEY_VERIFICATION_CHECK_INTERVAL,
                            CACHE_TTL,
                            db_val,
                        )
                except (ValueError, TypeError):
                    logger.warning("Invalid DB verification_check_interval_minutes: %r", db_val)

        # Parse and cache interval_config
        if not interval_resolved:
            rpd_val = db_rows.get(_DB_KEY_RPD)
            rph_val = db_rows.get(_DB_KEY_RPH)
            if rpd_val is not None and rph_val is not None:
                try:
                    rpd = int(rpd_val)
                    rph = json.loads(rph_val)
                    interval_config = (rpd, rph)
                    interval_resolved = True
                    _memory_cache[CACHE_KEY_INTERVAL] = interval_config
                    cache_blob = json.dumps({"rpd": rpd, "rph": rph})
                    with contextlib.suppress(RedisError):
                        await redis.setex(CACHE_KEY_INTERVAL, CACHE_TTL, cache_blob)
                except (ValueError, TypeError, json.JSONDecodeError):
                    logger.warning("Invalid DB interval config")

    # 5. Apply defaults for anything still missing
    if min_version is None:
        min_version = settings.MIN_SEARCH_APP_VERSION
    if check_interval is None:
        check_interval = DEFAULT_VERIFICATION_CHECK_INTERVAL_MINUTES
    # interval_config stays None when not configured (caller uses flat default)

    return PingConfigs(
        min_search_version=min_version,
        verification_check_interval_minutes=check_interval,
        search_interval_config=interval_config,
    )
