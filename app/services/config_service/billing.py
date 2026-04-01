"""Billing config: registration bonus, credit products, ride credit tiers."""

import json
import logging

from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.app_config import AppConfig
from app.schemas.billing_config import CreditProductsConfig, RideCreditTiersConfig
from app.services.config_service.cache import (
    CACHE_KEY_CREDIT_PRODUCTS,
    CACHE_KEY_REGISTRATION_BONUS,
    CACHE_KEY_RIDE_CREDIT_TIERS,
    CACHE_TTL,
    _memory_cache,
)

logger = logging.getLogger(__name__)

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
