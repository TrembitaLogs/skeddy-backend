"""Tests for billing-related AppConfig getters in config_service.

Covers: get_credit_products, get_ride_credit_tiers, get_registration_bonus_credits.

Test strategy (task 13.2):
1. get_credit_products() returns valid CreditProductsConfig
2. get_ride_credit_tiers() returns valid RideCreditTiersConfig
3. get_registration_bonus_credits() returns int
4. Fallback to DEFAULT_VALUES when key missing in Redis and DB
5. DB contains value → returned from DB and cached in Redis
6. Invalid JSON in DB → fallback to default with warning log
"""

import json
from unittest.mock import AsyncMock

from redis.exceptions import RedisError

from app.models.app_config import AppConfig
from app.schemas.billing_config import CreditProductsConfig, RideCreditTiersConfig
from app.services.config_service import (
    CACHE_KEY_CREDIT_PRODUCTS,
    CACHE_KEY_REGISTRATION_BONUS,
    CACHE_KEY_RIDE_CREDIT_TIERS,
    CACHE_TTL,
    DEFAULT_CREDIT_PRODUCTS,
    DEFAULT_REGISTRATION_BONUS_CREDITS,
    DEFAULT_RIDE_CREDIT_TIERS,
    get_credit_products,
    get_registration_bonus_credits,
    get_ride_credit_tiers,
)

# ===========================================================================
# get_registration_bonus_credits tests
# ===========================================================================


async def test_registration_bonus_returns_default_when_empty(db_session, fake_redis):
    """Returns DEFAULT_REGISTRATION_BONUS_CREDITS when no Redis/DB value."""
    result = await get_registration_bonus_credits(db_session, fake_redis)
    assert result == DEFAULT_REGISTRATION_BONUS_CREDITS


async def test_registration_bonus_reads_from_redis_cache(db_session, fake_redis):
    """Returns cached value from Redis without DB query."""
    fake_redis._store[CACHE_KEY_REGISTRATION_BONUS] = "20"
    result = await get_registration_bonus_credits(db_session, fake_redis)
    assert result == 20


async def test_registration_bonus_falls_back_to_db_and_caches(db_session, fake_redis):
    """Reads from DB on Redis miss and writes back to Redis cache."""
    db_session.add(AppConfig(key="registration_bonus_credits", value="15"))
    await db_session.flush()

    result = await get_registration_bonus_credits(db_session, fake_redis)

    assert result == 15
    assert fake_redis._store.get(CACHE_KEY_REGISTRATION_BONUS) == "15"
    fake_redis.setex.assert_called_with(CACHE_KEY_REGISTRATION_BONUS, CACHE_TTL, "15")


async def test_registration_bonus_invalid_db_value_returns_default(db_session, fake_redis):
    """Returns default when DB value is not a valid integer."""
    db_session.add(AppConfig(key="registration_bonus_credits", value="not_a_number"))
    await db_session.flush()

    result = await get_registration_bonus_credits(db_session, fake_redis)
    assert result == DEFAULT_REGISTRATION_BONUS_CREDITS


async def test_registration_bonus_redis_error_falls_back_to_db(db_session):
    """Falls back to DB when Redis raises RedisError."""
    db_session.add(AppConfig(key="registration_bonus_credits", value="25"))
    await db_session.flush()

    broken_redis = AsyncMock()
    broken_redis.get = AsyncMock(side_effect=RedisError("connection refused"))
    broken_redis.setex = AsyncMock(side_effect=RedisError("connection refused"))

    result = await get_registration_bonus_credits(db_session, broken_redis)
    assert result == 25


async def test_registration_bonus_cache_refresh_after_expiry(db_session, fake_redis):
    """Reads new DB value after cache expires."""
    db_session.add(AppConfig(key="registration_bonus_credits", value="30"))
    await db_session.flush()

    # Stale cache
    fake_redis._store[CACHE_KEY_REGISTRATION_BONUS] = "10"
    result = await get_registration_bonus_credits(db_session, fake_redis)
    assert result == 10  # Returns stale cached value

    # Simulate cache expiry
    del fake_redis._store[CACHE_KEY_REGISTRATION_BONUS]
    result = await get_registration_bonus_credits(db_session, fake_redis)
    assert result == 30  # Reads fresh from DB
    assert fake_redis._store.get(CACHE_KEY_REGISTRATION_BONUS) == "30"


# ===========================================================================
# get_credit_products tests
# ===========================================================================

CUSTOM_PRODUCTS = [
    {"product_id": "test_5", "credits": 5, "price_usd": 5.00},
    {"product_id": "test_20", "credits": 20, "price_usd": 18.00},
]


async def test_credit_products_returns_default_when_empty(db_session, fake_redis):
    """Returns DEFAULT_CREDIT_PRODUCTS as CreditProductsConfig when no config."""
    result = await get_credit_products(db_session, fake_redis)

    assert isinstance(result, CreditProductsConfig)
    assert len(result.root) == len(DEFAULT_CREDIT_PRODUCTS)
    assert result.root[0].product_id == "credits_10"


async def test_credit_products_reads_from_redis_cache(db_session, fake_redis):
    """Returns cached value from Redis, parsed as CreditProductsConfig."""
    fake_redis._store[CACHE_KEY_CREDIT_PRODUCTS] = json.dumps(CUSTOM_PRODUCTS)

    result = await get_credit_products(db_session, fake_redis)

    assert isinstance(result, CreditProductsConfig)
    assert len(result.root) == 2
    assert result.root[0].product_id == "test_5"
    assert result.root[0].credits == 5


async def test_credit_products_falls_back_to_db_and_caches(db_session, fake_redis):
    """Reads from DB on Redis miss and writes back to Redis cache."""
    db_session.add(AppConfig(key="credit_products", value=json.dumps(CUSTOM_PRODUCTS)))
    await db_session.flush()

    result = await get_credit_products(db_session, fake_redis)

    assert isinstance(result, CreditProductsConfig)
    assert len(result.root) == 2
    assert result.root[1].product_id == "test_20"
    # Verify cached in Redis
    assert CACHE_KEY_CREDIT_PRODUCTS in fake_redis._store


async def test_credit_products_invalid_json_returns_default(db_session, fake_redis):
    """Returns defaults when DB has invalid JSON."""
    db_session.add(AppConfig(key="credit_products", value="not valid json"))
    await db_session.flush()

    result = await get_credit_products(db_session, fake_redis)

    assert isinstance(result, CreditProductsConfig)
    assert len(result.root) == len(DEFAULT_CREDIT_PRODUCTS)


async def test_credit_products_invalid_structure_returns_default(db_session, fake_redis):
    """Returns defaults when DB has valid JSON but invalid structure."""
    db_session.add(
        AppConfig(
            key="credit_products",
            value=json.dumps([{"bad_field": "value"}]),
        )
    )
    await db_session.flush()

    result = await get_credit_products(db_session, fake_redis)

    assert isinstance(result, CreditProductsConfig)
    assert len(result.root) == len(DEFAULT_CREDIT_PRODUCTS)


async def test_credit_products_empty_list_returns_default(db_session, fake_redis):
    """Returns defaults when DB has empty list (fails validation)."""
    db_session.add(AppConfig(key="credit_products", value="[]"))
    await db_session.flush()

    result = await get_credit_products(db_session, fake_redis)

    assert isinstance(result, CreditProductsConfig)
    assert len(result.root) == len(DEFAULT_CREDIT_PRODUCTS)


async def test_credit_products_redis_error_falls_back_to_db(db_session):
    """Falls back to DB when Redis raises RedisError."""
    db_session.add(AppConfig(key="credit_products", value=json.dumps(CUSTOM_PRODUCTS)))
    await db_session.flush()

    broken_redis = AsyncMock()
    broken_redis.get = AsyncMock(side_effect=RedisError("connection refused"))
    broken_redis.setex = AsyncMock(side_effect=RedisError("connection refused"))

    result = await get_credit_products(db_session, broken_redis)

    assert isinstance(result, CreditProductsConfig)
    assert len(result.root) == 2


async def test_credit_products_get_product_by_id(db_session, fake_redis):
    """CreditProductsConfig.get_product_by_id works correctly."""
    result = await get_credit_products(db_session, fake_redis)

    product = result.get_product_by_id("credits_50")
    assert product is not None
    assert product.credits == 50

    missing = result.get_product_by_id("nonexistent")
    assert missing is None


# ===========================================================================
# get_ride_credit_tiers tests
# ===========================================================================

CUSTOM_TIERS = [
    {"max_price": 30.0, "credits": 2},
    {"max_price": None, "credits": 5},
]


async def test_ride_credit_tiers_returns_default_when_empty(db_session, fake_redis):
    """Returns DEFAULT_RIDE_CREDIT_TIERS as RideCreditTiersConfig when no config."""
    result = await get_ride_credit_tiers(db_session, fake_redis)

    assert isinstance(result, RideCreditTiersConfig)
    assert len(result.root) == len(DEFAULT_RIDE_CREDIT_TIERS)
    assert result.root[0].max_price == 20.0
    assert result.root[0].credits == 1


async def test_ride_credit_tiers_reads_from_redis_cache(db_session, fake_redis):
    """Returns cached value from Redis, parsed as RideCreditTiersConfig."""
    fake_redis._store[CACHE_KEY_RIDE_CREDIT_TIERS] = json.dumps(CUSTOM_TIERS)

    result = await get_ride_credit_tiers(db_session, fake_redis)

    assert isinstance(result, RideCreditTiersConfig)
    assert len(result.root) == 2
    assert result.root[0].max_price == 30.0
    assert result.root[0].credits == 2


async def test_ride_credit_tiers_falls_back_to_db_and_caches(db_session, fake_redis):
    """Reads from DB on Redis miss and writes back to Redis cache."""
    db_session.add(AppConfig(key="ride_credit_tiers", value=json.dumps(CUSTOM_TIERS)))
    await db_session.flush()

    result = await get_ride_credit_tiers(db_session, fake_redis)

    assert isinstance(result, RideCreditTiersConfig)
    assert len(result.root) == 2
    assert CACHE_KEY_RIDE_CREDIT_TIERS in fake_redis._store


async def test_ride_credit_tiers_invalid_json_returns_default(db_session, fake_redis):
    """Returns defaults when DB has invalid JSON."""
    db_session.add(AppConfig(key="ride_credit_tiers", value="not valid json"))
    await db_session.flush()

    result = await get_ride_credit_tiers(db_session, fake_redis)

    assert isinstance(result, RideCreditTiersConfig)
    assert len(result.root) == len(DEFAULT_RIDE_CREDIT_TIERS)


async def test_ride_credit_tiers_empty_list_returns_default(db_session, fake_redis):
    """Returns defaults when DB has empty list (fails validation)."""
    db_session.add(AppConfig(key="ride_credit_tiers", value="[]"))
    await db_session.flush()

    result = await get_ride_credit_tiers(db_session, fake_redis)

    assert isinstance(result, RideCreditTiersConfig)
    assert len(result.root) == len(DEFAULT_RIDE_CREDIT_TIERS)


async def test_ride_credit_tiers_invalid_structure_returns_default(db_session, fake_redis):
    """Returns defaults when tiers have invalid order."""
    bad_tiers = [
        {"max_price": 50.0, "credits": 2},
        {"max_price": 20.0, "credits": 1},  # Not ascending
    ]
    db_session.add(AppConfig(key="ride_credit_tiers", value=json.dumps(bad_tiers)))
    await db_session.flush()

    result = await get_ride_credit_tiers(db_session, fake_redis)

    assert isinstance(result, RideCreditTiersConfig)
    assert len(result.root) == len(DEFAULT_RIDE_CREDIT_TIERS)


async def test_ride_credit_tiers_redis_error_falls_back_to_db(db_session):
    """Falls back to DB when Redis raises RedisError."""
    db_session.add(AppConfig(key="ride_credit_tiers", value=json.dumps(CUSTOM_TIERS)))
    await db_session.flush()

    broken_redis = AsyncMock()
    broken_redis.get = AsyncMock(side_effect=RedisError("connection refused"))
    broken_redis.setex = AsyncMock(side_effect=RedisError("connection refused"))

    result = await get_ride_credit_tiers(db_session, broken_redis)

    assert isinstance(result, RideCreditTiersConfig)
    assert len(result.root) == 2


async def test_ride_credit_tiers_get_credits_for_price(db_session, fake_redis):
    """RideCreditTiersConfig.get_credits_for_price works through getter."""
    result = await get_ride_credit_tiers(db_session, fake_redis)

    assert result.get_credits_for_price(15.0) == 1
    assert result.get_credits_for_price(20.0) == 1
    assert result.get_credits_for_price(20.01) == 2
    assert result.get_credits_for_price(50.0) == 2
    assert result.get_credits_for_price(100.0) == 3
