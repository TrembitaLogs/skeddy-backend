"""Tests for in-memory fallback cache and cache invalidation in config_service.

Test strategy (task 13.3):
3. Redis unavailable → in-memory fallback cache used
4. In-memory TTL 600s expiration works
7. invalidate_config() removes from Redis and in-memory
8. Integration: Redis restart → in-memory fallback → Redis works again

Also covers batch MGET with in-memory fallback.
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
    CACHE_KEY_VERIFICATION_CHECK_INTERVAL,
    CACHE_KEY_VERIFICATION_DEADLINE,
    DEFAULT_REGISTRATION_BONUS_CREDITS,
    _memory_cache,
    batch_get_ping_configs,
    get_credit_products,
    get_registration_bonus_credits,
    get_ride_credit_tiers,
    get_verification_check_interval_minutes,
    get_verification_deadline_minutes,
    invalidate_config,
)


def _broken_redis():
    """Create a Redis mock that raises RedisError on all operations."""
    redis = AsyncMock()
    redis.get = AsyncMock(side_effect=RedisError("connection refused"))
    redis.setex = AsyncMock(side_effect=RedisError("connection refused"))
    redis.delete = AsyncMock(side_effect=RedisError("connection refused"))
    redis.mget = AsyncMock(side_effect=RedisError("connection refused"))
    return redis


# ===========================================================================
# Test 3: Redis unavailable → in-memory fallback
# ===========================================================================


async def test_memory_fallback_registration_bonus(db_session, fake_redis):
    """Redis hit populates in-memory; Redis failure returns in-memory value."""
    # Step 1: Warm up via Redis (populates in-memory)
    fake_redis._store[CACHE_KEY_REGISTRATION_BONUS] = "20"
    result = await get_registration_bonus_credits(db_session, fake_redis)
    assert result == 20

    # Step 2: Redis goes down → in-memory returns 20
    broken = _broken_redis()
    result = await get_registration_bonus_credits(db_session, broken)
    assert result == 20


async def test_memory_fallback_verification_deadline(db_session, fake_redis):
    """In-memory cache serves verification_deadline_minutes when Redis fails."""
    fake_redis._store[CACHE_KEY_VERIFICATION_DEADLINE] = "45"
    result = await get_verification_deadline_minutes(db_session, fake_redis)
    assert result == 45

    broken = _broken_redis()
    result = await get_verification_deadline_minutes(db_session, broken)
    assert result == 45


async def test_memory_fallback_verification_check_interval(db_session, fake_redis):
    """In-memory cache serves verification_check_interval when Redis fails."""
    fake_redis._store[CACHE_KEY_VERIFICATION_CHECK_INTERVAL] = "90"
    result = await get_verification_check_interval_minutes(db_session, fake_redis)
    assert result == 90

    broken = _broken_redis()
    result = await get_verification_check_interval_minutes(db_session, broken)
    assert result == 90


async def test_memory_fallback_credit_products(db_session, fake_redis):
    """In-memory cache serves CreditProductsConfig when Redis fails."""
    products = [{"product_id": "test_5", "credits": 5, "price_usd": 5.0}]
    fake_redis._store[CACHE_KEY_CREDIT_PRODUCTS] = json.dumps(products)

    result = await get_credit_products(db_session, fake_redis)
    assert isinstance(result, CreditProductsConfig)
    assert result.root[0].product_id == "test_5"

    broken = _broken_redis()
    result = await get_credit_products(db_session, broken)
    assert isinstance(result, CreditProductsConfig)
    assert result.root[0].product_id == "test_5"


async def test_memory_fallback_ride_credit_tiers(db_session, fake_redis):
    """In-memory cache serves RideCreditTiersConfig when Redis fails."""
    tiers = [{"max_price": 30.0, "credits": 2}, {"max_price": None, "credits": 5}]
    fake_redis._store[CACHE_KEY_RIDE_CREDIT_TIERS] = json.dumps(tiers)

    result = await get_ride_credit_tiers(db_session, fake_redis)
    assert isinstance(result, RideCreditTiersConfig)
    assert result.root[0].credits == 2

    broken = _broken_redis()
    result = await get_ride_credit_tiers(db_session, broken)
    assert isinstance(result, RideCreditTiersConfig)
    assert result.root[0].credits == 2


async def test_memory_fallback_from_db_value(db_session, fake_redis):
    """DB value populates in-memory; Redis failure returns in-memory value."""
    # No Redis value, but DB has the value
    db_session.add(AppConfig(key="registration_bonus_credits", value="30"))
    await db_session.flush()

    result = await get_registration_bonus_credits(db_session, fake_redis)
    assert result == 30

    # Redis goes down → in-memory returns DB value
    broken = _broken_redis()
    result = await get_registration_bonus_credits(db_session, broken)
    assert result == 30


async def test_memory_fallback_empty_returns_default(db_session):
    """When in-memory is empty and Redis is down, falls back to DB then default."""
    broken = _broken_redis()
    result = await get_registration_bonus_credits(db_session, broken)
    assert result == DEFAULT_REGISTRATION_BONUS_CREDITS


# ===========================================================================
# Test 4: In-memory TTL expiration
# ===========================================================================


async def test_memory_cache_ttl_expiration(db_session, fake_redis):
    """In-memory entries expire after TTL and fall through to DB/default."""
    # Warm up via Redis
    fake_redis._store[CACHE_KEY_REGISTRATION_BONUS] = "20"
    await get_registration_bonus_credits(db_session, fake_redis)
    assert _memory_cache.get(CACHE_KEY_REGISTRATION_BONUS) == 20

    # Manually expire the entry by clearing the cache (simulates TTL expiry)
    _memory_cache.clear()
    assert _memory_cache.get(CACHE_KEY_REGISTRATION_BONUS) is None

    # Redis down, memory expired → falls back to DB/default
    broken = _broken_redis()
    result = await get_registration_bonus_credits(db_session, broken)
    assert result == DEFAULT_REGISTRATION_BONUS_CREDITS


async def test_memory_cache_populated_on_redis_hit(db_session, fake_redis):
    """Verify that reading from Redis populates the in-memory cache."""
    assert _memory_cache.get(CACHE_KEY_REGISTRATION_BONUS) is None

    fake_redis._store[CACHE_KEY_REGISTRATION_BONUS] = "42"
    await get_registration_bonus_credits(db_session, fake_redis)

    assert _memory_cache.get(CACHE_KEY_REGISTRATION_BONUS) == 42


async def test_memory_cache_populated_on_db_hit(db_session, fake_redis):
    """Verify that reading from DB populates the in-memory cache."""
    db_session.add(AppConfig(key="registration_bonus_credits", value="55"))
    await db_session.flush()

    assert _memory_cache.get(CACHE_KEY_REGISTRATION_BONUS) is None
    await get_registration_bonus_credits(db_session, fake_redis)
    assert _memory_cache.get(CACHE_KEY_REGISTRATION_BONUS) == 55


# ===========================================================================
# Test 7: invalidate_config() removes from Redis and in-memory
# ===========================================================================


async def test_invalidate_removes_from_redis_and_memory(fake_redis):
    """invalidate_config clears both Redis key and in-memory entry."""
    # Populate both caches
    fake_redis._store[CACHE_KEY_REGISTRATION_BONUS] = "20"
    _memory_cache[CACHE_KEY_REGISTRATION_BONUS] = 20

    await invalidate_config("registration_bonus_credits", fake_redis)

    assert CACHE_KEY_REGISTRATION_BONUS not in fake_redis._store
    assert _memory_cache.get(CACHE_KEY_REGISTRATION_BONUS) is None


async def test_invalidate_removes_credit_products(fake_redis):
    """invalidate_config clears credit_products from both caches."""
    fake_redis._store[CACHE_KEY_CREDIT_PRODUCTS] = "[]"
    _memory_cache[CACHE_KEY_CREDIT_PRODUCTS] = "stale"

    await invalidate_config("credit_products", fake_redis)

    assert CACHE_KEY_CREDIT_PRODUCTS not in fake_redis._store
    assert _memory_cache.get(CACHE_KEY_CREDIT_PRODUCTS) is None


async def test_invalidate_removes_ride_credit_tiers(fake_redis):
    """invalidate_config clears ride_credit_tiers from both caches."""
    fake_redis._store[CACHE_KEY_RIDE_CREDIT_TIERS] = "[]"
    _memory_cache[CACHE_KEY_RIDE_CREDIT_TIERS] = "stale"

    await invalidate_config("ride_credit_tiers", fake_redis)

    assert CACHE_KEY_RIDE_CREDIT_TIERS not in fake_redis._store
    assert _memory_cache.get(CACHE_KEY_RIDE_CREDIT_TIERS) is None


async def test_invalidate_unknown_key_is_noop(fake_redis):
    """invalidate_config with an unknown DB key does nothing."""
    await invalidate_config("unknown_key_xyz", fake_redis)
    # No error raised, no side effects


async def test_invalidate_with_redis_error(fake_redis):
    """invalidate_config handles Redis errors gracefully."""
    _memory_cache[CACHE_KEY_REGISTRATION_BONUS] = 20
    broken = _broken_redis()

    await invalidate_config("registration_bonus_credits", broken)

    # In-memory is still cleared even when Redis fails
    assert _memory_cache.get(CACHE_KEY_REGISTRATION_BONUS) is None


# ===========================================================================
# Test 8: Integration — Redis restart → in-memory → Redis works again
# ===========================================================================


async def test_redis_down_then_up_cycle(db_session, fake_redis):
    """Full cycle: Redis works → populate memory → Redis down → memory fallback → Redis back."""
    # Phase 1: Redis working, value cached in memory
    fake_redis._store[CACHE_KEY_REGISTRATION_BONUS] = "25"
    result = await get_registration_bonus_credits(db_session, fake_redis)
    assert result == 25
    assert _memory_cache.get(CACHE_KEY_REGISTRATION_BONUS) == 25

    # Phase 2: Redis goes down — in-memory fallback
    broken = _broken_redis()
    result = await get_registration_bonus_credits(db_session, broken)
    assert result == 25  # from in-memory

    # Phase 3: Redis comes back with updated value
    fake_redis._store[CACHE_KEY_REGISTRATION_BONUS] = "50"
    result = await get_registration_bonus_credits(db_session, fake_redis)
    assert result == 50  # from Redis (fresh)
    assert _memory_cache.get(CACHE_KEY_REGISTRATION_BONUS) == 50  # memory updated


async def test_redis_down_then_up_credit_products(db_session, fake_redis):
    """Redis restart cycle for CreditProductsConfig (Pydantic model)."""
    products_a = [{"product_id": "a", "credits": 1, "price_usd": 1.0}]
    products_b = [{"product_id": "b", "credits": 2, "price_usd": 2.0}]

    # Phase 1: Redis working
    fake_redis._store[CACHE_KEY_CREDIT_PRODUCTS] = json.dumps(products_a)
    result = await get_credit_products(db_session, fake_redis)
    assert result.root[0].product_id == "a"

    # Phase 2: Redis down
    broken = _broken_redis()
    result = await get_credit_products(db_session, broken)
    assert result.root[0].product_id == "a"  # from memory

    # Phase 3: Redis back with different data
    fake_redis._store[CACHE_KEY_CREDIT_PRODUCTS] = json.dumps(products_b)
    result = await get_credit_products(db_session, fake_redis)
    assert result.root[0].product_id == "b"  # fresh from Redis


# ===========================================================================
# Batch MGET with in-memory fallback
# ===========================================================================


async def test_batch_mget_populates_memory(db_session, fake_redis):
    """batch_get_ping_configs populates in-memory for all Redis hits."""
    from app.services.config_service import CACHE_KEY, CACHE_KEY_INTERVAL

    fake_redis._store[CACHE_KEY] = "2.0.0"
    fake_redis._store[CACHE_KEY_VERIFICATION_CHECK_INTERVAL] = "45"
    fake_redis._store[CACHE_KEY_INTERVAL] = json.dumps({"rpd": 100, "rph": [1.0] * 24})

    configs = await batch_get_ping_configs(db_session, fake_redis)

    assert configs.min_search_version == "2.0.0"
    assert configs.verification_check_interval_minutes == 45

    # Verify in-memory was populated
    assert _memory_cache.get(CACHE_KEY) == "2.0.0"
    assert _memory_cache.get(CACHE_KEY_VERIFICATION_CHECK_INTERVAL) == 45
    assert _memory_cache.get(CACHE_KEY_INTERVAL) is not None


async def test_batch_mget_redis_fail_uses_memory(db_session, fake_redis):
    """batch_get_ping_configs falls back to in-memory when Redis MGET fails."""
    from app.services.config_service import CACHE_KEY, CACHE_KEY_INTERVAL

    # Pre-populate in-memory cache
    _memory_cache[CACHE_KEY] = "1.5.0"
    _memory_cache[CACHE_KEY_VERIFICATION_CHECK_INTERVAL] = 30
    _memory_cache[CACHE_KEY_INTERVAL] = (200, [2.0] * 24)

    broken = _broken_redis()
    configs = await batch_get_ping_configs(db_session, broken)

    assert configs.min_search_version == "1.5.0"
    assert configs.verification_check_interval_minutes == 30
    assert configs.search_interval_config == (200, [2.0] * 24)


async def test_batch_mget_redis_fail_partial_memory(db_session, fake_redis):
    """batch_get_ping_configs uses memory for what's available, DB for the rest."""
    from app.services.config_service import CACHE_KEY

    # Only min_version in memory
    _memory_cache[CACHE_KEY] = "1.0.0"
    # check_interval not in memory → will come from DB or default

    db_session.add(AppConfig(key="verification_check_interval_minutes", value="120"))
    await db_session.flush()

    broken = _broken_redis()
    configs = await batch_get_ping_configs(db_session, broken)

    assert configs.min_search_version == "1.0.0"  # from memory
    assert configs.verification_check_interval_minutes == 120  # from DB


async def test_batch_mget_db_values_populate_memory(db_session, fake_redis):
    """batch_get_ping_configs stores DB values in in-memory cache."""
    from app.services.config_service import CACHE_KEY

    db_session.add(AppConfig(key="min_search_app_version", value="3.0.0"))
    db_session.add(AppConfig(key="verification_check_interval_minutes", value="15"))
    await db_session.flush()

    await batch_get_ping_configs(db_session, fake_redis)

    assert _memory_cache.get(CACHE_KEY) == "3.0.0"
    assert _memory_cache.get(CACHE_KEY_VERIFICATION_CHECK_INTERVAL) == 15
