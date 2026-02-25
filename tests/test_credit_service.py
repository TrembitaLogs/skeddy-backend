import json
import uuid
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.exc import OperationalError

from app.models.app_config import AppConfig
from app.models.credit_balance import CreditBalance
from app.models.credit_transaction import CreditTransaction, TransactionType
from app.models.user import User
from app.services.credit_service import (
    BALANCE_CACHE_TTL,
    _balance_key,
    add_credits,
    cache_balance,
    charge_credits,
    create_balance_with_bonus,
    get_balance,
    get_ride_credit_cost,
    low_balance_notified_key,
    refund_credits,
)


def _make_user(email: str = "credit@example.com") -> User:
    return User(email=email, password_hash="hashed")


# ---------------------------------------------------------------------------
# Test 1: Cache hit — returns cached value, no DB query
# ---------------------------------------------------------------------------


async def test_get_balance_cache_hit(db_session, fake_redis):
    """get_balance returns cached value from Redis without hitting DB."""
    user_id = uuid.uuid4()
    cache_key = _balance_key(user_id)
    fake_redis._store[cache_key] = "42"

    result = await get_balance(user_id, db_session, fake_redis)

    assert result == 42


# ---------------------------------------------------------------------------
# Test 2: Cache miss — reads from DB, caches result
# ---------------------------------------------------------------------------


async def test_get_balance_cache_miss_reads_db(db_session, fake_redis):
    """get_balance falls back to DB on cache miss and caches the result."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    cb = CreditBalance(user_id=user.id, balance=25)
    db_session.add(cb)
    await db_session.flush()

    result = await get_balance(user.id, db_session, fake_redis)

    assert result == 25
    # Verify the value was cached in Redis
    cache_key = _balance_key(user.id)
    assert fake_redis._store.get(cache_key) == "25"


# ---------------------------------------------------------------------------
# Test 3: Redis unavailable — graceful fallback to DB
# ---------------------------------------------------------------------------


async def test_get_balance_redis_unavailable(db_session):
    """get_balance falls back to DB when Redis raises RedisError."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    cb = CreditBalance(user_id=user.id, balance=17)
    db_session.add(cb)
    await db_session.flush()

    broken_redis = AsyncMock()
    broken_redis.get = AsyncMock(side_effect=RedisError("connection refused"))
    broken_redis.setex = AsyncMock(side_effect=RedisError("connection refused"))

    result = await get_balance(user.id, db_session, broken_redis)

    assert result == 17


# ---------------------------------------------------------------------------
# Test 4: Balance row does not exist — returns 0
# ---------------------------------------------------------------------------


async def test_get_balance_no_row_returns_zero(db_session, fake_redis):
    """get_balance returns 0 when no CreditBalance row exists for user."""
    user_id = uuid.uuid4()

    result = await get_balance(user_id, db_session, fake_redis)

    assert result == 0


# ---------------------------------------------------------------------------
# Test 5: Cache miss — verify setex called with correct TTL
# ---------------------------------------------------------------------------


async def test_get_balance_caches_with_correct_ttl(db_session, fake_redis):
    """get_balance caches the DB result with the expected TTL."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    cb = CreditBalance(user_id=user.id, balance=100)
    db_session.add(cb)
    await db_session.flush()

    cache_key = _balance_key(user.id)

    await get_balance(user.id, db_session, fake_redis)

    fake_redis.setex.assert_called_once_with(cache_key, BALANCE_CACHE_TTL, "100")


# ===========================================================================
# create_balance_with_bonus tests
# ===========================================================================


# ---------------------------------------------------------------------------
# Test 6: Creates balance with AppConfig bonus value
# ---------------------------------------------------------------------------


async def test_create_balance_with_bonus_from_config(db_session, fake_redis):
    """create_balance_with_bonus reads bonus amount from AppConfig."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    db_session.add(AppConfig(key="registration_bonus_credits", value="15"))
    await db_session.flush()

    result = await create_balance_with_bonus(user.id, db_session, fake_redis)

    assert result.balance == 15
    assert result.user_id == user.id


# ---------------------------------------------------------------------------
# Test 7: Fallback to 10 when AppConfig is missing
# ---------------------------------------------------------------------------


async def test_create_balance_with_bonus_fallback(db_session, fake_redis):
    """create_balance_with_bonus uses default 10 when AppConfig is absent."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    result = await create_balance_with_bonus(user.id, db_session, fake_redis)

    assert result.balance == 10


# ---------------------------------------------------------------------------
# Test 8: Creates CreditTransaction with REGISTRATION_BONUS type
# ---------------------------------------------------------------------------


async def test_create_balance_with_bonus_creates_transaction(db_session, fake_redis):
    """create_balance_with_bonus creates a REGISTRATION_BONUS transaction."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    await create_balance_with_bonus(user.id, db_session, fake_redis)

    result = await db_session.execute(
        select(CreditTransaction).where(CreditTransaction.user_id == user.id)
    )
    tx = result.scalar_one()
    assert tx.type == TransactionType.REGISTRATION_BONUS
    assert tx.amount == 10
    assert tx.balance_after == 10
    assert tx.reference_id is None


# ---------------------------------------------------------------------------
# Test 9: cache_balance updates Redis correctly
# ---------------------------------------------------------------------------


async def test_cache_balance_updates_redis(fake_redis):
    """cache_balance writes balance to Redis with correct TTL."""
    user_id = uuid.uuid4()
    await cache_balance(user_id, 42, fake_redis)

    cache_key = _balance_key(user_id)
    assert fake_redis._store.get(cache_key) == "42"
    fake_redis.setex.assert_called_once_with(cache_key, BALANCE_CACHE_TTL, "42")


# ---------------------------------------------------------------------------
# Test 10: create_balance_with_bonus works when Redis is unavailable
# ---------------------------------------------------------------------------


async def test_create_balance_with_bonus_redis_unavailable(db_session):
    """create_balance_with_bonus works gracefully when Redis is unavailable."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    broken_redis = AsyncMock()
    broken_redis.get = AsyncMock(side_effect=RedisError("connection refused"))
    broken_redis.setex = AsyncMock(side_effect=RedisError("connection refused"))

    result = await create_balance_with_bonus(user.id, db_session, broken_redis)

    assert result.balance == 10
    assert result.user_id == user.id


# ---------------------------------------------------------------------------
# Test 11: cache_balance deletes key on SET failure
# ---------------------------------------------------------------------------


async def test_cache_balance_redis_set_failure_deletes_key(fake_redis):
    """cache_balance deletes key on SET failure to prevent stale data."""
    user_id = uuid.uuid4()
    cache_key = _balance_key(user_id)

    # Pre-populate with stale value
    fake_redis._store[cache_key] = "old_value"

    # Make setex fail
    fake_redis.setex = AsyncMock(side_effect=RedisError("write failed"))

    await cache_balance(user_id, 42, fake_redis)

    # Key should be deleted to prevent stale cache
    assert cache_key not in fake_redis._store


# ---------------------------------------------------------------------------
# Test 12: cache_balance handles total Redis failure gracefully
# ---------------------------------------------------------------------------


async def test_cache_balance_total_redis_failure():
    """cache_balance handles both SET and DELETE failures gracefully."""
    user_id = uuid.uuid4()

    broken_redis = AsyncMock()
    broken_redis.setex = AsyncMock(side_effect=RedisError("write failed"))
    broken_redis.delete = AsyncMock(side_effect=RedisError("delete failed"))

    # Should not raise
    await cache_balance(user_id, 42, broken_redis)


# ===========================================================================
# charge_credits tests
# ===========================================================================


# ---------------------------------------------------------------------------
# Test 13: Full charge with sufficient balance
# ---------------------------------------------------------------------------


async def test_charge_credits_full_charge(db_session, fake_redis):
    """charge_credits deducts full amount when balance is sufficient."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    cb = CreditBalance(user_id=user.id, balance=10)
    db_session.add(cb)
    await db_session.flush()

    ride_id = uuid.uuid4()
    charged, new_balance = await charge_credits(user.id, 3, ride_id, db_session, fake_redis)

    assert charged == 3
    assert new_balance == 7

    # Verify DB balance is actually updated
    result = await db_session.execute(
        select(CreditBalance.balance).where(CreditBalance.user_id == user.id)
    )
    assert result.scalar_one() == 7


# ---------------------------------------------------------------------------
# Test 14: Partial charge when balance < amount
# ---------------------------------------------------------------------------


async def test_charge_credits_partial_charge(db_session, fake_redis):
    """charge_credits charges min(amount, balance) when balance < amount."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    cb = CreditBalance(user_id=user.id, balance=2)
    db_session.add(cb)
    await db_session.flush()

    charged, new_balance = await charge_credits(user.id, 5, uuid.uuid4(), db_session, fake_redis)

    assert charged == 2
    assert new_balance == 0


# ---------------------------------------------------------------------------
# Test 15: Zero balance — no transaction created
# ---------------------------------------------------------------------------


async def test_charge_credits_zero_balance_no_transaction(db_session, fake_redis):
    """charge_credits returns (0, 0) and creates no transaction when balance is 0."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    cb = CreditBalance(user_id=user.id, balance=0)
    db_session.add(cb)
    await db_session.flush()

    charged, new_balance = await charge_credits(user.id, 3, uuid.uuid4(), db_session, fake_redis)

    assert charged == 0
    assert new_balance == 0

    # No CreditTransaction should have been created
    result = await db_session.execute(
        select(CreditTransaction).where(CreditTransaction.user_id == user.id)
    )
    assert result.scalar_one_or_none() is None


# ---------------------------------------------------------------------------
# Test 16: CreditTransaction RIDE_CHARGE with correct fields
# ---------------------------------------------------------------------------


async def test_charge_credits_creates_ride_charge_transaction(db_session, fake_redis):
    """charge_credits creates RIDE_CHARGE transaction with negative amount."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    cb = CreditBalance(user_id=user.id, balance=10)
    db_session.add(cb)
    await db_session.flush()

    ride_id = uuid.uuid4()
    await charge_credits(user.id, 3, ride_id, db_session, fake_redis)

    result = await db_session.execute(
        select(CreditTransaction).where(CreditTransaction.user_id == user.id)
    )
    tx = result.scalar_one()
    assert tx.type == TransactionType.RIDE_CHARGE
    assert tx.amount == -3
    assert tx.balance_after == 7
    assert tx.reference_id == ride_id


# ---------------------------------------------------------------------------
# Test 17: Write-through Redis after charge
# ---------------------------------------------------------------------------


async def test_charge_credits_does_not_cache_balance(db_session, fake_redis):
    """charge_credits does NOT update Redis cache — caller is responsible."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    cb = CreditBalance(user_id=user.id, balance=10)
    db_session.add(cb)
    await db_session.flush()

    await charge_credits(user.id, 3, uuid.uuid4(), db_session, fake_redis)

    cache_key = _balance_key(user.id)
    assert fake_redis._store.get(cache_key) is None


# ---------------------------------------------------------------------------
# Test 18: Lock conflict raises HTTPException 503
# ---------------------------------------------------------------------------


async def test_charge_credits_lock_conflict_raises_503(fake_redis):
    """charge_credits raises HTTPException(503) on NOWAIT lock conflict."""
    mock_db = AsyncMock()
    lock_error = OperationalError(
        "SELECT ... FOR UPDATE NOWAIT",
        {},
        Exception('could not obtain lock on row in relation "credit_balances"'),
    )
    mock_db.execute = AsyncMock(side_effect=lock_error)

    with pytest.raises(HTTPException) as exc_info:
        await charge_credits(uuid.uuid4(), 3, uuid.uuid4(), mock_db, fake_redis)

    assert exc_info.value.status_code == 503


# ---------------------------------------------------------------------------
# Test 19: Concurrent charges — first succeeds, second gets 503
# ---------------------------------------------------------------------------


async def test_charge_credits_concurrent_second_gets_503(db_session, fake_redis):
    """Simulates concurrent charges: first succeeds, second gets lock conflict."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    cb = CreditBalance(user_id=user.id, balance=10)
    db_session.add(cb)
    await db_session.flush()

    # First charge succeeds normally
    charged, new_balance = await charge_credits(user.id, 3, uuid.uuid4(), db_session, fake_redis)
    assert charged == 3
    assert new_balance == 7

    # Second charge encounters lock conflict (simulated via mock session)
    mock_db = AsyncMock()
    lock_error = OperationalError(
        "SELECT ... FOR UPDATE NOWAIT",
        {},
        Exception('could not obtain lock on row in relation "credit_balances"'),
    )
    mock_db.execute = AsyncMock(side_effect=lock_error)

    with pytest.raises(HTTPException) as exc_info:
        await charge_credits(user.id, 3, uuid.uuid4(), mock_db, fake_redis)

    assert exc_info.value.status_code == 503

    # First charge's balance should remain unchanged
    result = await db_session.execute(
        select(CreditBalance.balance).where(CreditBalance.user_id == user.id)
    )
    assert result.scalar_one() == 7


# ===========================================================================
# add_credits tests
# ===========================================================================

TIERS_JSON = json.dumps(
    [
        {"max_price": 20.0, "credits": 1},
        {"max_price": 50.0, "credits": 2},
        {"max_price": None, "credits": 3},
    ]
)


# ---------------------------------------------------------------------------
# Test 20: Balance increases by amount
# ---------------------------------------------------------------------------


async def test_add_credits_balance_increases(db_session, fake_redis):
    """add_credits increases user balance by the given amount."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    cb = CreditBalance(user_id=user.id, balance=10)
    db_session.add(cb)
    await db_session.flush()

    new_balance = await add_credits(
        user.id, 50, TransactionType.PURCHASE, uuid.uuid4(), db_session, fake_redis
    )

    assert new_balance == 60

    # Verify DB balance is actually updated
    result = await db_session.execute(
        select(CreditBalance.balance).where(CreditBalance.user_id == user.id)
    )
    assert result.scalar_one() == 60


# ---------------------------------------------------------------------------
# Test 21: CreditTransaction created with correct tx_type
# ---------------------------------------------------------------------------


async def test_add_credits_creates_transaction_with_correct_type(db_session, fake_redis):
    """add_credits creates CreditTransaction with the given tx_type."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    cb = CreditBalance(user_id=user.id, balance=10)
    db_session.add(cb)
    await db_session.flush()

    purchase_order_id = uuid.uuid4()
    await add_credits(
        user.id, 50, TransactionType.PURCHASE, purchase_order_id, db_session, fake_redis
    )

    result = await db_session.execute(
        select(CreditTransaction).where(CreditTransaction.user_id == user.id)
    )
    tx = result.scalar_one()
    assert tx.type == TransactionType.PURCHASE
    assert tx.amount == 50
    assert tx.balance_after == 60
    assert tx.reference_id == purchase_order_id


# ---------------------------------------------------------------------------
# Test 22: reference_id can be None
# ---------------------------------------------------------------------------


async def test_add_credits_reference_id_none(db_session, fake_redis):
    """add_credits accepts None as reference_id (e.g. ADMIN_ADJUSTMENT)."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    cb = CreditBalance(user_id=user.id, balance=5)
    db_session.add(cb)
    await db_session.flush()

    new_balance = await add_credits(
        user.id, 10, TransactionType.ADMIN_ADJUSTMENT, None, db_session, fake_redis
    )

    assert new_balance == 15

    result = await db_session.execute(
        select(CreditTransaction).where(CreditTransaction.user_id == user.id)
    )
    tx = result.scalar_one()
    assert tx.reference_id is None
    assert tx.type == TransactionType.ADMIN_ADJUSTMENT


# ---------------------------------------------------------------------------
# Test 23: Redis cache updated after add_credits
# ---------------------------------------------------------------------------


async def test_add_credits_updates_redis_cache(db_session, fake_redis):
    """add_credits writes new balance to Redis cache via write-through."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    cb = CreditBalance(user_id=user.id, balance=10)
    db_session.add(cb)
    await db_session.flush()

    await add_credits(user.id, 25, TransactionType.PURCHASE, uuid.uuid4(), db_session, fake_redis)

    cache_key = _balance_key(user.id)
    assert fake_redis._store.get(cache_key) == "35"


# ---------------------------------------------------------------------------
# Test 24: low_balance_notified key deleted when new_balance >= threshold
# ---------------------------------------------------------------------------


async def test_add_credits_clears_low_balance_notified(db_session, fake_redis):
    """add_credits deletes low_balance_notified key when balance >= threshold."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    cb = CreditBalance(user_id=user.id, balance=1)
    db_session.add(cb)
    db_session.add(AppConfig(key="ride_credit_tiers", value=TIERS_JSON))
    await db_session.flush()

    # Pre-set low_balance_notified flag
    notified_key = low_balance_notified_key(user.id)
    fake_redis._store[notified_key] = "1"

    await add_credits(user.id, 10, TransactionType.PURCHASE, uuid.uuid4(), db_session, fake_redis)

    # new_balance=11 >= threshold=3 → key should be deleted
    assert notified_key not in fake_redis._store


# ---------------------------------------------------------------------------
# Test 25: low_balance_notified key NOT deleted when new_balance < threshold
# ---------------------------------------------------------------------------


async def test_add_credits_keeps_low_balance_notified_below_threshold(db_session, fake_redis):
    """add_credits does NOT delete low_balance_notified when balance < threshold."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    cb = CreditBalance(user_id=user.id, balance=0)
    db_session.add(cb)
    db_session.add(AppConfig(key="ride_credit_tiers", value=TIERS_JSON))
    await db_session.flush()

    notified_key = low_balance_notified_key(user.id)
    fake_redis._store[notified_key] = "1"

    await add_credits(user.id, 2, TransactionType.PURCHASE, uuid.uuid4(), db_session, fake_redis)

    # new_balance=2 < threshold=3 → key should remain
    assert fake_redis._store.get(notified_key) == "1"


# ---------------------------------------------------------------------------
# Test 26: Redis error — operation still succeeds
# ---------------------------------------------------------------------------


async def test_add_credits_succeeds_when_redis_unavailable(db_session):
    """add_credits succeeds even when Redis is completely unavailable."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    cb = CreditBalance(user_id=user.id, balance=10)
    db_session.add(cb)
    await db_session.flush()

    broken_redis = AsyncMock()
    broken_redis.get = AsyncMock(side_effect=RedisError("connection refused"))
    broken_redis.setex = AsyncMock(side_effect=RedisError("connection refused"))
    broken_redis.delete = AsyncMock(side_effect=RedisError("connection refused"))

    new_balance = await add_credits(
        user.id, 50, TransactionType.PURCHASE, uuid.uuid4(), db_session, broken_redis
    )

    assert new_balance == 60

    # Verify DB balance is actually updated
    result = await db_session.execute(
        select(CreditBalance.balance).where(CreditBalance.user_id == user.id)
    )
    assert result.scalar_one() == 60


# ---------------------------------------------------------------------------
# Test 27: Statement timeout raises HTTPException 503
# ---------------------------------------------------------------------------


async def test_add_credits_timeout_raises_503(fake_redis):
    """add_credits raises HTTPException(503) on statement timeout."""
    mock_db = AsyncMock()

    # First call: SET LOCAL statement_timeout succeeds
    # Second call: SELECT FOR UPDATE hits timeout
    timeout_error = OperationalError(
        "SELECT ... FOR UPDATE",
        {},
        Exception("canceling statement due to statement timeout"),
    )
    mock_db.execute = AsyncMock(side_effect=[None, timeout_error])

    with pytest.raises(HTTPException) as exc_info:
        await add_credits(
            uuid.uuid4(),
            50,
            TransactionType.PURCHASE,
            uuid.uuid4(),
            mock_db,
            fake_redis,
        )

    assert exc_info.value.status_code == 503


# ===========================================================================
# refund_credits tests
# ===========================================================================


# ---------------------------------------------------------------------------
# Test 28: refund_credits delegates to add_credits with RIDE_REFUND
# ---------------------------------------------------------------------------


async def test_refund_credits_delegates_to_add_credits(db_session, fake_redis):
    """refund_credits calls add_credits with TransactionType.RIDE_REFUND."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    cb = CreditBalance(user_id=user.id, balance=5)
    db_session.add(cb)
    await db_session.flush()

    ride_id = uuid.uuid4()
    new_balance = await refund_credits(user.id, 3, ride_id, db_session, fake_redis)

    assert new_balance == 8

    result = await db_session.execute(
        select(CreditTransaction).where(CreditTransaction.user_id == user.id)
    )
    tx = result.scalar_one()
    assert tx.type == TransactionType.RIDE_REFUND
    assert tx.amount == 3
    assert tx.balance_after == 8
    assert tx.reference_id == ride_id


# ---------------------------------------------------------------------------
# Test 29: refund_credits passes reference_id correctly
# ---------------------------------------------------------------------------


async def test_refund_credits_reference_id_stored(db_session, fake_redis):
    """refund_credits stores the ride_id as reference_id on the transaction."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    cb = CreditBalance(user_id=user.id, balance=10)
    db_session.add(cb)
    await db_session.flush()

    ride_id = uuid.uuid4()
    await refund_credits(user.id, 2, ride_id, db_session, fake_redis)

    result = await db_session.execute(
        select(CreditTransaction).where(CreditTransaction.user_id == user.id)
    )
    tx = result.scalar_one()
    assert tx.reference_id == ride_id


# ---------------------------------------------------------------------------
# Test 30: refund_credits rejects amount <= 0
# ---------------------------------------------------------------------------


async def test_refund_credits_rejects_zero_amount(db_session, fake_redis):
    """refund_credits raises ValueError when amount is 0."""
    with pytest.raises(ValueError, match="must be positive"):
        await refund_credits(uuid.uuid4(), 0, uuid.uuid4(), db_session, fake_redis)


async def test_refund_credits_rejects_negative_amount(db_session, fake_redis):
    """refund_credits raises ValueError when amount is negative."""
    with pytest.raises(ValueError, match="must be positive"):
        await refund_credits(uuid.uuid4(), -5, uuid.uuid4(), db_session, fake_redis)


# ---------------------------------------------------------------------------
# Test 31: refund_credits rejects None reference_id
# ---------------------------------------------------------------------------


async def test_refund_credits_rejects_none_reference_id(db_session, fake_redis):
    """refund_credits raises ValueError when reference_id is None."""
    with pytest.raises(ValueError, match="reference_id is required"):
        await refund_credits(uuid.uuid4(), 5, None, db_session, fake_redis)


# ===========================================================================
# Edge case tests
# ===========================================================================


# ---------------------------------------------------------------------------
# Test 32: Charge exactly full balance — balance becomes 0
# ---------------------------------------------------------------------------


async def test_charge_exactly_full_balance(db_session, fake_redis):
    """charge_credits charges exactly full balance, leaving balance at 0."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    cb = CreditBalance(user_id=user.id, balance=5)
    db_session.add(cb)
    await db_session.flush()

    charged, new_balance = await charge_credits(user.id, 5, uuid.uuid4(), db_session, fake_redis)

    assert charged == 5
    assert new_balance == 0

    result = await db_session.execute(
        select(CreditBalance.balance).where(CreditBalance.user_id == user.id)
    )
    assert result.scalar_one() == 0


# ---------------------------------------------------------------------------
# Test 33: Add then charge sequence — balance reflects both operations
# ---------------------------------------------------------------------------


async def test_add_then_charge_sequence(db_session, fake_redis):
    """Sequential add_credits then charge_credits updates balance correctly."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    cb = CreditBalance(user_id=user.id, balance=10)
    db_session.add(cb)
    await db_session.flush()

    # Add 50 credits (purchase)
    new_balance = await add_credits(
        user.id, 50, TransactionType.PURCHASE, uuid.uuid4(), db_session, fake_redis
    )
    assert new_balance == 60

    # Charge 15 credits (ride)
    charged, final_balance = await charge_credits(
        user.id, 15, uuid.uuid4(), db_session, fake_redis
    )
    assert charged == 15
    assert final_balance == 45

    # Verify DB state
    result = await db_session.execute(
        select(CreditBalance.balance).where(CreditBalance.user_id == user.id)
    )
    assert result.scalar_one() == 45


# ---------------------------------------------------------------------------
# Test 34: Multiple transactions create correct history
# ---------------------------------------------------------------------------


async def test_multiple_transactions_history(db_session, fake_redis):
    """Multiple operations produce correct transaction log with balance_after."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    cb = CreditBalance(user_id=user.id, balance=10)
    db_session.add(cb)
    await db_session.flush()

    # 1) Purchase +20 → balance 30
    await add_credits(user.id, 20, TransactionType.PURCHASE, uuid.uuid4(), db_session, fake_redis)
    # 2) Ride charge -5 → balance 25
    await charge_credits(user.id, 5, uuid.uuid4(), db_session, fake_redis)
    # 3) Refund +5 → balance 30
    await refund_credits(user.id, 5, uuid.uuid4(), db_session, fake_redis)

    result = await db_session.execute(
        select(CreditTransaction).where(CreditTransaction.user_id == user.id)
    )
    txs = result.scalars().all()

    assert len(txs) == 3

    by_type = {tx.type: tx for tx in txs}

    purchase = by_type[TransactionType.PURCHASE]
    assert purchase.amount == 20
    assert purchase.balance_after == 30

    charge = by_type[TransactionType.RIDE_CHARGE]
    assert charge.amount == -5
    assert charge.balance_after == 25

    refund = by_type[TransactionType.RIDE_REFUND]
    assert refund.amount == 5
    assert refund.balance_after == 30


# ===========================================================================
# get_ride_credit_cost tests (async — end-to-end tier matching with AppConfig)
# ===========================================================================


# ---------------------------------------------------------------------------
# Test 47: get_ride_credit_cost with default tiers
# ---------------------------------------------------------------------------


async def test_get_ride_credit_cost_default_tiers(db_session, fake_redis):
    """get_ride_credit_cost returns correct credits using default tiers."""
    assert await get_ride_credit_cost(15.0, db_session, fake_redis) == 1
    assert await get_ride_credit_cost(45.0, db_session, fake_redis) == 2
    assert await get_ride_credit_cost(100.0, db_session, fake_redis) == 3


# ---------------------------------------------------------------------------
# Test 48: get_ride_credit_cost with custom AppConfig tiers
# ---------------------------------------------------------------------------


async def test_get_ride_credit_cost_custom_tiers(db_session, fake_redis):
    """get_ride_credit_cost uses custom tiers from AppConfig."""
    custom_tiers = [
        {"max_price": 10.0, "credits": 1},
        {"max_price": 30.0, "credits": 3},
        {"max_price": None, "credits": 5},
    ]
    db_session.add(AppConfig(key="ride_credit_tiers", value=json.dumps(custom_tiers)))
    await db_session.flush()

    assert await get_ride_credit_cost(5.0, db_session, fake_redis) == 1
    assert await get_ride_credit_cost(10.0, db_session, fake_redis) == 1
    assert await get_ride_credit_cost(25.0, db_session, fake_redis) == 3
    assert await get_ride_credit_cost(50.0, db_session, fake_redis) == 5


# ---------------------------------------------------------------------------
# Test 49: get_ride_credit_cost with Redis unavailable falls back to DB
# ---------------------------------------------------------------------------


async def test_get_ride_credit_cost_redis_unavailable(db_session):
    """get_ride_credit_cost works when Redis is unavailable (falls back to DB)."""
    broken_redis = AsyncMock()
    broken_redis.get = AsyncMock(side_effect=RedisError("connection refused"))
    broken_redis.setex = AsyncMock(side_effect=RedisError("connection refused"))

    # No AppConfig → uses defaults
    result = await get_ride_credit_cost(45.0, db_session, broken_redis)
    assert result == 2
