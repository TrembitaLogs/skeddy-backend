"""Tests for process_expired_verifications (task 6.3).

Test strategy:
1.  deadline expired, last_reported_present=true  -> CONFIRMED
2.  deadline expired, last_reported_present=NULL  -> CONFIRMED (benefit of doubt)
3.  deadline expired, last_reported_present=false -> CANCELLED
4.  concurrent ping — only one handler processes (affected_rows check)
5.  deadline NOT expired -> ride untouched
6.  CANCELLED + credits_charged > 0 -> refund in same transaction
7.  CANCELLED + credits_charged = 0 -> no refund
8.  balance increased by credits_charged after CANCELLED refund
9.  CreditTransaction RIDE_REFUND created with reference_id = ride_id
10. refund error -> rollback (verification_status stays PENDING)
11. verified_at and credits_refunded recorded on Ride
"""

import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.credit_balance import CreditBalance
from app.models.credit_transaction import CreditTransaction, TransactionType
from app.models.ride import Ride
from app.models.user import User
from app.services.ping_service import process_expired_verifications

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _create_user(db: AsyncSession) -> User:
    """Create a test user."""
    user = User(
        email=f"verify-{uuid.uuid4().hex[:8]}@example.com",
        password_hash="hashed",
    )
    db.add(user)
    await db.flush()
    return user


async def _create_balance(
    db: AsyncSession, user_id: uuid.UUID, balance: int = 10
) -> CreditBalance:
    """Create a CreditBalance row for the user."""
    cb = CreditBalance(user_id=user_id, balance=balance)
    db.add(cb)
    await db.flush()
    return cb


async def _create_ride(
    db: AsyncSession,
    user_id: uuid.UUID,
    *,
    verification_status: str = "PENDING",
    verification_deadline: datetime | None = None,
    last_reported_present: bool | None = None,
    credits_charged: int = 2,
    credits_refunded: int = 0,
) -> Ride:
    """Create a Ride for verification tests."""
    if verification_deadline is None:
        # Default: deadline already passed (1 hour ago).
        verification_deadline = datetime.now(UTC) - timedelta(hours=1)
    ride = Ride(
        user_id=user_id,
        idempotency_key=str(uuid.uuid4()),
        event_type="ACCEPTED",
        ride_data={"price": 25.0, "pickup_time": "Tomorrow · 6:05AM"},
        ride_hash=uuid.uuid4().hex + uuid.uuid4().hex,  # 64 hex chars
        verification_status=verification_status,
        verification_deadline=verification_deadline,
        last_reported_present=last_reported_present,
        credits_charged=credits_charged,
        credits_refunded=credits_refunded,
    )
    db.add(ride)
    await db.flush()
    return ride


def _make_fake_redis() -> AsyncMock:
    """Create an in-memory fake Redis for cache_balance calls."""
    store: dict[str, str] = {}

    async def mock_get(key):
        return store.get(key)

    async def mock_setex(key, ttl, value):
        store[key] = value

    async def mock_delete(*keys):
        count = 0
        for key in keys:
            if key in store:
                del store[key]
                count += 1
        return count

    redis = AsyncMock()
    redis.get = AsyncMock(side_effect=mock_get)
    redis.setex = AsyncMock(side_effect=mock_setex)
    redis.delete = AsyncMock(side_effect=mock_delete)
    redis._store = store
    return redis


async def _reload_ride(db: AsyncSession, ride_id: uuid.UUID) -> Ride:
    """Re-fetch a Ride from DB to get fresh column values after raw SQL UPDATE."""
    result = await db.execute(
        select(Ride).where(Ride.id == ride_id).execution_options(populate_existing=True)
    )
    return result.scalar_one()


async def _reload_balance(db: AsyncSession, user_id: uuid.UUID) -> int:
    """Re-fetch balance from DB."""
    result = await db.execute(
        select(CreditBalance.balance).where(CreditBalance.user_id == user_id)
    )
    return result.scalar_one()


# ---------------------------------------------------------------------------
# Test 1: deadline expired, last_reported_present=true -> CONFIRMED
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_expired_present_true_confirmed(db_session, fake_redis):
    user = await _create_user(db_session)
    await _create_balance(db_session, user.id, balance=10)
    ride = await _create_ride(db_session, user.id, last_reported_present=True, credits_charged=2)

    result = await process_expired_verifications(db_session, user.id, fake_redis)
    await db_session.commit()

    ride = await _reload_ride(db_session, ride.id)
    assert ride.verification_status == "CONFIRMED"
    assert result == []  # No refunds for CONFIRMED rides


# ---------------------------------------------------------------------------
# Test 2: deadline expired, last_reported_present=NULL -> CONFIRMED
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_expired_present_null_confirmed(db_session, fake_redis):
    user = await _create_user(db_session)
    await _create_balance(db_session, user.id, balance=10)
    ride = await _create_ride(db_session, user.id, last_reported_present=None, credits_charged=2)

    result = await process_expired_verifications(db_session, user.id, fake_redis)
    await db_session.commit()

    ride = await _reload_ride(db_session, ride.id)
    assert ride.verification_status == "CONFIRMED"
    assert result == []


# ---------------------------------------------------------------------------
# Test 3: deadline expired, last_reported_present=false -> CANCELLED
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_expired_present_false_cancelled(db_session, fake_redis):
    user = await _create_user(db_session)
    await _create_balance(db_session, user.id, balance=10)
    ride = await _create_ride(db_session, user.id, last_reported_present=False, credits_charged=2)

    result = await process_expired_verifications(db_session, user.id, fake_redis)
    await db_session.commit()

    ride = await _reload_ride(db_session, ride.id)
    assert ride.verification_status == "CANCELLED"
    assert len(result) == 1
    assert result[0]["ride_id"] == ride.id
    assert result[0]["credits_refunded"] == 2


# ---------------------------------------------------------------------------
# Test 4: concurrent ping — only one processes (affected_rows check)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_concurrent_only_one_processes(db_session, fake_redis):
    user = await _create_user(db_session)
    await _create_balance(db_session, user.id, balance=10)
    ride = await _create_ride(db_session, user.id, last_reported_present=True, credits_charged=2)

    # First call processes the ride.
    result1 = await process_expired_verifications(db_session, user.id, fake_redis)
    await db_session.commit()

    ride = await _reload_ride(db_session, ride.id)
    assert ride.verification_status == "CONFIRMED"

    # Second call finds no PENDING rides — nothing to process.
    result2 = await process_expired_verifications(db_session, user.id, fake_redis)
    assert result2 == []
    assert result1 == []  # CONFIRMED, no refunds


# ---------------------------------------------------------------------------
# Test 5: deadline NOT expired -> ride untouched
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_deadline_not_expired_untouched(db_session, fake_redis):
    user = await _create_user(db_session)
    await _create_balance(db_session, user.id, balance=10)
    future_deadline = datetime.now(UTC) + timedelta(hours=2)
    ride = await _create_ride(
        db_session,
        user.id,
        last_reported_present=False,
        credits_charged=2,
        verification_deadline=future_deadline,
    )

    result = await process_expired_verifications(db_session, user.id, fake_redis)
    await db_session.commit()

    ride = await _reload_ride(db_session, ride.id)
    assert ride.verification_status == "PENDING"
    assert ride.verified_at is None
    assert result == []


# ---------------------------------------------------------------------------
# Test 6: CANCELLED + credits_charged > 0 -> refund in same transaction
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancelled_with_credits_refund_executed(db_session, fake_redis):
    user = await _create_user(db_session)
    await _create_balance(db_session, user.id, balance=5)
    ride = await _create_ride(db_session, user.id, last_reported_present=False, credits_charged=3)

    result = await process_expired_verifications(db_session, user.id, fake_redis)
    await db_session.commit()

    assert len(result) == 1
    assert result[0]["new_balance"] == 8  # 5 + 3

    ride = await _reload_ride(db_session, ride.id)
    assert ride.verification_status == "CANCELLED"
    assert ride.credits_refunded == 3


# ---------------------------------------------------------------------------
# Test 7: CANCELLED + credits_charged = 0 -> no refund
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancelled_zero_credits_no_refund(db_session, fake_redis):
    user = await _create_user(db_session)
    await _create_balance(db_session, user.id, balance=5)
    ride = await _create_ride(db_session, user.id, last_reported_present=False, credits_charged=0)

    result = await process_expired_verifications(db_session, user.id, fake_redis)
    await db_session.commit()

    ride = await _reload_ride(db_session, ride.id)
    assert ride.verification_status == "CANCELLED"
    assert ride.credits_refunded == 0
    assert result == []  # No refund info returned

    # Balance unchanged.
    balance = await _reload_balance(db_session, user.id)
    assert balance == 5


# ---------------------------------------------------------------------------
# Test 8: balance increased by credits_charged after CANCELLED refund
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_balance_increased_after_refund(db_session, fake_redis):
    user = await _create_user(db_session)
    await _create_balance(db_session, user.id, balance=7)
    await _create_ride(db_session, user.id, last_reported_present=False, credits_charged=3)

    await process_expired_verifications(db_session, user.id, fake_redis)
    await db_session.commit()

    balance = await _reload_balance(db_session, user.id)
    assert balance == 10  # 7 + 3


# ---------------------------------------------------------------------------
# Test 9: CreditTransaction RIDE_REFUND created with reference_id = ride_id
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_credit_transaction_ride_refund_created(db_session, fake_redis):
    user = await _create_user(db_session)
    await _create_balance(db_session, user.id, balance=5)
    ride = await _create_ride(db_session, user.id, last_reported_present=False, credits_charged=2)

    await process_expired_verifications(db_session, user.id, fake_redis)
    await db_session.commit()

    result = await db_session.execute(
        select(CreditTransaction).where(
            CreditTransaction.user_id == user.id,
            CreditTransaction.type == TransactionType.RIDE_REFUND,
        )
    )
    txn = result.scalar_one()

    assert txn.amount == 2
    assert txn.balance_after == 7  # 5 + 2
    assert txn.reference_id == ride.id


# ---------------------------------------------------------------------------
# Test 10: refund error -> rollback (verification_status stays PENDING)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_refund_error_rollback_keeps_pending(db_session, fake_redis):
    user = await _create_user(db_session)
    await _create_balance(db_session, user.id, balance=5)
    ride = await _create_ride(db_session, user.id, last_reported_present=False, credits_charged=2)
    # Capture IDs before the function call — sa_update(Ride) inside
    # begin_nested() expires all Ride instances in the session, making
    # attribute access unsafe after a savepoint rollback.
    ride_id = ride.id
    user_id = user.id

    # Patch refund_credits_in_txn to simulate a lock failure
    # (OperationalError with "could not obtain lock" message).
    from sqlalchemy.exc import OperationalError

    lock_error = OperationalError("could not obtain lock on row", params=None, orig=Exception())
    with patch(
        "app.services.ping_service.refund_credits_in_txn",
        side_effect=lock_error,
    ):
        result = await process_expired_verifications(db_session, user_id, fake_redis)
    await db_session.commit()

    # Ride should stay PENDING because the savepoint was rolled back.
    ride = await _reload_ride(db_session, ride_id)
    assert ride.verification_status == "PENDING"
    assert ride.verified_at is None
    assert ride.credits_refunded == 0

    # Balance unchanged.
    balance = await _reload_balance(db_session, user_id)
    assert balance == 5

    # No refund info returned.
    assert result == []


# ---------------------------------------------------------------------------
# Test 11: verified_at and credits_refunded recorded on Ride
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_verified_at_and_credits_refunded_recorded(db_session, fake_redis):
    user = await _create_user(db_session)
    await _create_balance(db_session, user.id, balance=10)

    # CONFIRMED ride — verified_at set, credits_refunded stays 0.
    ride_confirmed = await _create_ride(
        db_session, user.id, last_reported_present=True, credits_charged=2
    )
    # CANCELLED ride — verified_at set, credits_refunded = credits_charged.
    ride_cancelled = await _create_ride(
        db_session, user.id, last_reported_present=False, credits_charged=3
    )

    await process_expired_verifications(db_session, user.id, fake_redis)
    await db_session.commit()

    rc = await _reload_ride(db_session, ride_confirmed.id)
    assert rc.verification_status == "CONFIRMED"
    assert rc.verified_at is not None
    assert rc.credits_refunded == 0

    rx = await _reload_ride(db_session, ride_cancelled.id)
    assert rx.verification_status == "CANCELLED"
    assert rx.verified_at is not None
    assert rx.credits_refunded == 3
