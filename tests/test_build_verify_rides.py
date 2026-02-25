"""Tests for build_verify_rides throttle logic (task 6.4).

Test strategy:
1. last_verification_requested_at=NULL -> include in verify_rides
2. interval elapsed -> include
3. interval NOT elapsed -> NOT include
4. interval=0, deadline close, cycle_duration from request -> include
5. interval=0, deadline far -> NOT include
6. interval=0, cycle_duration absent -> fallback to interval_seconds * 2
7. last_verification_requested_at updated for included rides
"""

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.ride import Ride
from app.models.user import User
from app.services.ping_service import build_verify_rides

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _create_user(db: AsyncSession) -> User:
    user = User(
        email=f"vr-{uuid.uuid4().hex[:8]}@example.com",
        password_hash="hashed",
    )
    db.add(user)
    await db.flush()
    return user


async def _create_ride(
    db: AsyncSession,
    user_id: uuid.UUID,
    *,
    verification_status: str = "PENDING",
    verification_deadline: datetime | None = None,
    last_verification_requested_at: datetime | None = None,
    ride_hash: str | None = None,
) -> Ride:
    if verification_deadline is None:
        verification_deadline = datetime.now(UTC) + timedelta(hours=2)
    if ride_hash is None:
        ride_hash = uuid.uuid4().hex + uuid.uuid4().hex  # 64 hex chars
    ride = Ride(
        user_id=user_id,
        idempotency_key=str(uuid.uuid4()),
        event_type="ACCEPTED",
        ride_data={"price": 25.0, "pickup_time": "Tomorrow · 6:05AM"},
        ride_hash=ride_hash,
        verification_status=verification_status,
        verification_deadline=verification_deadline,
        last_verification_requested_at=last_verification_requested_at,
        credits_charged=2,
    )
    db.add(ride)
    await db.flush()
    return ride


async def _reload_ride(db: AsyncSession, ride_id: uuid.UUID) -> Ride:
    result = await db.execute(
        select(Ride).where(Ride.id == ride_id).execution_options(populate_existing=True)
    )
    return result.scalar_one()


# ---------------------------------------------------------------------------
# Test 1: last_verification_requested_at=NULL -> include in verify_rides
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_null_last_requested_included(db_session):
    user = await _create_user(db_session)
    ride = await _create_ride(
        db_session,
        user.id,
        last_verification_requested_at=None,
    )

    result = await build_verify_rides(
        db=db_session,
        user_id=user.id,
        check_interval_minutes=60,
        cycle_duration_ms=None,
        last_interval_sent=None,
    )

    assert len(result) == 1
    assert result[0] == ride.ride_hash


# ---------------------------------------------------------------------------
# Test 2: interval elapsed -> include
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_interval_elapsed_included(db_session):
    user = await _create_user(db_session)
    ride = await _create_ride(
        db_session,
        user.id,
        last_verification_requested_at=datetime.now(UTC) - timedelta(minutes=65),
    )

    result = await build_verify_rides(
        db=db_session,
        user_id=user.id,
        check_interval_minutes=60,
        cycle_duration_ms=None,
        last_interval_sent=None,
    )

    assert len(result) == 1
    assert result[0] == ride.ride_hash


# ---------------------------------------------------------------------------
# Test 3: interval NOT elapsed -> NOT include
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_interval_not_elapsed_excluded(db_session):
    user = await _create_user(db_session)
    await _create_ride(
        db_session,
        user.id,
        last_verification_requested_at=datetime.now(UTC) - timedelta(minutes=30),
    )

    result = await build_verify_rides(
        db=db_session,
        user_id=user.id,
        check_interval_minutes=60,
        cycle_duration_ms=None,
        last_interval_sent=None,
    )

    assert result == []


# ---------------------------------------------------------------------------
# Test 4: interval=0, deadline close, cycle_duration from request -> include
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_interval_zero_deadline_close_with_cycle_duration(db_session):
    user = await _create_user(db_session)
    # cycle_duration = 15s, threshold = 15 * 2 = 30s
    # Deadline in 20s (< 30s threshold) -> include
    ride = await _create_ride(
        db_session,
        user.id,
        verification_deadline=datetime.now(UTC) + timedelta(seconds=20),
    )

    result = await build_verify_rides(
        db=db_session,
        user_id=user.id,
        check_interval_minutes=0,
        cycle_duration_ms=15000,  # 15 seconds
        last_interval_sent=None,
    )

    assert len(result) == 1
    assert result[0] == ride.ride_hash


# ---------------------------------------------------------------------------
# Test 5: interval=0, deadline far -> NOT include
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_interval_zero_deadline_far_excluded(db_session):
    user = await _create_user(db_session)
    # cycle_duration = 15s, threshold = 15 * 2 = 30s
    # Deadline in 2 hours (>> 30s threshold) -> exclude
    await _create_ride(
        db_session,
        user.id,
        verification_deadline=datetime.now(UTC) + timedelta(hours=2),
    )

    result = await build_verify_rides(
        db=db_session,
        user_id=user.id,
        check_interval_minutes=0,
        cycle_duration_ms=15000,
        last_interval_sent=None,
    )

    assert result == []


# ---------------------------------------------------------------------------
# Test 6: interval=0, cycle_duration absent -> fallback to last_interval_sent * 2
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_interval_zero_fallback_to_last_interval_sent(db_session):
    user = await _create_user(db_session)
    # last_interval_sent = 30, cycle_duration fallback = 30 * 2 = 60s
    # threshold = 60 * 2 = 120s
    # Deadline in 100s (< 120s) -> include
    ride = await _create_ride(
        db_session,
        user.id,
        verification_deadline=datetime.now(UTC) + timedelta(seconds=100),
    )

    result = await build_verify_rides(
        db=db_session,
        user_id=user.id,
        check_interval_minutes=0,
        cycle_duration_ms=None,
        last_interval_sent=30,
    )

    assert len(result) == 1
    assert result[0] == ride.ride_hash


@pytest.mark.asyncio
async def test_interval_zero_fallback_deadline_outside_threshold(db_session):
    """Complement to test 6: deadline beyond fallback threshold -> exclude."""
    user = await _create_user(db_session)
    # last_interval_sent = 30, cycle_duration fallback = 60s, threshold = 120s
    # Deadline in 200s (> 120s) -> exclude
    await _create_ride(
        db_session,
        user.id,
        verification_deadline=datetime.now(UTC) + timedelta(seconds=200),
    )

    result = await build_verify_rides(
        db=db_session,
        user_id=user.id,
        check_interval_minutes=0,
        cycle_duration_ms=None,
        last_interval_sent=30,
    )

    assert result == []


# ---------------------------------------------------------------------------
# Test 7: last_verification_requested_at updated for included rides
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_last_verification_requested_at_updated(db_session):
    user = await _create_user(db_session)
    ride = await _create_ride(
        db_session,
        user.id,
        last_verification_requested_at=None,
    )
    assert ride.last_verification_requested_at is None

    before = datetime.now(UTC)
    await build_verify_rides(
        db=db_session,
        user_id=user.id,
        check_interval_minutes=60,
        cycle_duration_ms=None,
        last_interval_sent=None,
    )
    await db_session.commit()

    ride = await _reload_ride(db_session, ride.id)
    assert ride.last_verification_requested_at is not None
    assert ride.last_verification_requested_at >= before


@pytest.mark.asyncio
async def test_excluded_ride_not_updated(db_session):
    """Rides that don't pass the throttle should NOT have their timestamp updated."""
    user = await _create_user(db_session)
    original_ts = datetime.now(UTC) - timedelta(minutes=30)
    ride = await _create_ride(
        db_session,
        user.id,
        last_verification_requested_at=original_ts,
    )

    await build_verify_rides(
        db=db_session,
        user_id=user.id,
        check_interval_minutes=60,  # 30 min < 60 min -> excluded
        cycle_duration_ms=None,
        last_interval_sent=None,
    )
    await db_session.commit()

    ride = await _reload_ride(db_session, ride.id)
    # Timestamp unchanged (within 1 second tolerance for DB precision).
    delta = abs((ride.last_verification_requested_at - original_ts).total_seconds())
    assert delta < 1


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_pending_rides_returns_empty(db_session):
    """No PENDING rides -> empty list."""
    user = await _create_user(db_session)
    # CONFIRMED ride should be ignored.
    await _create_ride(
        db_session,
        user.id,
        verification_status="CONFIRMED",
    )

    result = await build_verify_rides(
        db=db_session,
        user_id=user.id,
        check_interval_minutes=60,
        cycle_duration_ms=None,
        last_interval_sent=None,
    )

    assert result == []


@pytest.mark.asyncio
async def test_expired_deadline_excluded(db_session):
    """PENDING rides whose deadline already passed should NOT be included."""
    user = await _create_user(db_session)
    await _create_ride(
        db_session,
        user.id,
        verification_deadline=datetime.now(UTC) - timedelta(hours=1),
        last_verification_requested_at=None,
    )

    result = await build_verify_rides(
        db=db_session,
        user_id=user.id,
        check_interval_minutes=60,
        cycle_duration_ms=None,
        last_interval_sent=None,
    )

    assert result == []


@pytest.mark.asyncio
async def test_mixed_rides_only_eligible_included(db_session):
    """Only rides passing the throttle are included; others are excluded."""
    user = await _create_user(db_session)

    # Eligible: never checked.
    ride_a = await _create_ride(
        db_session,
        user.id,
        last_verification_requested_at=None,
        ride_hash="a" * 64,
    )
    # Eligible: checked 90 minutes ago (> 60 min interval).
    ride_b = await _create_ride(
        db_session,
        user.id,
        last_verification_requested_at=datetime.now(UTC) - timedelta(minutes=90),
        ride_hash="b" * 64,
    )
    # Not eligible: checked 20 minutes ago (< 60 min interval).
    await _create_ride(
        db_session,
        user.id,
        last_verification_requested_at=datetime.now(UTC) - timedelta(minutes=20),
        ride_hash="c" * 64,
    )

    result = await build_verify_rides(
        db=db_session,
        user_id=user.id,
        check_interval_minutes=60,
        cycle_duration_ms=None,
        last_interval_sent=None,
    )

    assert sorted(result) == sorted([ride_a.ride_hash, ride_b.ride_hash])


@pytest.mark.asyncio
async def test_other_users_rides_not_included(db_session):
    """Rides belonging to another user must not appear."""
    user_a = await _create_user(db_session)
    user_b = await _create_user(db_session)

    # Ride owned by user_b.
    await _create_ride(db_session, user_b.id, last_verification_requested_at=None)

    result = await build_verify_rides(
        db=db_session,
        user_id=user_a.id,
        check_interval_minutes=60,
        cycle_duration_ms=None,
        last_interval_sent=None,
    )

    assert result == []
