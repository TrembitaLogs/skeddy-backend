import asyncio
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest

from app.models.accept_failure import AcceptFailure
from app.models.ride import Ride
from app.models.user import User
from app.tasks.data_cleanup import (
    cleanup_old_data,
    delete_old_accept_failures,
    delete_old_rides,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_user(email: str = "datacleanup@example.com") -> User:
    return User(
        id=uuid.uuid4(),
        email=email,
        password_hash="fakehash",
    )


def _make_ride(user: User, *, weeks_ago: int) -> Ride:
    """Create a Ride with created_at set to *weeks_ago* weeks in the past."""
    created_at = datetime.now(UTC) - timedelta(weeks=weeks_ago)
    return Ride(
        id=uuid.uuid4(),
        user_id=user.id,
        idempotency_key=str(uuid.uuid4()),
        event_type="ACCEPTED",
        ride_data={"price": "25.00", "pickup_location": "Test"},
        created_at=created_at,
    )


def _make_accept_failure(user: User, *, weeks_ago: int) -> AcceptFailure:
    """Create an AcceptFailure with reported_at set to *weeks_ago* weeks in the past."""
    reported_at = datetime.now(UTC) - timedelta(weeks=weeks_ago)
    return AcceptFailure(
        id=uuid.uuid4(),
        user_id=user.id,
        reason="ACCESSIBILITY_NOT_ENABLED",
        reported_at=reported_at,
    )


# ---------------------------------------------------------------------------
# delete_old_rides — integration tests (real DB)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_deletes_old_rides(db_session):
    """Rides older than 8 weeks should be deleted."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    old_ride = _make_ride(user, weeks_ago=9)
    db_session.add(old_ride)
    await db_session.flush()

    cutoff = datetime.now(UTC) - timedelta(weeks=8)
    deleted = await delete_old_rides(db_session, cutoff)

    assert deleted == 1

    from sqlalchemy import select

    result = await db_session.execute(select(Ride).where(Ride.id == old_ride.id))
    assert result.scalar_one_or_none() is None


@pytest.mark.asyncio
async def test_keeps_recent_rides(db_session):
    """Rides younger than 8 weeks should NOT be deleted."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    recent_ride = _make_ride(user, weeks_ago=4)
    db_session.add(recent_ride)
    await db_session.flush()

    cutoff = datetime.now(UTC) - timedelta(weeks=8)
    deleted = await delete_old_rides(db_session, cutoff)

    assert deleted == 0

    from sqlalchemy import select

    result = await db_session.execute(select(Ride).where(Ride.id == recent_ride.id))
    assert result.scalar_one_or_none() is not None


@pytest.mark.asyncio
async def test_mixed_old_and_recent_rides(db_session):
    """Only old rides are deleted; recent ones remain."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    old1 = _make_ride(user, weeks_ago=10)
    old2 = _make_ride(user, weeks_ago=52)
    recent1 = _make_ride(user, weeks_ago=1)
    recent2 = _make_ride(user, weeks_ago=7)
    db_session.add_all([old1, old2, recent1, recent2])
    await db_session.flush()

    cutoff = datetime.now(UTC) - timedelta(weeks=8)
    deleted = await delete_old_rides(db_session, cutoff)

    assert deleted == 2

    from sqlalchemy import select

    result = await db_session.execute(select(Ride))
    remaining = result.scalars().all()
    remaining_ids = {r.id for r in remaining}
    assert recent1.id in remaining_ids
    assert recent2.id in remaining_ids
    assert old1.id not in remaining_ids
    assert old2.id not in remaining_ids


# ---------------------------------------------------------------------------
# delete_old_accept_failures — integration tests (real DB)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_deletes_old_accept_failures(db_session):
    """Accept failures older than 8 weeks should be deleted."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    old_failure = _make_accept_failure(user, weeks_ago=9)
    db_session.add(old_failure)
    await db_session.flush()

    cutoff = datetime.now(UTC) - timedelta(weeks=8)
    deleted = await delete_old_accept_failures(db_session, cutoff)

    assert deleted == 1

    from sqlalchemy import select

    result = await db_session.execute(
        select(AcceptFailure).where(AcceptFailure.id == old_failure.id)
    )
    assert result.scalar_one_or_none() is None


@pytest.mark.asyncio
async def test_keeps_recent_accept_failures(db_session):
    """Accept failures younger than 8 weeks should NOT be deleted."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    recent_failure = _make_accept_failure(user, weeks_ago=3)
    db_session.add(recent_failure)
    await db_session.flush()

    cutoff = datetime.now(UTC) - timedelta(weeks=8)
    deleted = await delete_old_accept_failures(db_session, cutoff)

    assert deleted == 0

    from sqlalchemy import select

    result = await db_session.execute(
        select(AcceptFailure).where(AcceptFailure.id == recent_failure.id)
    )
    assert result.scalar_one_or_none() is not None


@pytest.mark.asyncio
async def test_no_records_at_all(db_session):
    """Empty tables — should return 0 deleted for both."""
    cutoff = datetime.now(UTC) - timedelta(weeks=8)

    deleted_rides = await delete_old_rides(db_session, cutoff)
    assert deleted_rides == 0

    deleted_failures = await delete_old_accept_failures(db_session, cutoff)
    assert deleted_failures == 0


# ---------------------------------------------------------------------------
# Batch deletion — integration tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_batch_deletion_rides(db_session):
    """Batch deletion should handle more records than BATCH_SIZE correctly."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    rides = [_make_ride(user, weeks_ago=10) for _ in range(5)]
    db_session.add_all(rides)
    await db_session.flush()

    cutoff = datetime.now(UTC) - timedelta(weeks=8)

    with patch("app.tasks.data_cleanup.BATCH_SIZE", 2):
        deleted = await delete_old_rides(db_session, cutoff)

    assert deleted == 5

    from sqlalchemy import select

    result = await db_session.execute(select(Ride))
    remaining = result.scalars().all()
    assert len(remaining) == 0


@pytest.mark.asyncio
async def test_batch_deletion_accept_failures(db_session):
    """Batch deletion should handle more accept failures than BATCH_SIZE."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    failures = [_make_accept_failure(user, weeks_ago=12) for _ in range(5)]
    db_session.add_all(failures)
    await db_session.flush()

    cutoff = datetime.now(UTC) - timedelta(weeks=8)

    with patch("app.tasks.data_cleanup.BATCH_SIZE", 2):
        deleted = await delete_old_accept_failures(db_session, cutoff)

    assert deleted == 5

    from sqlalchemy import select

    result = await db_session.execute(select(AcceptFailure))
    remaining = result.scalars().all()
    assert len(remaining) == 0


# ---------------------------------------------------------------------------
# cleanup_old_data — unit tests (mocked DB + sleep)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cleanup_loop_calls_delete_functions():
    """The loop should call both delete functions on each iteration."""
    mock_db = AsyncMock()

    @asynccontextmanager
    async def mock_session_factory():
        yield mock_db

    call_count = 0

    with (
        patch(
            "app.tasks.data_cleanup.AsyncSessionLocal",
            side_effect=mock_session_factory,
        ),
        patch(
            "app.tasks.data_cleanup.delete_old_rides",
            new_callable=AsyncMock,
            return_value=3,
        ) as mock_del_rides,
        patch(
            "app.tasks.data_cleanup.delete_old_accept_failures",
            new_callable=AsyncMock,
            return_value=2,
        ) as mock_del_failures,
        patch(
            "app.tasks.data_cleanup.asyncio.sleep",
            new_callable=AsyncMock,
        ) as mock_sleep,
    ):

        async def sleep_side_effect(seconds):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                # Second sleep (after work iteration) — stop the loop
                raise asyncio.CancelledError()

        mock_sleep.side_effect = sleep_side_effect

        with pytest.raises(asyncio.CancelledError):
            await cleanup_old_data()

        mock_del_rides.assert_called_once()
        mock_del_failures.assert_called_once()

        # Verify cutoff is approximately 8 weeks ago
        ride_call_args = mock_del_rides.call_args
        assert ride_call_args[0][0] is mock_db
        cutoff = ride_call_args[0][1]
        expected = datetime.now(UTC) - timedelta(weeks=8)
        assert abs((cutoff - expected).total_seconds()) < 5


@pytest.mark.asyncio
async def test_cleanup_continues_after_db_error():
    """The loop should catch exceptions and continue (not crash)."""
    call_count = 0

    @asynccontextmanager
    async def mock_session_factory():
        raise RuntimeError("DB connection failed")
        yield  # pragma: no cover

    with (
        patch(
            "app.tasks.data_cleanup.AsyncSessionLocal",
            side_effect=mock_session_factory,
        ),
        patch(
            "app.tasks.data_cleanup.asyncio.sleep",
            new_callable=AsyncMock,
        ) as mock_sleep,
    ):

        async def sleep_side_effect(seconds):
            nonlocal call_count
            call_count += 1
            if call_count >= 3:
                # Initial delay + 2 work iterations — stop after surviving 2 errors
                raise asyncio.CancelledError()

        mock_sleep.side_effect = sleep_side_effect

        with pytest.raises(asyncio.CancelledError):
            await cleanup_old_data()

        # Should have survived: initial sleep(3600) + sleep after error #1 + sleep after error #2
        assert call_count == 3


@pytest.mark.asyncio
async def test_cleanup_uses_correct_interval_and_delay():
    """The initial delay should be 3600s, and work interval should be 86400s."""
    mock_db = AsyncMock()

    @asynccontextmanager
    async def mock_session_factory():
        yield mock_db

    sleep_calls = []

    with (
        patch(
            "app.tasks.data_cleanup.AsyncSessionLocal",
            side_effect=mock_session_factory,
        ),
        patch(
            "app.tasks.data_cleanup.delete_old_rides",
            new_callable=AsyncMock,
            return_value=0,
        ),
        patch(
            "app.tasks.data_cleanup.delete_old_accept_failures",
            new_callable=AsyncMock,
            return_value=0,
        ),
        patch(
            "app.tasks.data_cleanup.asyncio.sleep",
            new_callable=AsyncMock,
        ) as mock_sleep,
    ):

        async def sleep_side_effect(seconds):
            sleep_calls.append(seconds)
            if len(sleep_calls) >= 2:
                raise asyncio.CancelledError()

        mock_sleep.side_effect = sleep_side_effect

        with pytest.raises(asyncio.CancelledError):
            await cleanup_old_data()

        # First call: initial delay (3600s), second call: cleanup interval (86400s)
        assert sleep_calls[0] == 3600
        assert sleep_calls[1] == 86400
