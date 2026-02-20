from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from app.models.ride import Ride
from app.models.user import User
from app.services.ride_service import (
    create_ride,
    get_ride_by_idempotency,
    get_user_fcm_token,
    get_user_ride_events,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _create_user(db, email="test@example.com", fcm_token=None) -> User:
    """Insert a User row and return it."""
    user = User(email=email, password_hash="hashed", fcm_token=fcm_token)
    db.add(user)
    await db.flush()
    return user


async def _create_ride(
    db, user_id, idempotency_key=None, event_type="ACCEPTED", ride_data=None
) -> Ride:
    """Insert a Ride row directly via model (bypasses service layer)."""
    ride = Ride(
        user_id=user_id,
        idempotency_key=idempotency_key or str(uuid4()),
        event_type=event_type,
        ride_data=ride_data or {"price": 25.0, "pickup_time": "10:00 AM"},
    )
    db.add(ride)
    await db.flush()
    return ride


# ===========================================================================
# get_ride_by_idempotency
# ===========================================================================


async def test_get_ride_by_idempotency_finds_existing(db_session):
    """Should return the existing ride when user_id + idempotency_key match."""
    user = await _create_user(db_session)
    key = str(uuid4())
    ride = await _create_ride(db_session, user.id, idempotency_key=key)

    found = await get_ride_by_idempotency(db_session, user.id, key)

    assert found is not None
    assert found.id == ride.id
    assert found.idempotency_key == key


async def test_get_ride_by_idempotency_returns_none_when_not_found(db_session):
    """Should return None when no ride matches the idempotency_key."""
    user = await _create_user(db_session)

    found = await get_ride_by_idempotency(db_session, user.id, str(uuid4()))

    assert found is None


async def test_get_ride_by_idempotency_scoped_to_user(db_session):
    """Idempotency key is unique per user — same key for different users returns None."""
    user_a = await _create_user(db_session, "a@example.com")
    user_b = await _create_user(db_session, "b@example.com")
    key = str(uuid4())
    await _create_ride(db_session, user_a.id, idempotency_key=key)

    found = await get_ride_by_idempotency(db_session, user_b.id, key)

    assert found is None


# ===========================================================================
# create_ride
# ===========================================================================


async def test_create_ride_returns_ride_with_correct_fields(db_session):
    """Should create a ride with all fields correctly populated."""
    user = await _create_user(db_session)
    key = str(uuid4())
    data = {
        "price": 35.0,
        "pickup_time": "2:30 PM",
        "pickup_location": "123 Main St",
        "dropoff_location": "456 Oak Ave",
    }

    ride = await create_ride(
        db_session,
        user_id=user.id,
        idempotency_key=key,
        event_type="ACCEPTED",
        ride_data=data,
    )

    assert ride.id is not None
    assert ride.user_id == user.id
    assert ride.idempotency_key == key
    assert ride.event_type == "ACCEPTED"
    assert ride.ride_data == data


async def test_create_ride_persists_to_database(db_session):
    """Ride should be retrievable from DB after flush."""
    user = await _create_user(db_session)
    key = str(uuid4())

    ride = await create_ride(
        db_session,
        user_id=user.id,
        idempotency_key=key,
        event_type="ACCEPTED",
        ride_data={"price": 20.0},
    )

    result = await db_session.execute(select(Ride).where(Ride.id == ride.id))
    db_ride = result.scalar_one()
    assert db_ride.idempotency_key == key
    assert db_ride.ride_data == {"price": 20.0}


async def test_create_ride_duplicate_idempotency_key_raises_integrity_error(db_session):
    """Inserting a ride with duplicate (user_id, idempotency_key) should raise IntegrityError."""
    user = await _create_user(db_session)
    key = str(uuid4())

    await create_ride(
        db_session,
        user_id=user.id,
        idempotency_key=key,
        event_type="ACCEPTED",
        ride_data={"price": 25.0},
    )

    with pytest.raises(IntegrityError):
        await create_ride(
            db_session,
            user_id=user.id,
            idempotency_key=key,
            event_type="ACCEPTED",
            ride_data={"price": 30.0},
        )


async def test_create_ride_same_key_different_users_ok(db_session):
    """Same idempotency_key for different users should not conflict."""
    user_a = await _create_user(db_session, "a@example.com")
    user_b = await _create_user(db_session, "b@example.com")
    key = str(uuid4())

    ride_a = await create_ride(
        db_session,
        user_id=user_a.id,
        idempotency_key=key,
        event_type="ACCEPTED",
        ride_data={"price": 25.0},
    )
    ride_b = await create_ride(
        db_session,
        user_id=user_b.id,
        idempotency_key=key,
        event_type="ACCEPTED",
        ride_data={"price": 30.0},
    )

    assert ride_a.id != ride_b.id


# ===========================================================================
# get_user_ride_events
# ===========================================================================


async def test_get_user_ride_events_returns_events_and_total(db_session):
    """Should return the correct list of rides and total count."""
    user = await _create_user(db_session)
    for i in range(3):
        await _create_ride(db_session, user.id, ride_data={"price": 10.0 + i})

    events, total = await get_user_ride_events(db_session, user.id, limit=10, offset=0)

    assert total == 3
    assert len(events) == 3


async def test_get_user_ride_events_ordered_newest_first(db_session):
    """Events should be ordered by created_at descending (newest first)."""
    user = await _create_user(db_session)
    base_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

    ride1 = Ride(
        user_id=user.id,
        idempotency_key=str(uuid4()),
        event_type="ACCEPTED",
        ride_data={"price": 10.0},
        created_at=base_time,
    )
    ride2 = Ride(
        user_id=user.id,
        idempotency_key=str(uuid4()),
        event_type="ACCEPTED",
        ride_data={"price": 20.0},
        created_at=base_time + timedelta(hours=1),
    )
    ride3 = Ride(
        user_id=user.id,
        idempotency_key=str(uuid4()),
        event_type="ACCEPTED",
        ride_data={"price": 30.0},
        created_at=base_time + timedelta(hours=2),
    )
    db_session.add_all([ride1, ride2, ride3])
    await db_session.flush()

    events, _ = await get_user_ride_events(db_session, user.id, limit=10, offset=0)

    # Newest first
    assert events[0].id == ride3.id
    assert events[1].id == ride2.id
    assert events[2].id == ride1.id


async def test_get_user_ride_events_pagination_limit(db_session):
    """Limit should restrict the number of returned events."""
    user = await _create_user(db_session)
    for _ in range(5):
        await _create_ride(db_session, user.id)

    events, total = await get_user_ride_events(db_session, user.id, limit=2, offset=0)

    assert total == 5
    assert len(events) == 2


async def test_get_user_ride_events_pagination_offset(db_session):
    """Offset should skip the specified number of events."""
    user = await _create_user(db_session)
    for _ in range(5):
        await _create_ride(db_session, user.id)

    events, total = await get_user_ride_events(db_session, user.id, limit=10, offset=3)

    assert total == 5
    assert len(events) == 2  # 5 total - 3 skipped = 2 remaining


async def test_get_user_ride_events_empty_for_no_rides(db_session):
    """Should return empty list and zero total when user has no rides."""
    user = await _create_user(db_session)

    events, total = await get_user_ride_events(db_session, user.id, limit=10, offset=0)

    assert total == 0
    assert events == []


async def test_get_user_ride_events_since_filters_old_events(db_session):
    """Since parameter should exclude events created before the cutoff."""
    user = await _create_user(db_session)
    base_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

    old_ride = Ride(
        user_id=user.id,
        idempotency_key=str(uuid4()),
        event_type="ACCEPTED",
        ride_data={"price": 10.0},
        created_at=base_time,
    )
    new_ride = Ride(
        user_id=user.id,
        idempotency_key=str(uuid4()),
        event_type="ACCEPTED",
        ride_data={"price": 20.0},
        created_at=base_time + timedelta(days=60),
    )
    db_session.add_all([old_ride, new_ride])
    await db_session.flush()

    cutoff = base_time + timedelta(days=30)
    events, total = await get_user_ride_events(
        db_session, user.id, limit=10, offset=0, since=cutoff
    )

    assert total == 1
    assert len(events) == 1
    assert events[0].id == new_ride.id


async def test_get_user_ride_events_since_none_returns_all(db_session):
    """Without since parameter, all events should be returned."""
    user = await _create_user(db_session)
    base_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

    for i in range(3):
        ride = Ride(
            user_id=user.id,
            idempotency_key=str(uuid4()),
            event_type="ACCEPTED",
            ride_data={"price": 10.0 + i},
            created_at=base_time + timedelta(days=i * 30),
        )
        db_session.add(ride)
    await db_session.flush()

    events, total = await get_user_ride_events(db_session, user.id, limit=10, offset=0, since=None)

    assert total == 3
    assert len(events) == 3


async def test_get_user_ride_events_scoped_to_user(db_session):
    """Should only return rides for the specified user."""
    user_a = await _create_user(db_session, "a@example.com")
    user_b = await _create_user(db_session, "b@example.com")
    await _create_ride(db_session, user_a.id)
    await _create_ride(db_session, user_a.id)
    await _create_ride(db_session, user_b.id)

    _events_a, total_a = await get_user_ride_events(db_session, user_a.id, limit=10, offset=0)
    _events_b, total_b = await get_user_ride_events(db_session, user_b.id, limit=10, offset=0)

    assert total_a == 2
    assert total_b == 1


# ===========================================================================
# get_user_fcm_token
# ===========================================================================


async def test_get_user_fcm_token_returns_token(db_session):
    """Should return FCM token when set."""
    user = await _create_user(db_session, fcm_token="fcm-token-123")

    token = await get_user_fcm_token(db_session, user.id)

    assert token == "fcm-token-123"


async def test_get_user_fcm_token_returns_none_when_not_set(db_session):
    """Should return None when user has no FCM token."""
    user = await _create_user(db_session)

    token = await get_user_fcm_token(db_session, user.id)

    assert token is None
