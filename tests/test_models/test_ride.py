import uuid

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError

from app.models.ride import Ride
from app.models.user import User


async def test_ride_pk_is_uuid(db_session):
    """Ride primary key is a UUID."""
    user = User(email="rider@example.com", password_hash="hashed")
    db_session.add(user)
    await db_session.flush()

    ride = Ride(
        user_id=user.id,
        idempotency_key=str(uuid.uuid4()),
        event_type="ACCEPTED",
        ride_data={"price": 25.0},
    )
    db_session.add(ride)
    await db_session.flush()

    assert isinstance(ride.id, uuid.UUID)


async def test_ride_data_jsonb_serializes_dict(db_session):
    """JSONB field ride_data correctly serializes and deserializes a dict."""
    user = User(email="jsonb@example.com", password_hash="hashed")
    db_session.add(user)
    await db_session.flush()

    ride_data = {
        "price": 35.50,
        "pickup_time": "2026-02-10T08:30:00",
        "pickup_location": "123 Main St",
        "dropoff_location": "456 Oak Ave",
        "duration": "25 min",
        "distance": "12.5 mi",
        "rider_name": "John",
    }

    ride = Ride(
        user_id=user.id,
        idempotency_key=str(uuid.uuid4()),
        event_type="ACCEPTED",
        ride_data=ride_data,
    )
    db_session.add(ride)
    await db_session.flush()

    result = await db_session.execute(select(Ride).where(Ride.id == ride.id))
    fetched = result.scalar_one()

    assert fetched.ride_data == ride_data
    assert isinstance(fetched.ride_data, dict)
    assert fetched.ride_data["price"] == 35.50


async def test_ride_idempotency_unique_index_exists(db_session):
    """Unique index idx_rides_idempotency exists on (user_id, idempotency_key)."""
    result = await db_session.execute(
        text(
            "SELECT indexname FROM pg_indexes "
            "WHERE tablename = 'rides' AND indexname = 'idx_rides_idempotency'"
        )
    )
    row = result.scalar_one_or_none()
    assert row == "idx_rides_idempotency"


async def test_ride_idempotency_unique_constraint_enforced(db_session):
    """Duplicate (user_id, idempotency_key) raises IntegrityError."""
    user = User(email="dedup@example.com", password_hash="hashed")
    db_session.add(user)
    await db_session.flush()

    key = str(uuid.uuid4())

    ride1 = Ride(
        user_id=user.id,
        idempotency_key=key,
        event_type="ACCEPTED",
        ride_data={"price": 20.0},
    )
    db_session.add(ride1)
    await db_session.flush()

    ride2 = Ride(
        user_id=user.id,
        idempotency_key=key,
        event_type="ACCEPTED",
        ride_data={"price": 30.0},
    )
    db_session.add(ride2)

    with pytest.raises(IntegrityError):
        await db_session.flush()


async def test_ride_user_created_index_exists(db_session):
    """Index idx_rides_user_created exists on (user_id, created_at DESC)."""
    result = await db_session.execute(
        text(
            "SELECT indexname FROM pg_indexes "
            "WHERE tablename = 'rides' AND indexname = 'idx_rides_user_created'"
        )
    )
    row = result.scalar_one_or_none()
    assert row == "idx_rides_user_created"


async def test_ride_cascade_delete_on_user_removal(db_session):
    """Deleting a User automatically deletes all associated rides (CASCADE)."""
    user = User(email="cascade-ride@example.com", password_hash="hashed")
    db_session.add(user)
    await db_session.flush()

    ride = Ride(
        user_id=user.id,
        idempotency_key=str(uuid.uuid4()),
        event_type="ACCEPTED",
        ride_data={"price": 20.0},
    )
    db_session.add(ride)
    await db_session.flush()

    ride_id = ride.id

    await db_session.delete(user)
    await db_session.flush()

    result = await db_session.execute(select(Ride).where(Ride.id == ride_id))
    assert result.scalar_one_or_none() is None
