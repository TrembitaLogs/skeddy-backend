import uuid

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError

from app.models.ride import Ride
from app.models.user import User

# ===========================================================================
# Existing tests (updated with ride_hash)
# ===========================================================================


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
        ride_hash="a" * 64,
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
        ride_hash="a" * 64,
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
        ride_hash="a" * 64,
    )
    db_session.add(ride1)
    await db_session.flush()

    ride2 = Ride(
        user_id=user.id,
        idempotency_key=key,
        event_type="ACCEPTED",
        ride_data={"price": 30.0},
        ride_hash="a" * 64,
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
        ride_hash="a" * 64,
    )
    db_session.add(ride)
    await db_session.flush()

    ride_id = ride.id

    await db_session.delete(user)
    await db_session.flush()

    result = await db_session.execute(select(Ride).where(Ride.id == ride_id))
    assert result.scalar_one_or_none() is None


# ===========================================================================
# New billing/verification field tests (task 1.4)
# ===========================================================================


async def test_ride_with_ride_hash_creates_successfully(db_session):
    """Creating a Ride with a valid ride_hash succeeds."""
    user = User(email="hash@example.com", password_hash="hashed")
    db_session.add(user)
    await db_session.flush()

    ride_hash = "c76966d3a4f8e1b2d5c9f0a3e7b4d6c8a1f3e5b7d9c2a4f6e8b0d3c5a7f9b1"
    ride = Ride(
        user_id=user.id,
        idempotency_key=str(uuid.uuid4()),
        event_type="ACCEPTED",
        ride_data={"price": 25.0},
        ride_hash=ride_hash,
    )
    db_session.add(ride)
    await db_session.flush()

    result = await db_session.execute(select(Ride).where(Ride.id == ride.id))
    fetched = result.scalar_one()
    assert fetched.ride_hash == ride_hash


async def test_ride_hash_not_unique_different_users(db_session):
    """Two Rides with the same ride_hash for different users succeed (NOT unique)."""
    user_a = User(email="hash-a@example.com", password_hash="hashed")
    user_b = User(email="hash-b@example.com", password_hash="hashed")
    db_session.add_all([user_a, user_b])
    await db_session.flush()

    same_hash = "a" * 64

    ride_a = Ride(
        user_id=user_a.id,
        idempotency_key=str(uuid.uuid4()),
        event_type="ACCEPTED",
        ride_data={"price": 20.0},
        ride_hash=same_hash,
    )
    ride_b = Ride(
        user_id=user_b.id,
        idempotency_key=str(uuid.uuid4()),
        event_type="ACCEPTED",
        ride_data={"price": 30.0},
        ride_hash=same_hash,
    )
    db_session.add_all([ride_a, ride_b])
    await db_session.flush()

    assert ride_a.id != ride_b.id
    assert ride_a.ride_hash == ride_b.ride_hash


async def test_ride_credits_charged_negative_raises_integrity_error(db_session):
    """CHECK constraint: credits_charged < 0 raises IntegrityError."""
    user = User(email="neg-charge@example.com", password_hash="hashed")
    db_session.add(user)
    await db_session.flush()

    ride = Ride(
        user_id=user.id,
        idempotency_key=str(uuid.uuid4()),
        event_type="ACCEPTED",
        ride_data={"price": 25.0},
        ride_hash="a" * 64,
        credits_charged=-1,
    )
    db_session.add(ride)

    with pytest.raises(IntegrityError):
        await db_session.flush()


async def test_ride_credits_refunded_negative_raises_integrity_error(db_session):
    """CHECK constraint: credits_refunded < 0 raises IntegrityError."""
    user = User(email="neg-refund@example.com", password_hash="hashed")
    db_session.add(user)
    await db_session.flush()

    ride = Ride(
        user_id=user.id,
        idempotency_key=str(uuid.uuid4()),
        event_type="ACCEPTED",
        ride_data={"price": 25.0},
        ride_hash="a" * 64,
        credits_refunded=-1,
    )
    db_session.add(ride)

    with pytest.raises(IntegrityError):
        await db_session.flush()


async def test_ride_verification_status_defaults_to_pending(db_session):
    """verification_status server_default is 'PENDING'."""
    user = User(email="vstatus@example.com", password_hash="hashed")
    db_session.add(user)
    await db_session.flush()

    ride = Ride(
        user_id=user.id,
        idempotency_key=str(uuid.uuid4()),
        event_type="ACCEPTED",
        ride_data={"price": 25.0},
        ride_hash="a" * 64,
    )
    db_session.add(ride)
    await db_session.flush()

    ride_id = ride.id

    # Refresh from DB to verify server_default was applied
    await db_session.refresh(ride)
    assert ride.verification_status == "PENDING"

    # Double-check via fresh query
    result = await db_session.execute(select(Ride).where(Ride.id == ride_id))
    fetched = result.scalar_one()
    assert fetched.verification_status == "PENDING"


async def test_ride_last_reported_present_boolean_values(db_session):
    """last_reported_present accepts true, false, and null."""
    user = User(email="lrp@example.com", password_hash="hashed")
    db_session.add(user)
    await db_session.flush()

    # True
    ride_true = Ride(
        user_id=user.id,
        idempotency_key=str(uuid.uuid4()),
        event_type="ACCEPTED",
        ride_data={"price": 10.0},
        ride_hash="a" * 64,
        last_reported_present=True,
    )
    # False
    ride_false = Ride(
        user_id=user.id,
        idempotency_key=str(uuid.uuid4()),
        event_type="ACCEPTED",
        ride_data={"price": 20.0},
        ride_hash="b" * 64,
        last_reported_present=False,
    )
    # None (default)
    ride_none = Ride(
        user_id=user.id,
        idempotency_key=str(uuid.uuid4()),
        event_type="ACCEPTED",
        ride_data={"price": 30.0},
        ride_hash="c" * 64,
    )
    db_session.add_all([ride_true, ride_false, ride_none])
    await db_session.flush()

    result = await db_session.execute(
        select(Ride).where(Ride.user_id == user.id).order_by(Ride.ride_hash)
    )
    rides = list(result.scalars().all())

    assert rides[0].last_reported_present is True
    assert rides[1].last_reported_present is False
    assert rides[2].last_reported_present is None


async def test_ride_verification_index_exists(db_session):
    """Partial index idx_rides_verification exists."""
    result = await db_session.execute(
        text(
            "SELECT indexname FROM pg_indexes "
            "WHERE tablename = 'rides' AND indexname = 'idx_rides_verification'"
        )
    )
    row = result.scalar_one_or_none()
    assert row == "idx_rides_verification"


async def test_ride_ride_hash_index_exists(db_session):
    """Index idx_rides_ride_hash exists."""
    result = await db_session.execute(
        text(
            "SELECT indexname FROM pg_indexes "
            "WHERE tablename = 'rides' AND indexname = 'idx_rides_ride_hash'"
        )
    )
    row = result.scalar_one_or_none()
    assert row == "idx_rides_ride_hash"
