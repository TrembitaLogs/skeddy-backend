import uuid

from sqlalchemy import select, text

from app.models.accept_failure import AcceptFailure
from app.models.user import User


async def test_accept_failure_pk_is_uuid(db_session):
    """AcceptFailure primary key is a UUID."""
    user = User(email="fail-uuid@example.com", password_hash="hashed")
    db_session.add(user)
    await db_session.flush()

    failure = AcceptFailure(
        user_id=user.id,
        reason="AcceptButtonNotFound",
    )
    db_session.add(failure)
    await db_session.flush()

    assert isinstance(failure.id, uuid.UUID)


async def test_accept_failure_fields_match_prd_schema(db_session):
    """AcceptFailure fields reason, ride_price, pickup_time match PRD schema."""
    user = User(email="fail-fields@example.com", password_hash="hashed")
    db_session.add(user)
    await db_session.flush()

    failure = AcceptFailure(
        user_id=user.id,
        reason="AcceptButtonNotFound",
        ride_price=45.99,
        pickup_time="2026-02-10T09:00:00",
    )
    db_session.add(failure)
    await db_session.flush()

    result = await db_session.execute(select(AcceptFailure).where(AcceptFailure.id == failure.id))
    fetched = result.scalar_one()

    assert fetched.reason == "AcceptButtonNotFound"
    assert fetched.ride_price == 45.99
    assert fetched.pickup_time == "2026-02-10T09:00:00"
    assert fetched.reported_at is not None


async def test_accept_failure_nullable_fields(db_session):
    """ride_price and pickup_time are nullable as per PRD."""
    user = User(email="fail-null@example.com", password_hash="hashed")
    db_session.add(user)
    await db_session.flush()

    failure = AcceptFailure(
        user_id=user.id,
        reason="Timeout",
    )
    db_session.add(failure)
    await db_session.flush()

    result = await db_session.execute(select(AcceptFailure).where(AcceptFailure.id == failure.id))
    fetched = result.scalar_one()

    assert fetched.ride_price is None
    assert fetched.pickup_time is None


async def test_accept_failure_user_index_exists(db_session):
    """Index idx_accept_failures_user exists on user_id column."""
    result = await db_session.execute(
        text(
            "SELECT indexname FROM pg_indexes "
            "WHERE tablename = 'accept_failures' "
            "AND indexname = 'idx_accept_failures_user'"
        )
    )
    row = result.scalar_one_or_none()
    assert row == "idx_accept_failures_user"


async def test_accept_failure_cascade_delete_on_user_removal(db_session):
    """Deleting a User automatically deletes all associated accept failures (CASCADE)."""
    user = User(email="cascade-fail@example.com", password_hash="hashed")
    db_session.add(user)
    await db_session.flush()

    failure = AcceptFailure(
        user_id=user.id,
        reason="AcceptButtonNotFound",
        ride_price=20.0,
    )
    db_session.add(failure)
    await db_session.flush()

    failure_id = failure.id

    await db_session.delete(user)
    await db_session.flush()

    result = await db_session.execute(select(AcceptFailure).where(AcceptFailure.id == failure_id))
    assert result.scalar_one_or_none() is None
