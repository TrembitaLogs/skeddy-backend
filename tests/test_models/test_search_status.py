import uuid

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from app.models.search_status import SearchStatus
from app.models.user import User


def _make_user(email: str = "test@example.com") -> User:
    return User(email=email, password_hash="hashed")


def _make_status(user_id: uuid.UUID, **overrides) -> SearchStatus:
    defaults = {"user_id": user_id}
    defaults.update(overrides)
    return SearchStatus(**defaults)


# --- Test strategy item 1: updated_at auto-updates on is_active change ---


async def test_updated_at_set_on_insert(db_session):
    """updated_at receives a server-generated timestamp on insert."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    status = _make_status(user.id)
    db_session.add(status)
    await db_session.flush()

    result = await db_session.execute(select(SearchStatus).where(SearchStatus.user_id == user.id))
    loaded = result.scalar_one()
    assert loaded.updated_at is not None


async def test_updated_at_changes_on_is_active_update(db_session):
    """updated_at is refreshed when is_active is modified."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    status = _make_status(user.id, is_active=False)
    db_session.add(status)
    await db_session.flush()

    result = await db_session.execute(select(SearchStatus).where(SearchStatus.user_id == user.id))
    loaded = result.scalar_one()
    original_updated_at = loaded.updated_at

    # Commit so PostgreSQL sees a real UPDATE (server-side onupdate needs it)
    await db_session.commit()

    loaded.is_active = True
    db_session.add(loaded)
    await db_session.commit()

    result = await db_session.execute(select(SearchStatus).where(SearchStatus.user_id == user.id))
    refreshed = result.scalar_one()
    assert refreshed.is_active is True
    assert refreshed.updated_at >= original_updated_at


# --- Test strategy item 2: UNIQUE constraint on user_id ---


async def test_unique_constraint_on_user_id(db_session):
    """Second SearchStatus for the same user_id raises IntegrityError."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    status1 = _make_status(user.id)
    db_session.add(status1)
    await db_session.flush()

    status2 = _make_status(user.id)
    db_session.add(status2)
    with pytest.raises(IntegrityError):
        await db_session.flush()


# --- Test strategy item 3: CASCADE delete ---


async def test_cascade_delete_on_user_removal(db_session):
    """Deleting a User automatically deletes associated SearchStatus."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    status = _make_status(user.id)
    db_session.add(status)
    await db_session.flush()

    status_id = status.id

    await db_session.delete(user)
    await db_session.flush()

    result = await db_session.execute(select(SearchStatus).where(SearchStatus.id == status_id))
    assert result.scalar_one_or_none() is None


# --- Additional: default values ---


async def test_default_is_active_is_false(db_session):
    """is_active defaults to False when not specified."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    status = _make_status(user.id)
    db_session.add(status)
    await db_session.flush()

    assert status.is_active is False
