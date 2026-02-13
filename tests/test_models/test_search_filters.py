import uuid

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from app.models.search_filters import SearchFilters
from app.models.user import User


def _make_user(email: str = "test@example.com") -> User:
    return User(email=email, password_hash="hashed")


def _make_filters(user_id: uuid.UUID, **overrides) -> SearchFilters:
    defaults = {"user_id": user_id}
    defaults.update(overrides)
    return SearchFilters(**defaults)


# --- Test strategy item 1: ARRAY field stores and returns list correctly ---


async def test_working_days_array_stores_and_returns_list(db_session):
    """ARRAY column round-trips a Python list of day strings."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    days = ["MON", "WED", "FRI"]
    filters = _make_filters(user.id, working_days=days)
    db_session.add(filters)
    await db_session.flush()

    result = await db_session.execute(
        select(SearchFilters).where(SearchFilters.user_id == user.id)
    )
    loaded = result.scalar_one()
    assert loaded.working_days == ["MON", "WED", "FRI"]


async def test_working_days_array_empty_list(db_session):
    """ARRAY column can store an empty list."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    filters = _make_filters(user.id, working_days=[])
    db_session.add(filters)
    await db_session.flush()

    result = await db_session.execute(
        select(SearchFilters).where(SearchFilters.user_id == user.id)
    )
    loaded = result.scalar_one()
    assert loaded.working_days == []


# --- Test strategy item 2: Default working_days contains all 7 days ---


async def test_default_working_days_contains_all_seven_days(db_session):
    """When no working_days provided, default is all 7 days."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    filters = _make_filters(user.id)
    db_session.add(filters)
    await db_session.flush()

    assert filters.working_days == ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]


async def test_default_values_for_all_fields(db_session):
    """All fields receive correct defaults when only user_id is provided."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    filters = _make_filters(user.id)
    db_session.add(filters)
    await db_session.flush()

    assert filters.min_price == 20.0
    assert filters.start_time == "06:30"
    assert filters.working_time == 24
    assert len(filters.working_days) == 7


# --- Test strategy item 3: UNIQUE on user_id works ---


async def test_unique_constraint_on_user_id(db_session):
    """Second SearchFilters for the same user_id raises IntegrityError."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    filters1 = _make_filters(user.id)
    db_session.add(filters1)
    await db_session.flush()

    filters2 = _make_filters(user.id)
    db_session.add(filters2)
    with pytest.raises(IntegrityError):
        await db_session.flush()


# --- Additional: start_time validator ---


def test_start_time_validator_rejects_invalid_format():
    """Validator raises ValueError for non-HH:MM values."""
    with pytest.raises(ValueError, match="HH:MM 24h format"):
        SearchFilters(user_id=uuid.uuid4(), start_time="25:00")


def test_start_time_validator_rejects_invalid_minutes():
    """Validator raises ValueError for minutes >= 60."""
    with pytest.raises(ValueError, match="HH:MM 24h format"):
        SearchFilters(user_id=uuid.uuid4(), start_time="12:60")


def test_start_time_validator_accepts_valid_times():
    """Validator accepts properly formatted HH:MM strings."""
    f1 = SearchFilters(user_id=uuid.uuid4(), start_time="00:00")
    assert f1.start_time == "00:00"

    f2 = SearchFilters(user_id=uuid.uuid4(), start_time="23:59")
    assert f2.start_time == "23:59"

    f3 = SearchFilters(user_id=uuid.uuid4(), start_time="06:30")
    assert f3.start_time == "06:30"


# --- Additional: cascade delete ---


async def test_cascade_delete_on_user_removal(db_session):
    """Deleting a User automatically deletes associated SearchFilters."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    filters = _make_filters(user.id)
    db_session.add(filters)
    await db_session.flush()

    filters_id = filters.id

    await db_session.delete(user)
    await db_session.flush()

    result = await db_session.execute(select(SearchFilters).where(SearchFilters.id == filters_id))
    assert result.scalar_one_or_none() is None
