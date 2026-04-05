"""Unit tests for search_service.py error paths and edge cases.

Covers branches not exercised by the integration-level router tests:
- get_search_status() fallback when no DB row exists
- get_search_status_with_device() when no SearchStatus row (LEFT JOIN miss)
- set_search_active() creating a new row when none exists
- set_search_active() DB commit failure propagation
- set_search_active() DB query failure propagation
"""

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError as SAIntegrityError
from sqlalchemy.exc import OperationalError

from app.models.paired_device import PairedDevice
from app.models.search_status import SearchStatus
from app.models.user import User
from app.services.search_service import (
    get_search_status,
    get_search_status_with_device,
    set_search_active,
)


def _make_user(email: str = "svc@example.com") -> User:
    return User(email=email, password_hash="hashed")


# ===== get_search_status =====


async def test_get_search_status_returns_existing_row(db_session):
    """get_search_status returns the persisted SearchStatus when it exists."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    status = SearchStatus(user_id=user.id, is_active=True)
    db_session.add(status)
    await db_session.flush()

    result = await get_search_status(db_session, user.id)

    assert result.user_id == user.id
    assert result.is_active is True
    assert result.id == status.id


async def test_get_search_status_returns_transient_when_no_row(db_session):
    """get_search_status returns a transient SearchStatus with defaults when no row exists."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    # Do NOT create a SearchStatus row — exercise the fallback branch
    result = await get_search_status(db_session, user.id)

    assert result.user_id == user.id
    assert result.is_active is False


async def test_get_search_status_transient_not_persisted(db_session):
    """The transient fallback object is not saved to the database."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    _ = await get_search_status(db_session, user.id)

    # Verify nothing was written
    rows = await db_session.execute(select(SearchStatus).where(SearchStatus.user_id == user.id))
    assert rows.scalar_one_or_none() is None


# ===== get_search_status_with_device =====


async def test_get_search_status_with_device_both_exist(db_session):
    """Returns (SearchStatus, PairedDevice) when both rows exist."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    status = SearchStatus(user_id=user.id, is_active=True)
    device = PairedDevice(
        user_id=user.id,
        device_id="dev-001",
        device_token_hash="fake-hash",
        timezone="UTC",
    )
    db_session.add_all([status, device])
    await db_session.flush()

    result_status, result_device = await get_search_status_with_device(db_session, user.id)

    assert result_status.id == status.id
    assert result_status.is_active is True
    assert result_device is not None
    assert result_device.device_id == "dev-001"


async def test_get_search_status_with_device_status_exists_no_device(db_session):
    """Returns (SearchStatus, None) when status exists but no paired device."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    status = SearchStatus(user_id=user.id, is_active=False)
    db_session.add(status)
    await db_session.flush()

    result_status, result_device = await get_search_status_with_device(db_session, user.id)

    assert result_status.id == status.id
    assert result_device is None


async def test_get_search_status_with_device_no_status_no_device(db_session):
    """Returns (transient SearchStatus, None) when neither row exists."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    result_status, result_device = await get_search_status_with_device(db_session, user.id)

    assert result_status.user_id == user.id
    assert result_status.is_active is False
    assert result_device is None


async def test_get_search_status_with_device_no_status_but_device_exists(db_session):
    """Returns (transient SearchStatus, PairedDevice) when device exists but no SearchStatus."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    device = PairedDevice(
        user_id=user.id,
        device_id="dev-orphan",
        device_token_hash="fake-hash",
        timezone="UTC",
    )
    db_session.add(device)
    await db_session.flush()

    result_status, result_device = await get_search_status_with_device(db_session, user.id)

    # No SearchStatus row -> transient fallback
    assert result_status.user_id == user.id
    assert result_status.is_active is False
    # Device should still be found via the separate query
    assert result_device is not None
    assert result_device.device_id == "dev-orphan"


# ===== set_search_active =====


async def test_set_search_active_creates_row_when_none_exists(db_session):
    """set_search_active creates a new SearchStatus when the user has none."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    # No SearchStatus exists — this exercises the create branch
    await set_search_active(db_session, user.id, active=True)

    result = await db_session.execute(select(SearchStatus).where(SearchStatus.user_id == user.id))
    status = result.scalar_one()
    assert status.is_active is True


async def test_set_search_active_creates_row_inactive(db_session):
    """set_search_active(active=False) creates a row with is_active=False."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    await set_search_active(db_session, user.id, active=False)

    result = await db_session.execute(select(SearchStatus).where(SearchStatus.user_id == user.id))
    status = result.scalar_one()
    assert status.is_active is False


async def test_set_search_active_updates_existing_row(db_session):
    """set_search_active flips is_active on an existing row."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    status = SearchStatus(user_id=user.id, is_active=False)
    db_session.add(status)
    await db_session.commit()

    await set_search_active(db_session, user.id, active=True)

    result = await db_session.execute(select(SearchStatus).where(SearchStatus.user_id == user.id))
    updated = result.scalar_one()
    assert updated.is_active is True
    assert updated.id == status.id  # same row, not a new one


async def test_set_search_active_toggle_back(db_session):
    """set_search_active can toggle active -> inactive on existing row."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    status = SearchStatus(user_id=user.id, is_active=True)
    db_session.add(status)
    await db_session.commit()

    await set_search_active(db_session, user.id, active=False)

    result = await db_session.execute(select(SearchStatus).where(SearchStatus.user_id == user.id))
    updated = result.scalar_one()
    assert updated.is_active is False


# --- Error propagation tests ---


async def test_set_search_active_commit_failure_propagates():
    """DB commit failure in set_search_active propagates as-is."""
    mock_db = AsyncMock()

    # Simulate execute returning no existing row
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    mock_db.execute = AsyncMock(return_value=mock_result)

    # Simulate commit failure
    mock_db.commit = AsyncMock(side_effect=OperationalError("connection lost", {}, None))

    user_id = uuid.uuid4()
    with pytest.raises(OperationalError):
        await set_search_active(mock_db, user_id, active=True)

    mock_db.add.assert_called_once()
    mock_db.commit.assert_awaited_once()


async def test_set_search_active_commit_integrity_error_propagates():
    """IntegrityError during commit (e.g. race condition) propagates."""
    mock_db = AsyncMock()

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    mock_db.execute = AsyncMock(return_value=mock_result)

    mock_db.commit = AsyncMock(side_effect=SAIntegrityError("duplicate key", {}, None))

    user_id = uuid.uuid4()
    with pytest.raises(SAIntegrityError):
        await set_search_active(mock_db, user_id, active=True)


async def test_set_search_active_query_failure_propagates():
    """DB query failure in set_search_active propagates."""
    mock_db = AsyncMock()
    mock_db.execute = AsyncMock(side_effect=OperationalError("connection refused", {}, None))

    user_id = uuid.uuid4()
    with pytest.raises(OperationalError):
        await set_search_active(mock_db, user_id, active=True)

    # commit should never be reached
    mock_db.commit.assert_not_awaited()


async def test_set_search_active_updates_via_mock():
    """When an existing row is found, set_search_active updates it (mock path)."""
    mock_db = AsyncMock()

    existing_status = MagicMock(spec=SearchStatus)
    existing_status.is_active = False

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = existing_status
    mock_db.execute = AsyncMock(return_value=mock_result)

    user_id = uuid.uuid4()
    await set_search_active(mock_db, user_id, active=True)

    assert existing_status.is_active is True
    mock_db.add.assert_not_called()  # should NOT add — row already exists
    mock_db.commit.assert_awaited_once()


# --- get_search_status error propagation ---


async def test_get_search_status_query_failure_propagates():
    """DB query failure in get_search_status propagates."""
    mock_db = AsyncMock()
    mock_db.execute = AsyncMock(side_effect=OperationalError("timeout", {}, None))

    with pytest.raises(OperationalError):
        await get_search_status(mock_db, uuid.uuid4())


# --- get_search_status_with_device error propagation ---


async def test_get_search_status_with_device_query_failure_propagates():
    """DB query failure in get_search_status_with_device propagates."""
    mock_db = AsyncMock()
    mock_db.execute = AsyncMock(side_effect=OperationalError("timeout", {}, None))

    with pytest.raises(OperationalError):
        await get_search_status_with_device(mock_db, uuid.uuid4())


async def test_get_search_status_with_device_fallback_query_failure():
    """When first query returns None, failure on the device fallback query propagates."""
    mock_db = AsyncMock()

    # First call (joined query) returns None
    first_result = MagicMock()
    first_result.one_or_none.return_value = None

    # Second call (device-only query) fails
    call_count = {"n": 0}

    async def side_effect(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return first_result
        raise OperationalError("connection lost", {}, None)

    mock_db.execute = AsyncMock(side_effect=side_effect)

    with pytest.raises(OperationalError):
        await get_search_status_with_device(mock_db, uuid.uuid4())
