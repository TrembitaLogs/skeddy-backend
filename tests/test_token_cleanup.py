import asyncio
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy.exc import OperationalError

from app.models.refresh_token import RefreshToken
from app.models.user import User
from app.tasks.token_cleanup import (
    cleanup_expired_tokens,
    delete_expired_refresh_tokens,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_user(email: str = "cleanup@example.com") -> User:
    return User(
        id=uuid.uuid4(),
        email=email,
        password_hash="fakehash",
    )


def _make_refresh_token(
    user: User, *, expired: bool = True, hours_offset: int = 1
) -> RefreshToken:
    """Create a RefreshToken that is either expired or still valid.

    Args:
        user: Owner of the token.
        expired: If True, expires_at is in the past; otherwise in the future.
        hours_offset: How many hours in the past/future for expires_at.
    """
    now = datetime.now(UTC)
    if expired:
        expires_at = now - timedelta(hours=hours_offset)
    else:
        expires_at = now + timedelta(hours=hours_offset)
    return RefreshToken(
        id=uuid.uuid4(),
        user_id=user.id,
        token_hash=uuid.uuid4().hex + uuid.uuid4().hex[:32],  # 64 hex chars
        expires_at=expires_at,
    )


# ---------------------------------------------------------------------------
# delete_expired_refresh_tokens — integration tests (real DB)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_deletes_expired_tokens(db_session):
    """Expired tokens should be deleted from the database."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    expired_token = _make_refresh_token(user, expired=True)
    db_session.add(expired_token)
    await db_session.flush()

    deleted = await delete_expired_refresh_tokens(db_session)

    assert deleted == 1

    # Verify token is gone
    from sqlalchemy import select

    result = await db_session.execute(
        select(RefreshToken).where(RefreshToken.id == expired_token.id)
    )
    assert result.scalar_one_or_none() is None


@pytest.mark.asyncio
async def test_valid_tokens_remain(db_session):
    """Tokens that have not expired should NOT be deleted."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    valid_token = _make_refresh_token(user, expired=False, hours_offset=24)
    db_session.add(valid_token)
    await db_session.flush()

    deleted = await delete_expired_refresh_tokens(db_session)

    assert deleted == 0

    # Verify token still exists
    from sqlalchemy import select

    result = await db_session.execute(
        select(RefreshToken).where(RefreshToken.id == valid_token.id)
    )
    assert result.scalar_one_or_none() is not None


@pytest.mark.asyncio
async def test_mixed_expired_and_valid(db_session):
    """Only expired tokens are deleted; valid ones remain."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    expired1 = _make_refresh_token(user, expired=True, hours_offset=2)
    expired2 = _make_refresh_token(user, expired=True, hours_offset=48)
    valid1 = _make_refresh_token(user, expired=False, hours_offset=1)
    valid2 = _make_refresh_token(user, expired=False, hours_offset=720)
    db_session.add_all([expired1, expired2, valid1, valid2])
    await db_session.flush()

    deleted = await delete_expired_refresh_tokens(db_session)

    assert deleted == 2

    from sqlalchemy import select

    result = await db_session.execute(select(RefreshToken))
    remaining = result.scalars().all()
    remaining_ids = {t.id for t in remaining}
    assert valid1.id in remaining_ids
    assert valid2.id in remaining_ids
    assert expired1.id not in remaining_ids
    assert expired2.id not in remaining_ids


@pytest.mark.asyncio
async def test_no_tokens_at_all(db_session):
    """Empty table — should return 0 deleted."""
    deleted = await delete_expired_refresh_tokens(db_session)
    assert deleted == 0


# ---------------------------------------------------------------------------
# cleanup_expired_tokens — unit tests (mocked DB + sleep)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cleanup_loop_calls_delete():
    """The loop should call delete_expired_refresh_tokens on each iteration."""
    mock_db = AsyncMock()

    @asynccontextmanager
    async def mock_session_factory():
        yield mock_db

    call_count = 0

    with (
        patch(
            "app.tasks.token_cleanup.AsyncSessionLocal",
            side_effect=mock_session_factory,
        ),
        patch(
            "app.tasks.token_cleanup.delete_expired_refresh_tokens",
            new_callable=AsyncMock,
            return_value=5,
        ) as mock_delete,
        patch(
            "app.tasks.token_cleanup.asyncio.sleep",
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
            await cleanup_expired_tokens()

        mock_delete.assert_called_once_with(mock_db)


@pytest.mark.asyncio
async def test_cleanup_continues_after_db_error():
    """The loop should catch exceptions and continue (not crash)."""
    call_count = 0

    @asynccontextmanager
    async def mock_session_factory():
        raise OperationalError("SELECT 1", {}, Exception("DB connection failed"))
        yield  # pragma: no cover

    with (
        patch(
            "app.tasks.token_cleanup.AsyncSessionLocal",
            side_effect=mock_session_factory,
        ),
        patch(
            "app.tasks.token_cleanup.asyncio.sleep",
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
            await cleanup_expired_tokens()

        # Should have survived: initial sleep(10) + sleep after error #1 + sleep after error #2
        assert call_count == 3


@pytest.mark.asyncio
async def test_cleanup_uses_correct_interval():
    """The sleep interval should be 86400 seconds (24 hours)."""
    mock_db = AsyncMock()

    @asynccontextmanager
    async def mock_session_factory():
        yield mock_db

    sleep_calls = []

    with (
        patch(
            "app.tasks.token_cleanup.AsyncSessionLocal",
            side_effect=mock_session_factory,
        ),
        patch(
            "app.tasks.token_cleanup.delete_expired_refresh_tokens",
            new_callable=AsyncMock,
            return_value=0,
        ),
        patch(
            "app.tasks.token_cleanup.asyncio.sleep",
            new_callable=AsyncMock,
        ) as mock_sleep,
    ):

        async def sleep_side_effect(seconds):
            sleep_calls.append(seconds)
            if len(sleep_calls) >= 2:
                raise asyncio.CancelledError()

        mock_sleep.side_effect = sleep_side_effect

        with pytest.raises(asyncio.CancelledError):
            await cleanup_expired_tokens()

        # First call: initial delay (10s), second call: cleanup interval (86400s)
        assert sleep_calls[0] == 10
        assert sleep_calls[1] == 86400
