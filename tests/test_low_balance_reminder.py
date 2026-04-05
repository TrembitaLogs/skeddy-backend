"""Tests for app.tasks.low_balance_reminder (task 8.3).

Test strategy:
1. User with low balance → FCM push sent
2. User with balance >= threshold → skipped
3. User with balance == 0 → skipped
4. Anti-spam: Redis key exists → skipped
5. After push, Redis key set with correct TTL
6. Mock FCM service for unit tests
"""

import asyncio
import uuid
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy.exc import OperationalError

from app.models.credit_balance import CreditBalance
from app.models.user import User
from app.tasks.low_balance_reminder import (
    LOW_BALANCE_NOTIFIED_TTL,
    get_low_balance_users,
    process_user,
    run_low_balance_reminder,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_user(email: str = "test@example.com", fcm_token: str | None = "fcm-token-123") -> User:
    return User(
        id=uuid.uuid4(),
        email=email,
        password_hash="fakehash",
        fcm_token=fcm_token,
    )


def _make_credit_balance(user: User, balance: int) -> CreditBalance:
    return CreditBalance(
        id=uuid.uuid4(),
        user_id=user.id,
        balance=balance,
    )


# ---------------------------------------------------------------------------
# get_low_balance_users — integration tests (real DB)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_low_balance_users_returns_low_balance(db_session):
    """User with 0 < balance < threshold should be returned."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    balance = _make_credit_balance(user, 2)
    db_session.add(balance)
    await db_session.flush()

    result = await get_low_balance_users(3, db_session)
    assert len(result) == 1
    assert result[0] == (user.id, 2)


@pytest.mark.asyncio
async def test_get_low_balance_users_skips_zero_balance(db_session):
    """User with balance == 0 should NOT be returned."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    balance = _make_credit_balance(user, 0)
    db_session.add(balance)
    await db_session.flush()

    result = await get_low_balance_users(3, db_session)
    assert len(result) == 0


@pytest.mark.asyncio
async def test_get_low_balance_users_skips_high_balance(db_session):
    """User with balance >= threshold should NOT be returned."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    balance = _make_credit_balance(user, 5)
    db_session.add(balance)
    await db_session.flush()

    result = await get_low_balance_users(3, db_session)
    assert len(result) == 0


@pytest.mark.asyncio
async def test_get_low_balance_users_skips_exact_threshold(db_session):
    """User with balance == threshold should NOT be returned (strict <)."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    balance = _make_credit_balance(user, 3)
    db_session.add(balance)
    await db_session.flush()

    result = await get_low_balance_users(3, db_session)
    assert len(result) == 0


@pytest.mark.asyncio
async def test_get_low_balance_users_multiple_users(db_session):
    """Only users with qualifying balance are returned among multiple users."""
    user_low = _make_user("low@example.com")
    user_zero = _make_user("zero@example.com")
    user_high = _make_user("high@example.com")
    db_session.add_all([user_low, user_zero, user_high])
    await db_session.flush()

    db_session.add_all(
        [
            _make_credit_balance(user_low, 1),
            _make_credit_balance(user_zero, 0),
            _make_credit_balance(user_high, 10),
        ]
    )
    await db_session.flush()

    result = await get_low_balance_users(3, db_session)
    assert len(result) == 1
    assert result[0][0] == user_low.id
    assert result[0][1] == 1


# ---------------------------------------------------------------------------
# process_user — unit tests (mocked Redis and FCM)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_process_user_sends_push_when_not_notified(fake_redis):
    """User not yet notified → FCM push sent, returns True."""
    user_id = uuid.uuid4()
    mock_db = AsyncMock()

    with patch(
        "app.tasks.low_balance_reminder.send_credits_low",
        new_callable=AsyncMock,
    ) as mock_send:
        result = await process_user(user_id, 2, 3, mock_db, fake_redis)

    assert result is True
    mock_send.assert_called_once_with(mock_db, user_id, 2, 3)


@pytest.mark.asyncio
async def test_process_user_sets_redis_key_after_push(fake_redis):
    """After sending push, Redis anti-spam key should be set."""
    user_id = uuid.uuid4()
    mock_db = AsyncMock()

    with patch(
        "app.tasks.low_balance_reminder.send_credits_low",
        new_callable=AsyncMock,
    ):
        await process_user(user_id, 2, 3, mock_db, fake_redis)

    expected_key = f"low_balance_notified:{user_id}"
    fake_redis.setex.assert_called_with(expected_key, LOW_BALANCE_NOTIFIED_TTL, "1")

    # Verify the key is actually in the store
    assert fake_redis._store[expected_key] == "1"


@pytest.mark.asyncio
async def test_process_user_skips_when_already_notified(fake_redis):
    """User already notified (Redis key exists) → skip, returns False."""
    user_id = uuid.uuid4()
    mock_db = AsyncMock()

    # Pre-set the anti-spam key
    expected_key = f"low_balance_notified:{user_id}"
    fake_redis._store[expected_key] = "1"

    with patch(
        "app.tasks.low_balance_reminder.send_credits_low",
        new_callable=AsyncMock,
    ) as mock_send:
        result = await process_user(user_id, 2, 3, mock_db, fake_redis)

    assert result is False
    mock_send.assert_not_called()


@pytest.mark.asyncio
async def test_process_user_skips_when_redis_unavailable_on_check():
    """Redis unavailable on GET → skip user to avoid spam, returns False."""
    user_id = uuid.uuid4()
    mock_db = AsyncMock()
    mock_redis = AsyncMock()

    from redis.exceptions import RedisError

    mock_redis.get = AsyncMock(side_effect=RedisError("Connection refused"))

    with patch(
        "app.tasks.low_balance_reminder.send_credits_low",
        new_callable=AsyncMock,
    ) as mock_send:
        result = await process_user(user_id, 2, 3, mock_db, mock_redis)

    assert result is False
    mock_send.assert_not_called()


@pytest.mark.asyncio
async def test_process_user_still_returns_true_when_redis_set_fails(fake_redis):
    """Redis SET fails after push → push already sent, returns True."""
    user_id = uuid.uuid4()
    mock_db = AsyncMock()

    from redis.exceptions import RedisError

    async def setex_fail(key, ttl, value):
        raise RedisError("Connection refused")

    fake_redis.setex = AsyncMock(side_effect=setex_fail)

    with patch(
        "app.tasks.low_balance_reminder.send_credits_low",
        new_callable=AsyncMock,
    ) as mock_send:
        result = await process_user(user_id, 2, 3, mock_db, fake_redis)

    assert result is True
    mock_send.assert_called_once()


# ---------------------------------------------------------------------------
# run_low_balance_reminder — unit tests (fully mocked)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_low_balance_reminder_sends_to_eligible_users():
    """Full loop: finds eligible users and sends FCM push."""
    user_id = uuid.uuid4()
    mock_db = AsyncMock()

    @asynccontextmanager
    async def mock_session_factory():
        yield mock_db

    call_count = 0

    with (
        patch(
            "app.tasks.low_balance_reminder.AsyncSessionLocal",
            side_effect=mock_session_factory,
        ),
        patch(
            "app.tasks.low_balance_reminder.get_max_ride_credits",
            new_callable=AsyncMock,
            return_value=3,
        ),
        patch(
            "app.tasks.low_balance_reminder.get_low_balance_users",
            new_callable=AsyncMock,
            return_value=[(user_id, 2)],
        ),
        patch(
            "app.tasks.low_balance_reminder.process_user",
            new_callable=AsyncMock,
            return_value=True,
        ) as mock_process,
        patch(
            "app.tasks.low_balance_reminder.asyncio.sleep",
            new_callable=AsyncMock,
        ) as mock_sleep,
    ):

        async def sleep_handler(seconds):
            nonlocal call_count
            call_count += 1
            # Let initial delay pass, then break loop
            if call_count >= 2:
                raise asyncio.CancelledError()

        mock_sleep.side_effect = sleep_handler

        with pytest.raises(asyncio.CancelledError):
            await run_low_balance_reminder()

        mock_process.assert_called_once()
        args = mock_process.call_args[0]
        assert args[0] == user_id
        assert args[1] == 2
        assert args[2] == 3
        # args[3] is db session, args[4] is redis_client


@pytest.mark.asyncio
async def test_run_low_balance_reminder_skips_when_no_users():
    """No eligible users → no process_user calls."""
    mock_db = AsyncMock()

    @asynccontextmanager
    async def mock_session_factory():
        yield mock_db

    call_count = 0

    with (
        patch(
            "app.tasks.low_balance_reminder.AsyncSessionLocal",
            side_effect=mock_session_factory,
        ),
        patch(
            "app.tasks.low_balance_reminder.get_max_ride_credits",
            new_callable=AsyncMock,
            return_value=3,
        ),
        patch(
            "app.tasks.low_balance_reminder.get_low_balance_users",
            new_callable=AsyncMock,
            return_value=[],
        ),
        patch(
            "app.tasks.low_balance_reminder.process_user",
            new_callable=AsyncMock,
        ) as mock_process,
        patch(
            "app.tasks.low_balance_reminder.asyncio.sleep",
            new_callable=AsyncMock,
        ) as mock_sleep,
    ):

        async def sleep_handler(seconds):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise asyncio.CancelledError()

        mock_sleep.side_effect = sleep_handler

        with pytest.raises(asyncio.CancelledError):
            await run_low_balance_reminder()

        mock_process.assert_not_called()


@pytest.mark.asyncio
async def test_run_low_balance_reminder_handles_db_error():
    """DB error during main loop → logged, task continues."""
    call_count = 0

    @asynccontextmanager
    async def mock_session_factory():
        raise OperationalError("DB connection failed", {}, None)
        yield  # pragma: no cover

    with (
        patch(
            "app.tasks.low_balance_reminder.AsyncSessionLocal",
            side_effect=mock_session_factory,
        ),
        patch(
            "app.tasks.low_balance_reminder.asyncio.sleep",
            new_callable=AsyncMock,
        ) as mock_sleep,
    ):

        async def sleep_handler(seconds):
            nonlocal call_count
            call_count += 1
            if call_count >= 3:
                raise asyncio.CancelledError()

        mock_sleep.side_effect = sleep_handler

        with pytest.raises(asyncio.CancelledError):
            await run_low_balance_reminder()

        # Should have survived the error and slept multiple times
        assert call_count == 3


@pytest.mark.asyncio
async def test_run_low_balance_reminder_per_user_error_doesnt_stop_others():
    """Error processing one user doesn't prevent processing of next user."""
    user_a = uuid.uuid4()
    user_b = uuid.uuid4()
    mock_db = AsyncMock()

    @asynccontextmanager
    async def mock_session_factory():
        yield mock_db

    call_count = 0
    process_calls = []

    with (
        patch(
            "app.tasks.low_balance_reminder.AsyncSessionLocal",
            side_effect=mock_session_factory,
        ),
        patch(
            "app.tasks.low_balance_reminder.get_max_ride_credits",
            new_callable=AsyncMock,
            return_value=3,
        ),
        patch(
            "app.tasks.low_balance_reminder.get_low_balance_users",
            new_callable=AsyncMock,
            return_value=[(user_a, 1), (user_b, 2)],
        ),
        patch(
            "app.tasks.low_balance_reminder.process_user",
            new_callable=AsyncMock,
        ) as mock_process,
        patch(
            "app.tasks.low_balance_reminder.asyncio.sleep",
            new_callable=AsyncMock,
        ) as mock_sleep,
    ):
        call_order = 0

        async def process_side_effect(uid, balance, threshold, db, redis):
            nonlocal call_order
            call_order += 1
            process_calls.append(uid)
            if uid == user_a:
                raise OperationalError("FCM failed for user A", {}, None)
            return True

        mock_process.side_effect = process_side_effect

        async def sleep_handler(seconds):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise asyncio.CancelledError()

        mock_sleep.side_effect = sleep_handler

        with pytest.raises(asyncio.CancelledError):
            await run_low_balance_reminder()

        # Both users should have been processed despite error on first
        assert user_a in process_calls
        assert user_b in process_calls
