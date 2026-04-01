import asyncio
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy.exc import OperationalError

from app.models.paired_device import PairedDevice
from app.models.search_filters import SearchFilters
from app.models.search_status import SearchStatus
from app.models.user import User
from app.schemas.fcm import NotificationType
from app.tasks.health_check import (
    check_device_health,
    get_active_paired_devices,
    should_notify_device_offline,
    should_reset_offline_notified,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_user(email: str = "test@example.com") -> User:
    return User(
        id=uuid.uuid4(),
        email=email,
        password_hash="fakehash",
    )


def _make_paired_device(user: User, device_id: str = "device-1") -> PairedDevice:
    return PairedDevice(
        id=uuid.uuid4(),
        user_id=user.id,
        device_id=device_id,
        device_token_hash="a" * 64,
        timezone="America/New_York",
        last_ping_at=datetime.now(UTC),
    )


def _make_search_status(user: User, is_active: bool = True) -> SearchStatus:
    return SearchStatus(
        id=uuid.uuid4(),
        user_id=user.id,
        is_active=is_active,
    )


def _make_filters(user: User | None = None) -> SearchFilters:
    return SearchFilters(
        id=uuid.uuid4(),
        user_id=user.id if user else uuid.uuid4(),
    )


# ---------------------------------------------------------------------------
# get_active_paired_devices — integration tests (real DB)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_active_paired_devices_returns_active(db_session):
    """Devices with is_active=True should be returned."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    device = _make_paired_device(user)
    status = _make_search_status(user, is_active=True)
    db_session.add_all([device, status])
    await db_session.flush()

    devices = await get_active_paired_devices(db_session)
    assert len(devices) == 1
    assert devices[0].device_id == "device-1"


@pytest.mark.asyncio
async def test_get_active_paired_devices_excludes_inactive(db_session):
    """Devices with is_active=False should NOT be returned."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    device = _make_paired_device(user)
    status = _make_search_status(user, is_active=False)
    db_session.add_all([device, status])
    await db_session.flush()

    devices = await get_active_paired_devices(db_session)
    assert len(devices) == 0


@pytest.mark.asyncio
async def test_get_active_paired_devices_empty(db_session):
    """No devices paired at all — should return empty list."""
    devices = await get_active_paired_devices(db_session)
    assert devices == []


@pytest.mark.asyncio
async def test_get_active_paired_devices_multiple_users(db_session):
    """Only devices with active search status are returned among multiple users."""
    user_active = _make_user("active@example.com")
    user_inactive = _make_user("inactive@example.com")
    db_session.add_all([user_active, user_inactive])
    await db_session.flush()

    device_active = _make_paired_device(user_active, device_id="dev-active")
    status_active = _make_search_status(user_active, is_active=True)
    device_inactive = _make_paired_device(user_inactive, device_id="dev-inactive")
    status_inactive = _make_search_status(user_inactive, is_active=False)
    db_session.add_all([device_active, status_active, device_inactive, status_inactive])
    await db_session.flush()

    devices = await get_active_paired_devices(db_session)
    assert len(devices) == 1
    assert devices[0].device_id == "dev-active"


@pytest.mark.asyncio
async def test_get_active_paired_devices_no_search_status(db_session):
    """Device paired but no SearchStatus record — should NOT be returned."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    device = _make_paired_device(user)
    db_session.add(device)
    await db_session.flush()

    devices = await get_active_paired_devices(db_session)
    assert len(devices) == 0


# ---------------------------------------------------------------------------
# check_device_health — unit tests (mocked DB + sleep)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_device_health_calls_get_active_devices():
    """The loop should call get_active_paired_devices on each iteration."""
    call_count = 0

    mock_db = AsyncMock()

    @asynccontextmanager
    async def mock_session_factory():
        yield mock_db

    with (
        patch(
            "app.tasks.health_check.AsyncSessionLocal",
            side_effect=mock_session_factory,
        ),
        patch(
            "app.tasks.health_check.get_active_paired_devices",
            new_callable=AsyncMock,
            return_value=[],
        ) as mock_get_devices,
        patch("app.tasks.health_check.asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
    ):
        # Make sleep raise after first call to break the infinite loop
        async def sleep_once(seconds):
            nonlocal call_count
            call_count += 1
            if call_count >= 1:
                raise asyncio.CancelledError()

        mock_sleep.side_effect = sleep_once

        with pytest.raises(asyncio.CancelledError):
            await check_device_health()

        mock_get_devices.assert_called_once_with(mock_db)


@pytest.mark.asyncio
async def test_check_device_health_handles_db_error():
    """The loop should catch exceptions and continue (not crash)."""
    call_count = 0

    @asynccontextmanager
    async def mock_session_factory():
        raise OperationalError("SELECT 1", {}, Exception("DB connection failed"))
        yield  # pragma: no cover

    with (
        patch(
            "app.tasks.health_check.AsyncSessionLocal",
            side_effect=mock_session_factory,
        ),
        patch("app.tasks.health_check.asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
    ):

        async def sleep_and_stop(seconds):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise asyncio.CancelledError()

        mock_sleep.side_effect = sleep_and_stop

        with pytest.raises(asyncio.CancelledError):
            await check_device_health()

        # Should have slept twice (survived the first error, then the second)
        assert call_count == 2


@pytest.mark.asyncio
async def test_check_device_health_uses_configured_interval():
    """The sleep interval should match HEALTH_CHECK_INTERVAL_MINUTES from settings."""
    mock_db = AsyncMock()

    @asynccontextmanager
    async def mock_session_factory():
        yield mock_db

    with (
        patch(
            "app.tasks.health_check.AsyncSessionLocal",
            side_effect=mock_session_factory,
        ),
        patch(
            "app.tasks.health_check.get_active_paired_devices",
            new_callable=AsyncMock,
            return_value=[],
        ),
        patch("app.tasks.health_check.asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
        patch("app.tasks.health_check.settings") as mock_settings,
    ):
        mock_settings.HEALTH_CHECK_INTERVAL_MINUTES = 10

        async def stop_after_one(seconds):
            raise asyncio.CancelledError()

        mock_sleep.side_effect = stop_after_one

        with pytest.raises(asyncio.CancelledError):
            await check_device_health()

        # interval = 10 * 60 = 600 seconds
        mock_sleep.assert_called_once_with(600)


# ---------------------------------------------------------------------------
# should_notify_device_offline — unit tests (subtask 10.2)
# ---------------------------------------------------------------------------

NOW = datetime(2026, 2, 12, 12, 0, 0, tzinfo=UTC)


class TestShouldNotifyDeviceOffline:
    """Tests matching 10.2 test strategy for offline detection."""

    def test_offline_31min_within_schedule(self):
        """Device offline 31 min, within schedule → True for notification."""
        user = _make_user()
        device = _make_paired_device(user)
        device.last_ping_at = NOW - timedelta(minutes=31)
        device.offline_notified = False
        filters = _make_filters(user)

        with patch("app.tasks.health_check.is_within_schedule", return_value=True):
            assert should_notify_device_offline(device, filters, 30, NOW) is True

    def test_offline_31min_outside_schedule(self):
        """Device offline 31 min, outside schedule (night) → False."""
        user = _make_user()
        device = _make_paired_device(user)
        device.last_ping_at = NOW - timedelta(minutes=31)
        device.offline_notified = False
        filters = _make_filters(user)

        with patch("app.tasks.health_check.is_within_schedule", return_value=False):
            assert should_notify_device_offline(device, filters, 30, NOW) is False

    def test_offline_29min_not_yet_offline(self):
        """Device offline 29 min → False (threshold not reached)."""
        user = _make_user()
        device = _make_paired_device(user)
        device.last_ping_at = NOW - timedelta(minutes=29)
        device.offline_notified = False
        filters = _make_filters(user)

        with patch("app.tasks.health_check.is_within_schedule", return_value=True):
            assert should_notify_device_offline(device, filters, 30, NOW) is False

    def test_different_timezones(self):
        """Timezone string correctly passed to is_within_schedule."""
        filters = _make_filters()

        for tz_str in ["Europe/Kyiv", "America/New_York"]:
            user = _make_user(email=f"{tz_str}@test.com")
            device = _make_paired_device(user)
            device.last_ping_at = NOW - timedelta(minutes=31)
            device.timezone = tz_str
            device.offline_notified = False

            with patch(
                "app.tasks.health_check.is_within_schedule", return_value=True
            ) as mock_schedule:
                result = should_notify_device_offline(device, filters, 30, NOW)

            mock_schedule.assert_called_once_with(filters, tz_str)
            assert result is True

    def test_no_last_ping_at(self):
        """Device never pinged (last_ping_at=None) → False."""
        user = _make_user()
        device = _make_paired_device(user)
        device.last_ping_at = None
        filters = _make_filters(user)

        assert should_notify_device_offline(device, filters, 30, NOW) is False

    def test_no_timezone(self):
        """Device with no timezone data → False."""
        user = _make_user()
        device = _make_paired_device(user)
        device.last_ping_at = NOW - timedelta(minutes=31)
        device.timezone = None
        filters = _make_filters(user)

        assert should_notify_device_offline(device, filters, 30, NOW) is False

    def test_already_notified(self):
        """Device already notified (offline_notified=True) → False."""
        user = _make_user()
        device = _make_paired_device(user)
        device.last_ping_at = NOW - timedelta(minutes=31)
        device.offline_notified = True
        filters = _make_filters(user)

        with patch("app.tasks.health_check.is_within_schedule", return_value=True):
            assert should_notify_device_offline(device, filters, 30, NOW) is False

    def test_exact_threshold_not_triggered(self):
        """Elapsed exactly equal to threshold → False (must exceed, not equal)."""
        user = _make_user()
        device = _make_paired_device(user)
        device.last_ping_at = NOW - timedelta(minutes=30)
        device.offline_notified = False
        filters = _make_filters(user)

        with patch("app.tasks.health_check.is_within_schedule", return_value=True):
            assert should_notify_device_offline(device, filters, 30, NOW) is False


# ---------------------------------------------------------------------------
# should_reset_offline_notified — unit tests (subtask 10.2)
# ---------------------------------------------------------------------------


class TestShouldResetOfflineNotified:
    """Tests for recovery detection (device back online)."""

    def test_device_back_online(self):
        """offline_notified=True, recent ping within interval*2 → True."""
        user = _make_user()
        device = _make_paired_device(user)
        device.last_ping_at = NOW - timedelta(seconds=10)
        device.last_interval_sent = 30
        device.offline_notified = True

        assert should_reset_offline_notified(device, 30, NOW) is True

    def test_not_notified(self):
        """offline_notified=False → False (nothing to reset)."""
        user = _make_user()
        device = _make_paired_device(user)
        device.last_ping_at = NOW - timedelta(seconds=10)
        device.offline_notified = False

        assert should_reset_offline_notified(device, 30, NOW) is False

    def test_still_offline(self):
        """offline_notified=True, elapsed > interval*2 → False."""
        user = _make_user()
        device = _make_paired_device(user)
        device.last_ping_at = NOW - timedelta(minutes=5)
        device.last_interval_sent = 30
        device.offline_notified = True

        # elapsed = 300s, interval * 2 = 60s → still offline
        assert should_reset_offline_notified(device, 30, NOW) is False

    def test_uses_default_interval_when_none(self):
        """When last_interval_sent is None, falls back to default."""
        user = _make_user()
        device = _make_paired_device(user)
        device.last_ping_at = NOW - timedelta(seconds=50)
        device.last_interval_sent = None
        device.offline_notified = True

        # elapsed = 50s, default interval * 2 = 60s → 50 < 60 → True
        assert should_reset_offline_notified(device, 30, NOW) is True

    def test_no_last_ping_at(self):
        """last_ping_at is None → False."""
        user = _make_user()
        device = _make_paired_device(user)
        device.last_ping_at = None
        device.offline_notified = True

        assert should_reset_offline_notified(device, 30, NOW) is False


# ---------------------------------------------------------------------------
# check_device_health — FCM push integration tests (subtask 10.3)
# ---------------------------------------------------------------------------


def _health_check_patches(
    devices,
    filters,
    *,
    notify_offline=False,
    reset_offline=False,
    send_push_result=True,
):
    """Return a dict of common patches for check_device_health FCM tests.

    Yields (mock_db, mock_send_push) after entering all patch contexts.
    """
    mock_db = AsyncMock()

    @asynccontextmanager
    async def mock_session_factory():
        yield mock_db

    return (
        mock_db,
        {
            "session": patch(
                "app.tasks.health_check.AsyncSessionLocal",
                side_effect=mock_session_factory,
            ),
            "devices": patch(
                "app.tasks.health_check.get_active_paired_devices",
                new_callable=AsyncMock,
                return_value=devices,
            ),
            "filters": patch(
                "app.tasks.health_check.get_user_filters",
                new_callable=AsyncMock,
                return_value=filters,
            ),
            "notify": patch(
                "app.tasks.health_check.should_notify_device_offline",
                return_value=notify_offline,
            ),
            "reset": patch(
                "app.tasks.health_check.should_reset_offline_notified",
                return_value=reset_offline,
            ),
            "push": patch(
                "app.tasks.health_check.send_push",
                new_callable=AsyncMock,
                return_value=send_push_result,
            ),
            "sleep": patch(
                "app.tasks.health_check.asyncio.sleep",
                new_callable=AsyncMock,
                side_effect=asyncio.CancelledError(),
            ),
        },
    )


class TestCheckDeviceHealthFcm:
    """FCM push integration tests for check_device_health (subtask 10.3)."""

    @pytest.mark.asyncio
    async def test_sends_push_when_device_offline(self):
        """Device offline, offline_notified=False → FCM push sent, flag=True."""
        user = _make_user()
        user.fcm_token = "valid-fcm-token"
        device = _make_paired_device(user)
        device.offline_notified = False
        device.user = user
        filters = _make_filters(user)

        mock_db, patches = _health_check_patches(
            [device],
            filters,
            notify_offline=True,
            send_push_result=True,
        )

        with (
            patches["session"],
            patches["devices"],
            patches["filters"],
            patches["notify"],
            patches["reset"],
            patches["push"] as mock_push,
            patches["sleep"],
        ):
            with pytest.raises(asyncio.CancelledError):
                await check_device_health()

            mock_push.assert_called_once()
            call_args = mock_push.call_args[0]
            assert call_args[0] is mock_db
            assert call_args[1] == "valid-fcm-token"
            assert call_args[2] == NotificationType.SEARCH_OFFLINE
            assert call_args[3]["device_id"] == device.device_id
            assert "last_ping_at" in call_args[3]
            assert call_args[4] == user.id
            assert device.offline_notified is True
            mock_db.commit.assert_called()

    @pytest.mark.asyncio
    async def test_no_push_when_already_notified(self):
        """Device offline, offline_notified=True → FCM push NOT sent (no spam)."""
        user = _make_user()
        user.fcm_token = "valid-fcm-token"
        device = _make_paired_device(user)
        device.offline_notified = True
        device.user = user
        filters = _make_filters(user)

        # should_notify_device_offline returns False when already notified
        _mock_db, patches = _health_check_patches(
            [device],
            filters,
            notify_offline=False,
            reset_offline=False,
        )

        with (
            patches["session"],
            patches["devices"],
            patches["filters"],
            patches["notify"],
            patches["reset"],
            patches["push"] as mock_push,
            patches["sleep"],
        ):
            with pytest.raises(asyncio.CancelledError):
                await check_device_health()

            mock_push.assert_not_called()

    @pytest.mark.asyncio
    async def test_resets_flag_when_device_back_online(self):
        """Device back online → offline_notified resets to False."""
        user = _make_user()
        device = _make_paired_device(user)
        device.offline_notified = True
        device.user = user
        filters = _make_filters(user)

        mock_db, patches = _health_check_patches(
            [device],
            filters,
            notify_offline=False,
            reset_offline=True,
        )

        with (
            patches["session"],
            patches["devices"],
            patches["filters"],
            patches["notify"],
            patches["reset"],
            patches["push"] as mock_push,
            patches["sleep"],
        ):
            with pytest.raises(asyncio.CancelledError):
                await check_device_health()

            mock_push.assert_not_called()
            assert device.offline_notified is False
            mock_db.commit.assert_called()

    @pytest.mark.asyncio
    async def test_flags_without_push_when_no_fcm_token(self):
        """FCM token absent → push not sent, but offline_notified flag set."""
        user = _make_user()
        user.fcm_token = None
        device = _make_paired_device(user)
        device.offline_notified = False
        device.user = user
        filters = _make_filters(user)

        mock_db, patches = _health_check_patches(
            [device],
            filters,
            notify_offline=True,
        )

        with (
            patches["session"],
            patches["devices"],
            patches["filters"],
            patches["notify"],
            patches["reset"],
            patches["push"] as mock_push,
            patches["sleep"],
        ):
            with pytest.raises(asyncio.CancelledError):
                await check_device_health()

            mock_push.assert_not_called()
            assert device.offline_notified is True
            mock_db.commit.assert_called()
