"""Tests for cluster_manager background task (SKE-33).

Test strategy:
1. Full cycle with mock DB and Redis
2. Feature flag off → skip
3. Empty eligible devices → no clusters
4. Penalty calculation: active, penalized, all-penalized reset
5. Redis write verification
6. Error handling: Redis down, DB error
"""

import asyncio
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy.exc import OperationalError

from app.models.app_config import AppConfig
from app.models.credit_balance import CreditBalance
from app.models.paired_device import PairedDevice
from app.models.ride import Ride
from app.models.search_status import SearchStatus
from app.models.user import User
from app.tasks.cluster_manager import (
    DEFAULT_CLUSTERING_PENALTY_MINUTES,
    DEFAULT_CLUSTERING_REBUILD_INTERVAL_MINUTES,
    DEFAULT_CLUSTERING_THRESHOLD_MILES,
    _safe_int,
    cleanup_stale_cluster_keys,
    clear_cluster_keys,
    compute_member_statuses,
    devices_to_dicts,
    get_clustering_config,
    get_eligible_devices,
    run_cluster_manager,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_user(email: str = "test@example.com") -> User:
    return User(id=uuid.uuid4(), email=email, password_hash="fakehash")


def _make_device(
    user: User,
    device_id: str = "device-1",
    latitude: float | None = 40.7128,
    longitude: float | None = -74.0060,
    offline_notified: bool = False,
) -> PairedDevice:
    return PairedDevice(
        id=uuid.uuid4(),
        user_id=user.id,
        device_id=device_id,
        device_token_hash="a" * 64,
        timezone="America/New_York",
        latitude=latitude,
        longitude=longitude,
        offline_notified=offline_notified,
    )


def _make_search_status(user: User, is_active: bool = True) -> SearchStatus:
    return SearchStatus(id=uuid.uuid4(), user_id=user.id, is_active=is_active)


def _make_balance(user: User, balance: int = 10) -> CreditBalance:
    return CreditBalance(id=uuid.uuid4(), user_id=user.id, balance=balance)


def _make_ride(
    user: User,
    event_type: str = "ACCEPTED",
    created_at: datetime | None = None,
) -> Ride:
    return Ride(
        id=uuid.uuid4(),
        user_id=user.id,
        idempotency_key=str(uuid.uuid4()),
        event_type=event_type,
        ride_data={"type": "test"},
        ride_hash="h" * 64,
        created_at=created_at or datetime.now(UTC),
    )


# ---------------------------------------------------------------------------
# _safe_int
# ---------------------------------------------------------------------------


def test_safe_int_valid():
    assert _safe_int("42", 10) == 42


def test_safe_int_non_numeric_returns_default():
    assert _safe_int("abc", 10) == 10


def test_safe_int_empty_string_returns_default():
    assert _safe_int("", 5) == 5


def test_safe_int_none_returns_default():
    assert _safe_int(None, 7) == 7


def test_safe_int_already_int():
    assert _safe_int(99, 10) == 99


# ---------------------------------------------------------------------------
# get_clustering_config
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_clustering_config_defaults(db_session):
    """No AppConfig rows → all defaults returned."""
    config = await get_clustering_config(db_session)
    assert config["enabled"] is False
    assert config["penalty_minutes"] == DEFAULT_CLUSTERING_PENALTY_MINUTES
    assert config["threshold_miles"] == DEFAULT_CLUSTERING_THRESHOLD_MILES
    assert config["rebuild_interval_minutes"] == DEFAULT_CLUSTERING_REBUILD_INTERVAL_MINUTES


@pytest.mark.asyncio
async def test_get_clustering_config_from_db(db_session):
    """AppConfig rows override defaults."""
    db_session.add_all(
        [
            AppConfig(key="clustering_enabled", value="true"),
            AppConfig(key="clustering_penalty_minutes", value="30"),
            AppConfig(key="clustering_threshold_miles", value="10"),
            AppConfig(key="clustering_rebuild_interval_minutes", value="3"),
        ]
    )
    await db_session.flush()

    config = await get_clustering_config(db_session)
    assert config["enabled"] is True
    assert config["penalty_minutes"] == 30
    assert config["threshold_miles"] == 10
    assert config["rebuild_interval_minutes"] == 3


@pytest.mark.asyncio
async def test_get_clustering_config_enabled_variants(db_session):
    """Various truthy values for clustering_enabled."""
    for val in ("true", "True", "1", "yes"):
        db_session.add(AppConfig(key="clustering_enabled", value=val))
        await db_session.flush()
        config = await get_clustering_config(db_session)
        assert config["enabled"] is True, f"Expected True for value '{val}'"
        await db_session.rollback()


@pytest.mark.asyncio
async def test_get_clustering_config_disabled_variants(db_session):
    """Various falsy values for clustering_enabled."""
    for val in ("false", "0", "no", "anything"):
        db_session.add(AppConfig(key="clustering_enabled", value=val))
        await db_session.flush()
        config = await get_clustering_config(db_session)
        assert config["enabled"] is False, f"Expected False for value '{val}'"
        await db_session.rollback()


@pytest.mark.asyncio
async def test_get_clustering_config_non_numeric_falls_back(db_session):
    """Non-numeric config values fall back to defaults instead of crashing."""
    db_session.add_all(
        [
            AppConfig(key="clustering_penalty_minutes", value="not_a_number"),
            AppConfig(key="clustering_threshold_miles", value="abc"),
            AppConfig(key="clustering_rebuild_interval_minutes", value=""),
        ]
    )
    await db_session.flush()

    config = await get_clustering_config(db_session)
    assert config["penalty_minutes"] == DEFAULT_CLUSTERING_PENALTY_MINUTES
    assert config["threshold_miles"] == DEFAULT_CLUSTERING_THRESHOLD_MILES
    assert config["rebuild_interval_minutes"] == DEFAULT_CLUSTERING_REBUILD_INTERVAL_MINUTES


# ---------------------------------------------------------------------------
# cleanup_stale_cluster_keys
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cleanup_stale_cluster_keys_removes_only_stale():
    """Only keys not in the current set are deleted."""
    mock_redis = AsyncMock()

    # Simulate existing keys: cluster c1 (current) and c2 (stale)
    async def mock_scan(cursor=0, match="", count=200):
        if "device_cluster" in match:
            return (0, ["device_cluster:d1", "device_cluster:d_old"])
        if match == "cluster:*":
            return (0, ["cluster:c1", "cluster:c2"])
        if "cluster_members" in match:
            return (0, ["cluster_members:c1", "cluster_members:c2"])
        if "cluster_last_search" in match:
            return (0, ["cluster_last_search:c1", "cluster_last_search:c2"])
        return (0, [])

    mock_redis.scan = AsyncMock(side_effect=mock_scan)
    mock_redis.delete = AsyncMock()

    await cleanup_stale_cluster_keys(
        mock_redis,
        current_cluster_ids={"c1"},
        current_device_ids={"d1"},
    )

    # Collect all deleted keys across calls
    deleted = []
    for call in mock_redis.delete.call_args_list:
        deleted.extend(call.args)

    # Stale keys should be deleted
    assert "device_cluster:d_old" in deleted
    assert "cluster:c2" in deleted
    assert "cluster_members:c2" in deleted
    assert "cluster_last_search:c2" in deleted
    # Current keys should NOT be deleted
    assert "device_cluster:d1" not in deleted
    assert "cluster:c1" not in deleted
    assert "cluster_members:c1" not in deleted
    assert "cluster_last_search:c1" not in deleted


@pytest.mark.asyncio
async def test_cleanup_stale_cluster_keys_nothing_stale():
    """No stale keys → no deletes."""
    mock_redis = AsyncMock()

    async def mock_scan(cursor=0, match="", count=200):
        if "device_cluster" in match:
            return (0, ["device_cluster:d1"])
        if match == "cluster:*":
            return (0, ["cluster:c1"])
        return (0, [])

    mock_redis.scan = AsyncMock(side_effect=mock_scan)
    mock_redis.delete = AsyncMock()

    await cleanup_stale_cluster_keys(
        mock_redis,
        current_cluster_ids={"c1"},
        current_device_ids={"d1"},
    )

    mock_redis.delete.assert_not_called()


# ---------------------------------------------------------------------------
# get_eligible_devices
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_eligible_devices_all_criteria_met(db_session):
    """Device with all criteria met is returned."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    device = _make_device(user, latitude=40.71, longitude=-74.00)
    status = _make_search_status(user, is_active=True)
    balance = _make_balance(user, balance=5)
    db_session.add_all([device, status, balance])
    await db_session.flush()

    devices = await get_eligible_devices(db_session)
    assert len(devices) == 1
    assert devices[0].device_id == "device-1"


@pytest.mark.asyncio
async def test_get_eligible_devices_inactive_search(db_session):
    """Device with is_active=False is excluded."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    device = _make_device(user)
    status = _make_search_status(user, is_active=False)
    balance = _make_balance(user)
    db_session.add_all([device, status, balance])
    await db_session.flush()

    assert await get_eligible_devices(db_session) == []


@pytest.mark.asyncio
async def test_get_eligible_devices_zero_balance(db_session):
    """Device with zero credit balance is excluded."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    device = _make_device(user)
    status = _make_search_status(user, is_active=True)
    balance = _make_balance(user, balance=0)
    db_session.add_all([device, status, balance])
    await db_session.flush()

    assert await get_eligible_devices(db_session) == []


@pytest.mark.asyncio
async def test_get_eligible_devices_offline_notified(db_session):
    """Device with offline_notified=True is excluded."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    device = _make_device(user, offline_notified=True)
    status = _make_search_status(user, is_active=True)
    balance = _make_balance(user)
    db_session.add_all([device, status, balance])
    await db_session.flush()

    assert await get_eligible_devices(db_session) == []


@pytest.mark.asyncio
async def test_get_eligible_devices_no_latitude(db_session):
    """Device with latitude=None is excluded."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    device = _make_device(user, latitude=None)
    status = _make_search_status(user, is_active=True)
    balance = _make_balance(user)
    db_session.add_all([device, status, balance])
    await db_session.flush()

    assert await get_eligible_devices(db_session) == []


@pytest.mark.asyncio
async def test_get_eligible_devices_empty(db_session):
    """No devices at all → empty list."""
    assert await get_eligible_devices(db_session) == []


@pytest.mark.asyncio
async def test_get_eligible_devices_multiple(db_session):
    """Multiple users: only eligible ones returned."""
    user_ok = _make_user("ok@test.com")
    user_bad = _make_user("bad@test.com")
    db_session.add_all([user_ok, user_bad])
    await db_session.flush()

    # Eligible user
    db_session.add_all(
        [
            _make_device(user_ok, device_id="dev-ok"),
            _make_search_status(user_ok, is_active=True),
            _make_balance(user_ok, balance=5),
        ]
    )
    # Ineligible (inactive)
    db_session.add_all(
        [
            _make_device(user_bad, device_id="dev-bad"),
            _make_search_status(user_bad, is_active=False),
            _make_balance(user_bad, balance=5),
        ]
    )
    await db_session.flush()

    devices = await get_eligible_devices(db_session)
    assert len(devices) == 1
    assert devices[0].device_id == "dev-ok"


# ---------------------------------------------------------------------------
# devices_to_dicts
# ---------------------------------------------------------------------------


def test_devices_to_dicts():
    """PairedDevice objects are converted to dicts with correct keys."""
    user = _make_user()
    device = _make_device(user, device_id="d1", latitude=40.0, longitude=-74.0)

    result = devices_to_dicts([device])
    assert len(result) == 1
    assert result[0]["device_id"] == "d1"
    assert result[0]["lat"] == 40.0
    assert result[0]["lon"] == -74.0
    assert result[0]["user_id"] == user.id


def test_devices_to_dicts_empty():
    """Empty list → empty result."""
    assert devices_to_dicts([]) == []


# ---------------------------------------------------------------------------
# compute_member_statuses
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_compute_statuses_no_rides_all_active(db_session):
    """Devices with no rides → all active."""
    user1 = _make_user("u1@test.com")
    user2 = _make_user("u2@test.com")
    db_session.add_all([user1, user2])
    await db_session.flush()

    devs = [
        {"device_id": "d1", "user_id": user1.id, "lat": 40.0, "lon": -74.0},
        {"device_id": "d2", "user_id": user2.id, "lat": 40.0, "lon": -74.0},
    ]
    now = datetime.now(UTC)
    statuses = await compute_member_statuses(db_session, devs, 60, now)
    assert statuses == {"d1": "active", "d2": "active"}


@pytest.mark.asyncio
async def test_compute_statuses_recent_ride_penalized(db_session):
    """Device with recent ACCEPTED ride → penalized."""
    user1 = _make_user("u1@test.com")
    user2 = _make_user("u2@test.com")
    db_session.add_all([user1, user2])
    await db_session.flush()

    now = datetime.now(UTC)
    ride = _make_ride(user1, event_type="ACCEPTED", created_at=now - timedelta(minutes=10))
    db_session.add(ride)
    await db_session.flush()

    devs = [
        {"device_id": "d1", "user_id": user1.id, "lat": 40.0, "lon": -74.0},
        {"device_id": "d2", "user_id": user2.id, "lat": 40.0, "lon": -74.0},
    ]
    statuses = await compute_member_statuses(db_session, devs, 60, now)
    assert statuses["d1"] == "penalized"
    assert statuses["d2"] == "active"


@pytest.mark.asyncio
async def test_compute_statuses_old_ride_active(db_session):
    """Device with old ride (beyond penalty window) → active."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    now = datetime.now(UTC)
    ride = _make_ride(user, event_type="CONFIRMED", created_at=now - timedelta(minutes=120))
    db_session.add(ride)
    await db_session.flush()

    devs = [{"device_id": "d1", "user_id": user.id, "lat": 40.0, "lon": -74.0}]
    statuses = await compute_member_statuses(db_session, devs, 60, now)
    assert statuses["d1"] == "active"


@pytest.mark.asyncio
async def test_compute_statuses_all_penalized_reset(db_session):
    """All members penalized → all reset to active."""
    user1 = _make_user("u1@test.com")
    user2 = _make_user("u2@test.com")
    db_session.add_all([user1, user2])
    await db_session.flush()

    now = datetime.now(UTC)
    ride1 = _make_ride(user1, event_type="ACCEPTED", created_at=now - timedelta(minutes=5))
    ride2 = _make_ride(user2, event_type="CONFIRMED", created_at=now - timedelta(minutes=10))
    db_session.add_all([ride1, ride2])
    await db_session.flush()

    devs = [
        {"device_id": "d1", "user_id": user1.id, "lat": 40.0, "lon": -74.0},
        {"device_id": "d2", "user_id": user2.id, "lat": 40.0, "lon": -74.0},
    ]
    statuses = await compute_member_statuses(db_session, devs, 60, now)
    # Both should be reset to active since all were penalized
    assert statuses == {"d1": "active", "d2": "active"}


@pytest.mark.asyncio
async def test_compute_statuses_mixed_active_penalized(db_session):
    """Mix of active and penalized → no reset."""
    user1 = _make_user("u1@test.com")
    user2 = _make_user("u2@test.com")
    db_session.add_all([user1, user2])
    await db_session.flush()

    now = datetime.now(UTC)
    # user1 has recent ride (penalized), user2 has no ride (active)
    ride1 = _make_ride(user1, event_type="ACCEPTED", created_at=now - timedelta(minutes=5))
    db_session.add(ride1)
    await db_session.flush()

    devs = [
        {"device_id": "d1", "user_id": user1.id, "lat": 40.0, "lon": -74.0},
        {"device_id": "d2", "user_id": user2.id, "lat": 40.0, "lon": -74.0},
    ]
    statuses = await compute_member_statuses(db_session, devs, 60, now)
    assert statuses["d1"] == "penalized"
    assert statuses["d2"] == "active"


# ---------------------------------------------------------------------------
# clear_cluster_keys
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_clear_cluster_keys():
    """All cluster patterns are scanned and deleted."""
    mock_redis = AsyncMock()

    # Simulate SCAN returning keys then 0 cursor
    async def mock_scan(cursor=0, match="", count=200):
        if cursor == 0 and "device_cluster" in match:
            return (0, ["device_cluster:d1", "device_cluster:d2"])
        if cursor == 0 and match == "cluster:*":
            return (0, ["cluster:c1"])
        if cursor == 0 and "cluster_members" in match:
            return (0, [])
        if cursor == 0 and "cluster_last_search" in match:
            return (0, [])
        return (0, [])

    mock_redis.scan = AsyncMock(side_effect=mock_scan)
    mock_redis.delete = AsyncMock()

    await clear_cluster_keys(mock_redis)

    # Should have called delete for the keys that were found
    assert mock_redis.delete.call_count == 2  # device_cluster:* and cluster:*


# ---------------------------------------------------------------------------
# run_cluster_manager — loop tests
# ---------------------------------------------------------------------------


def _make_mock_session(config_enabled=False):
    """Create a mock async session context manager."""
    mock_db = AsyncMock()

    @asynccontextmanager
    async def mock_session_factory():
        yield mock_db

    return mock_db, mock_session_factory


@pytest.mark.asyncio
async def test_run_cluster_manager_disabled():
    """Feature flag off → skip cycle, sleep, loop."""
    call_count = 0
    _mock_db, mock_session = _make_mock_session()

    with (
        patch(
            "app.tasks.cluster_manager.AsyncSessionLocal",
            side_effect=mock_session,
        ),
        patch(
            "app.tasks.cluster_manager.get_clustering_config",
            new_callable=AsyncMock,
            return_value={
                "enabled": False,
                "penalty_minutes": 60,
                "threshold_miles": 16,
                "rebuild_interval_minutes": 5,
            },
        ) as mock_config,
        patch(
            "app.tasks.cluster_manager.asyncio.sleep",
            new_callable=AsyncMock,
        ) as mock_sleep,
    ):

        async def stop_loop(seconds):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise asyncio.CancelledError()

        mock_sleep.side_effect = stop_loop

        with pytest.raises(asyncio.CancelledError):
            await run_cluster_manager()

        mock_config.assert_called()
        # Should not have called build_clusters or clear_cluster_keys


@pytest.mark.asyncio
async def test_run_cluster_manager_no_eligible_devices():
    """Enabled but no eligible devices → clear keys, skip cluster building."""
    call_count = 0
    _mock_db, mock_session = _make_mock_session()

    with (
        patch(
            "app.tasks.cluster_manager.AsyncSessionLocal",
            side_effect=mock_session,
        ),
        patch(
            "app.tasks.cluster_manager.get_clustering_config",
            new_callable=AsyncMock,
            return_value={
                "enabled": True,
                "penalty_minutes": 60,
                "threshold_miles": 16,
                "rebuild_interval_minutes": 5,
            },
        ),
        patch(
            "app.tasks.cluster_manager.clear_cluster_keys",
            new_callable=AsyncMock,
        ) as mock_clear,
        patch(
            "app.tasks.cluster_manager.get_eligible_devices",
            new_callable=AsyncMock,
            return_value=[],
        ),
        patch(
            "app.tasks.cluster_manager.build_clusters",
            new_callable=AsyncMock,
        ) as mock_build,
        patch(
            "app.tasks.cluster_manager.asyncio.sleep",
            new_callable=AsyncMock,
        ) as mock_sleep,
    ):

        async def stop_loop(seconds):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise asyncio.CancelledError()

        mock_sleep.side_effect = stop_loop

        with pytest.raises(asyncio.CancelledError):
            await run_cluster_manager()

        mock_clear.assert_called_once()
        mock_build.assert_not_called()


@pytest.mark.asyncio
async def test_run_cluster_manager_full_cycle():
    """Full cycle: eligible devices → clusters → statuses → Redis write → stale cleanup."""
    call_count = 0
    _mock_db, mock_session = _make_mock_session()

    user1 = _make_user("u1@test.com")
    user2 = _make_user("u2@test.com")

    device1 = _make_device(user1, device_id="d1")
    device2 = _make_device(user2, device_id="d2")

    cluster_result = {
        "cluster-abc": [
            {"device_id": "d1", "lat": 40.7, "lon": -74.0, "user_id": user1.id},
            {"device_id": "d2", "lat": 40.71, "lon": -74.01, "user_id": user2.id},
        ]
    }

    with (
        patch(
            "app.tasks.cluster_manager.AsyncSessionLocal",
            side_effect=mock_session,
        ),
        patch(
            "app.tasks.cluster_manager.get_clustering_config",
            new_callable=AsyncMock,
            return_value={
                "enabled": True,
                "penalty_minutes": 60,
                "threshold_miles": 16,
                "rebuild_interval_minutes": 5,
            },
        ),
        patch(
            "app.tasks.cluster_manager.cleanup_stale_cluster_keys",
            new_callable=AsyncMock,
        ) as mock_cleanup,
        patch(
            "app.tasks.cluster_manager.get_eligible_devices",
            new_callable=AsyncMock,
            return_value=[device1, device2],
        ),
        patch(
            "app.tasks.cluster_manager.build_clusters",
            new_callable=AsyncMock,
            return_value=cluster_result,
        ),
        patch(
            "app.tasks.cluster_manager.compute_member_statuses",
            new_callable=AsyncMock,
            return_value={"d1": "active", "d2": "active"},
        ),
        patch(
            "app.tasks.cluster_manager.get_search_interval_config",
            new_callable=AsyncMock,
            return_value=(1920, [4.17] * 24),
        ),
        patch(
            "app.tasks.cluster_manager.calculate_dynamic_interval",
            return_value=15,
        ),
        patch(
            "app.tasks.cluster_manager.write_clusters_to_redis",
            new_callable=AsyncMock,
        ) as mock_write,
        patch(
            "app.tasks.cluster_manager.asyncio.sleep",
            new_callable=AsyncMock,
        ) as mock_sleep,
    ):

        async def stop_loop(seconds):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise asyncio.CancelledError()

        mock_sleep.side_effect = stop_loop

        with pytest.raises(asyncio.CancelledError):
            await run_cluster_manager()

        # Verify write_clusters_to_redis was called with correct args
        mock_write.assert_called_once()
        call_args = mock_write.call_args[0]
        assert call_args[0] == cluster_result  # clusters
        assert call_args[1] == {"d1": "active", "d2": "active"}  # statuses
        # cluster_params: active_members=2, search_interval=15*2=30
        assert call_args[2]["cluster-abc"]["active_members"] == 2
        assert call_args[2]["cluster-abc"]["search_interval"] == 30

        # Verify stale cleanup was called with correct cluster/device ids
        mock_cleanup.assert_called_once()
        cleanup_args = mock_cleanup.call_args[0]
        assert cleanup_args[1] == {"cluster-abc"}
        assert cleanup_args[2] == {"d1", "d2"}


@pytest.mark.asyncio
async def test_run_cluster_manager_no_clusters_formed():
    """Devices found but build_clusters returns empty → clear keys, skip write."""
    call_count = 0
    _mock_db, mock_session = _make_mock_session()

    device = _make_device(_make_user())

    with (
        patch(
            "app.tasks.cluster_manager.AsyncSessionLocal",
            side_effect=mock_session,
        ),
        patch(
            "app.tasks.cluster_manager.get_clustering_config",
            new_callable=AsyncMock,
            return_value={
                "enabled": True,
                "penalty_minutes": 60,
                "threshold_miles": 16,
                "rebuild_interval_minutes": 5,
            },
        ),
        patch(
            "app.tasks.cluster_manager.clear_cluster_keys",
            new_callable=AsyncMock,
        ) as mock_clear,
        patch(
            "app.tasks.cluster_manager.get_eligible_devices",
            new_callable=AsyncMock,
            return_value=[device],
        ),
        patch(
            "app.tasks.cluster_manager.build_clusters",
            new_callable=AsyncMock,
            return_value={},
        ),
        patch(
            "app.tasks.cluster_manager.write_clusters_to_redis",
            new_callable=AsyncMock,
        ) as mock_write,
        patch(
            "app.tasks.cluster_manager.asyncio.sleep",
            new_callable=AsyncMock,
        ) as mock_sleep,
    ):

        async def stop_loop(seconds):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise asyncio.CancelledError()

        mock_sleep.side_effect = stop_loop

        with pytest.raises(asyncio.CancelledError):
            await run_cluster_manager()

        mock_clear.assert_called_once()
        mock_write.assert_not_called()


@pytest.mark.asyncio
async def test_run_cluster_manager_db_error():
    """DB error → logged, loop continues."""
    call_count = 0

    @asynccontextmanager
    async def failing_session():
        raise OperationalError("SELECT 1", {}, Exception("DB down"))
        yield  # pragma: no cover

    with (
        patch(
            "app.tasks.cluster_manager.AsyncSessionLocal",
            side_effect=failing_session,
        ),
        patch(
            "app.tasks.cluster_manager.asyncio.sleep",
            new_callable=AsyncMock,
        ) as mock_sleep,
    ):

        async def stop_loop(seconds):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise asyncio.CancelledError()

        mock_sleep.side_effect = stop_loop

        with pytest.raises(asyncio.CancelledError):
            await run_cluster_manager()

        # Loop should survive the error and sleep
        assert call_count == 2


@pytest.mark.asyncio
async def test_run_cluster_manager_redis_error_during_write():
    """Redis error during write_clusters_to_redis → logged, loop continues."""
    call_count = 0
    _mock_db, mock_session = _make_mock_session()

    from redis.exceptions import RedisError

    device = _make_device(_make_user())

    with (
        patch(
            "app.tasks.cluster_manager.AsyncSessionLocal",
            side_effect=mock_session,
        ),
        patch(
            "app.tasks.cluster_manager.get_clustering_config",
            new_callable=AsyncMock,
            return_value={
                "enabled": True,
                "penalty_minutes": 60,
                "threshold_miles": 16,
                "rebuild_interval_minutes": 5,
            },
        ),
        patch(
            "app.tasks.cluster_manager.get_eligible_devices",
            new_callable=AsyncMock,
            return_value=[device],
        ),
        patch(
            "app.tasks.cluster_manager.build_clusters",
            new_callable=AsyncMock,
            return_value={"c1": [{"device_id": "d1", "lat": 40.0, "lon": -74.0}]},
        ),
        patch(
            "app.tasks.cluster_manager.compute_member_statuses",
            new_callable=AsyncMock,
            return_value={"d1": "active"},
        ),
        patch(
            "app.tasks.cluster_manager.get_search_interval_config",
            new_callable=AsyncMock,
            return_value=None,
        ),
        patch(
            "app.tasks.cluster_manager.write_clusters_to_redis",
            new_callable=AsyncMock,
            side_effect=RedisError("connection refused"),
        ),
        patch(
            "app.tasks.cluster_manager.asyncio.sleep",
            new_callable=AsyncMock,
        ) as mock_sleep,
    ):

        async def stop_loop(seconds):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise asyncio.CancelledError()

        mock_sleep.side_effect = stop_loop

        with pytest.raises(asyncio.CancelledError):
            await run_cluster_manager()

        assert call_count == 2


@pytest.mark.asyncio
async def test_run_cluster_manager_sleep_interval():
    """Sleep uses rebuild_interval_minutes from config."""
    call_count = 0
    _mock_db, mock_session = _make_mock_session()

    with (
        patch(
            "app.tasks.cluster_manager.AsyncSessionLocal",
            side_effect=mock_session,
        ),
        patch(
            "app.tasks.cluster_manager.get_clustering_config",
            new_callable=AsyncMock,
            return_value={
                "enabled": False,
                "penalty_minutes": 60,
                "threshold_miles": 16,
                "rebuild_interval_minutes": 3,
            },
        ),
        patch(
            "app.tasks.cluster_manager.asyncio.sleep",
            new_callable=AsyncMock,
        ) as mock_sleep,
    ):

        async def stop_after_second(seconds):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise asyncio.CancelledError()

        mock_sleep.side_effect = stop_after_second

        with pytest.raises(asyncio.CancelledError):
            await run_cluster_manager()

        # First sleep is INITIAL_DELAY_SECONDS (10), second is 3*60=180
        calls = [c.args[0] for c in mock_sleep.call_args_list]
        assert calls[0] == 10  # initial delay
        assert calls[1] == 180  # 3 * 60


@pytest.mark.asyncio
async def test_run_cluster_manager_interval_fallback():
    """No interval config → fallback interval = 60 * active_members."""
    call_count = 0
    _mock_db, mock_session = _make_mock_session()

    user1 = _make_user("u1@test.com")
    device1 = _make_device(user1, device_id="d1")

    cluster_result = {
        "cluster-x": [
            {"device_id": "d1", "lat": 40.7, "lon": -74.0, "user_id": user1.id},
        ]
    }

    with (
        patch(
            "app.tasks.cluster_manager.AsyncSessionLocal",
            side_effect=mock_session,
        ),
        patch(
            "app.tasks.cluster_manager.get_clustering_config",
            new_callable=AsyncMock,
            return_value={
                "enabled": True,
                "penalty_minutes": 60,
                "threshold_miles": 16,
                "rebuild_interval_minutes": 5,
            },
        ),
        patch(
            "app.tasks.cluster_manager.cleanup_stale_cluster_keys",
            new_callable=AsyncMock,
        ),
        patch(
            "app.tasks.cluster_manager.get_eligible_devices",
            new_callable=AsyncMock,
            return_value=[device1],
        ),
        patch(
            "app.tasks.cluster_manager.build_clusters",
            new_callable=AsyncMock,
            return_value=cluster_result,
        ),
        patch(
            "app.tasks.cluster_manager.compute_member_statuses",
            new_callable=AsyncMock,
            return_value={"d1": "active"},
        ),
        patch(
            "app.tasks.cluster_manager.get_search_interval_config",
            new_callable=AsyncMock,
            return_value=None,
        ),
        patch(
            "app.tasks.cluster_manager.write_clusters_to_redis",
            new_callable=AsyncMock,
        ) as mock_write,
        patch(
            "app.tasks.cluster_manager.asyncio.sleep",
            new_callable=AsyncMock,
        ) as mock_sleep,
    ):

        async def stop_loop(seconds):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise asyncio.CancelledError()

        mock_sleep.side_effect = stop_loop

        with pytest.raises(asyncio.CancelledError):
            await run_cluster_manager()

        mock_write.assert_called_once()
        params = mock_write.call_args[0][2]
        # Fallback: 60 * 1 active member = 60
        assert params["cluster-x"]["search_interval"] == 60
        assert params["cluster-x"]["active_members"] == 1
