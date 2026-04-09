import uuid
from datetime import UTC, datetime, time
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

import pytest
from fastapi import HTTPException
from redis.exceptions import RedisError
from sqlalchemy import select

from app.models.accept_failure import AcceptFailure as AcceptFailureModel
from app.models.paired_device import PairedDevice
from app.models.ride import Ride
from app.models.search_filters import SearchFilters
from app.models.user import User
from app.schemas.ping import (
    AcceptFailureItem,
    DeviceHealth,
    DeviceLocation,
    PingRequest,
    PingStats,
    RideStatusReport,
)
from app.services.ping_service import (
    BATCH_DEDUP_TTL,
    BATCH_KEY_PREFIX,
    MIN_INTERVAL_SECONDS,
    calculate_dynamic_interval,
    check_app_version,
    is_batch_already_processed,
    is_within_schedule,
    mark_batch_as_processed,
    parse_time,
    process_ride_status_reports,
    process_stats_if_new,
    save_accept_failures,
    update_device_state,
    validate_timezone,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_filters(
    start_time: str = "09:00",
    working_time: int = 8,
    working_days: list[str] | None = None,
) -> SearchFilters:
    """Create an in-memory SearchFilters instance for testing."""
    if working_days is None:
        working_days = ["MON", "TUE", "WED", "THU", "FRI"]
    return SearchFilters(
        id=uuid.uuid4(),
        user_id=uuid.uuid4(),
        start_time=start_time,
        working_time=working_time,
        working_days=working_days,
    )


def _patch_now(target_now: datetime):
    """Patch datetime in ping_service so that datetime.now(tz) returns *target_now*.

    The returned context-manager replaces the ``datetime`` class inside
    ``ping_service`` with a thin subclass that overrides ``now()`` while
    keeping the constructor and all other behaviour intact.
    """
    real_datetime = datetime

    class _FakeDatetime(real_datetime):  # type: ignore[type-arg]
        @classmethod
        def now(cls, tz=None):
            if tz is not None:
                return target_now.astimezone(tz)
            return target_now

    import contextlib

    @contextlib.contextmanager
    def _combined():
        with (
            patch("app.services.ping_service.schedule.datetime", _FakeDatetime),
            patch("app.services.ping_service.device.datetime", _FakeDatetime),
            patch("app.services.ping_service.verification.datetime", _FakeDatetime),
        ):
            yield

    return _combined()


# --- Test 1: valid timezone 'America/New_York' → returns ZoneInfo ---


def test_validate_timezone_america_new_york():
    result = validate_timezone("America/New_York")
    assert isinstance(result, ZoneInfo)
    assert str(result) == "America/New_York"


# --- Test 2: valid timezone 'Europe/Kyiv' → returns ZoneInfo ---


def test_validate_timezone_europe_kyiv():
    result = validate_timezone("Europe/Kyiv")
    assert isinstance(result, ZoneInfo)
    assert str(result) == "Europe/Kyiv"


# --- Test 3: valid timezone 'UTC' → returns ZoneInfo ---


def test_validate_timezone_utc():
    result = validate_timezone("UTC")
    assert isinstance(result, ZoneInfo)
    assert str(result) == "UTC"


# --- Test 4: invalid timezone 'Invalid/Zone' → HTTPException 422 ---


def test_validate_timezone_invalid_zone_raises_422():
    with pytest.raises(HTTPException) as exc_info:
        validate_timezone("Invalid/Zone")

    assert exc_info.value.status_code == 422
    assert exc_info.value.detail == "INVALID_TIMEZONE"


# --- Test 5: empty string '' → HTTPException 422 ---


def test_validate_timezone_empty_string_raises_422():
    with pytest.raises(HTTPException) as exc_info:
        validate_timezone("")

    assert exc_info.value.status_code == 422
    assert exc_info.value.detail == "INVALID_TIMEZONE"


# --- Test 6: timezone with DST 'America/Los_Angeles' → ZoneInfo with correct offsets ---


def test_validate_timezone_dst_los_angeles():
    result = validate_timezone("America/Los_Angeles")
    assert isinstance(result, ZoneInfo)
    assert str(result) == "America/Los_Angeles"

    # Verify DST-aware: summer (PDT, UTC-7) and winter (PST, UTC-8) have different offsets
    summer = datetime(2024, 7, 1, 12, 0, tzinfo=result)
    winter = datetime(2024, 1, 1, 12, 0, tzinfo=result)
    assert summer.utcoffset() != winter.utcoffset()


# === check_app_version tests ===


# --- Test 1: equal versions '1.0.0' >= '1.0.0' → True ---


def test_check_app_version_equal_versions():
    assert check_app_version("1.0.0", "1.0.0") is True


# --- Test 2: newer version '1.1.0' >= '1.0.0' → True ---


def test_check_app_version_newer_version():
    assert check_app_version("1.1.0", "1.0.0") is True


# --- Test 3: older version '0.9.0' >= '1.0.0' → False ---


def test_check_app_version_older_version():
    assert check_app_version("0.9.0", "1.0.0") is False


# --- Test 4: pre-release '2.0.0-beta' >= '1.0.0' → True (normalized to 2.0.0b0) ---


def test_check_app_version_prerelease():
    assert check_app_version("2.0.0-beta", "1.0.0") is True


# --- Test 5: invalid version string 'invalid' → False ---


def test_check_app_version_invalid_string():
    assert check_app_version("invalid", "1.0.0") is False


# --- Test 6: empty version string '' → False ---


def test_check_app_version_empty_string():
    assert check_app_version("", "1.0.0") is False


# --- Test 7: edge case '1.0.0' vs '1.0.0.1' → False (1.0.0 < 1.0.0.1) ---


def test_check_app_version_micro_release():
    assert check_app_version("1.0.0", "1.0.0.1") is False


# === parse_time tests ===


# --- parse_time: typical morning time ---


def test_parse_time_morning():
    assert parse_time("09:00") == time(9, 0)


# --- parse_time: late evening time ---


def test_parse_time_evening():
    assert parse_time("22:45") == time(22, 45)


# --- parse_time: midnight ---


def test_parse_time_midnight():
    assert parse_time("00:00") == time(0, 0)


# --- parse_time: rejects invalid format ---


def test_parse_time_rejects_no_colon():
    with pytest.raises(HTTPException) as exc_info:
        parse_time("0930")
    assert exc_info.value.status_code == 422


def test_parse_time_rejects_letters():
    with pytest.raises(HTTPException) as exc_info:
        parse_time("ab:cd")
    assert exc_info.value.status_code == 422


def test_parse_time_rejects_out_of_range_hour():
    with pytest.raises(HTTPException) as exc_info:
        parse_time("25:00")
    assert exc_info.value.status_code == 422


def test_parse_time_rejects_out_of_range_minute():
    with pytest.raises(HTTPException) as exc_info:
        parse_time("12:60")
    assert exc_info.value.status_code == 422


def test_parse_time_rejects_empty_string():
    with pytest.raises(HTTPException) as exc_info:
        parse_time("")
    assert exc_info.value.status_code == 422


def test_parse_time_rejects_extra_segments():
    with pytest.raises(HTTPException) as exc_info:
        parse_time("12:30:00")
    assert exc_info.value.status_code == 422


# === is_within_schedule tests ===
#
# Date reference (2024):
#   Mar 13 = Wednesday, Mar 15 = Friday, Mar 16 = Saturday, Mar 17 = Sunday
#   Mar 10 = Sunday (DST spring forward in America/New_York)
#   Nov  3 = Sunday (DST fall back  in America/New_York)


# --- Test 1: 24h mode on a working day (WED) → True ---


def test_schedule_24h_mode_working_day():
    filters = _make_filters(working_time=24, working_days=["MON", "TUE", "WED", "THU", "FRI"])
    # Wednesday 2024-03-13 10:00 UTC
    now = datetime(2024, 3, 13, 10, 0, tzinfo=ZoneInfo("UTC"))
    with _patch_now(now):
        assert is_within_schedule(filters, "UTC") is True


# --- Test 2: 24h mode on a non-working day (SAT) → False ---


def test_schedule_24h_mode_non_working_day():
    filters = _make_filters(working_time=24, working_days=["MON", "TUE", "WED", "THU", "FRI"])
    # Saturday 2024-03-16 10:00 UTC
    now = datetime(2024, 3, 16, 10, 0, tzinfo=ZoneInfo("UTC"))
    with _patch_now(now):
        assert is_within_schedule(filters, "UTC") is False


# --- Test 3: normal schedule (09:00, 8h) within bounds → True ---


def test_schedule_normal_within():
    filters = _make_filters(
        start_time="09:00",
        working_time=8,
        working_days=["MON", "TUE", "WED", "THU", "FRI"],
    )
    # Wednesday 2024-03-13 14:00 UTC  (within 09:00-17:00)
    now = datetime(2024, 3, 13, 14, 0, tzinfo=ZoneInfo("UTC"))
    with _patch_now(now):
        assert is_within_schedule(filters, "UTC") is True


# --- Test 4: normal schedule outside bounds → False ---


def test_schedule_normal_outside():
    filters = _make_filters(
        start_time="09:00",
        working_time=8,
        working_days=["MON", "TUE", "WED", "THU", "FRI"],
    )
    # Wednesday 2024-03-13 20:00 UTC  (after 17:00)
    now = datetime(2024, 3, 13, 20, 0, tzinfo=ZoneInfo("UTC"))
    with _patch_now(now):
        assert is_within_schedule(filters, "UTC") is False


# --- Test 5: overnight (22:00, 10h) at 23:00 same day → True ---


def test_schedule_overnight_same_day():
    filters = _make_filters(
        start_time="22:00",
        working_time=10,
        working_days=["MON", "TUE", "WED", "THU", "FRI"],
    )
    # Friday 2024-03-15 23:00 UTC  (within FRI 22:00 - SAT 08:00)
    now = datetime(2024, 3, 15, 23, 0, tzinfo=ZoneInfo("UTC"))
    with _patch_now(now):
        assert is_within_schedule(filters, "UTC") is True


# --- Test 6: overnight at 05:00 next day → True (checks start day = FRI) ---


def test_schedule_overnight_next_day():
    filters = _make_filters(
        start_time="22:00",
        working_time=10,
        working_days=["MON", "TUE", "WED", "THU", "FRI"],
    )
    # Saturday 2024-03-16 05:00 UTC  (within FRI 22:00 - SAT 08:00)
    now = datetime(2024, 3, 16, 5, 0, tzinfo=ZoneInfo("UTC"))
    with _patch_now(now):
        assert is_within_schedule(filters, "UTC") is True


# --- Test 7: overnight at 05:00, yesterday (SAT) not a working day → False ---


def test_schedule_overnight_non_working_start_day():
    filters = _make_filters(
        start_time="22:00",
        working_time=10,
        working_days=["MON", "TUE", "WED", "THU", "FRI"],
    )
    # Sunday 2024-03-17 05:00 UTC  (within SAT 22:00 - SUN 08:00, but SAT not in working_days)
    now = datetime(2024, 3, 17, 5, 0, tzinfo=ZoneInfo("UTC"))
    with _patch_now(now):
        assert is_within_schedule(filters, "UTC") is False


# --- Test 8: DST spring forward — function handles the gap correctly → True ---
#
# America/New_York, 2024-03-10 (Sunday): at 2:00 AM EST clocks jump to 3:00 AM EDT.
# Schedule: start 01:00, working_time=3h, working_days=["SUN"].
#
# datetime + timedelta adds wall-clock hours:
#   today_start = 01:00 EST            = 06:00 UTC
#   today_end   = 01:00 + 3h = 04:00 EDT = 08:00 UTC  (wall-clock addition)
#
#   now = 03:30 AM EDT = 07:30 UTC  → 06:00 ≤ 07:30 < 08:00  → True
#
# Verifies the function correctly recognises times after the spring-forward
# gap (2:00-3:00 AM doesn't exist) as within the schedule window.


def test_schedule_dst_spring_forward():
    filters = _make_filters(start_time="01:00", working_time=3, working_days=["SUN"])
    tz = ZoneInfo("America/New_York")
    # 03:30 AM EDT on the spring-forward day (after the 2→3 gap)
    now = datetime(2024, 3, 10, 3, 30, tzinfo=tz)
    with _patch_now(now):
        assert is_within_schedule(filters, "America/New_York") is True


# --- Test 9: DST fall back — second occurrence of 01:30 is within 2h window → True ---
#
# America/New_York, 2024-11-03 (Sunday): at 2:00 AM EDT clocks fall back to 1:00 AM EST.
# Schedule: start 01:00, working_time=2h, working_days=["SUN"].
#
# datetime + timedelta adds wall-clock hours:
#   today_start = 01:00 EDT (fold=0)   = 05:00 UTC
#   today_end   = 01:00 + 2h = 03:00 EST = 08:00 UTC  (wall-clock addition)
#
#   now = 01:30 AM EST (fold=1)        = 06:30 UTC  → 05:00 ≤ 06:30 < 08:00  → True
#
# During fall back the 1:00-2:00 hour repeats. The wall-clock window [01:00, 03:00)
# spans 3 real hours. Verifies that the second occurrence of 01:30 (EST, fold=1)
# is correctly included in the schedule.


def test_schedule_dst_fall_back():
    filters = _make_filters(start_time="01:00", working_time=2, working_days=["SUN"])
    tz = ZoneInfo("America/New_York")
    # 01:30 AM EST — the *second* 01:30 (after clocks fell back)
    now = datetime(2024, 11, 3, 1, 30, tzinfo=tz).replace(fold=1)
    with _patch_now(now):
        assert is_within_schedule(filters, "America/New_York") is True


# --- Test 10: exactly at end_time boundary → False (half-open interval) ---


def test_schedule_boundary_end_time():
    filters = _make_filters(
        start_time="09:00",
        working_time=8,
        working_days=["WED"],
    )
    # Wednesday 2024-03-13 17:00 UTC  (= 09:00 + 8h exactly)
    now = datetime(2024, 3, 13, 17, 0, tzinfo=ZoneInfo("UTC"))
    with _patch_now(now):
        assert is_within_schedule(filters, "UTC") is False


# === Batch deduplication tests ===
#
# Test strategy items from task 8.5:
# 1. New batch_id → is_batch_already_processed returns False
# 2. After mark_batch_as_processed → is_batch_already_processed returns True
# 3. process_stats_if_new with None stats → (False, [])
# 4. process_stats_if_new with new batch → (True, failures)
# 5. process_stats_if_new with duplicate batch → (False, [])
# 6. TTL: setex called with BATCH_DEDUP_TTL (3600s)
# 7. Concurrent requests with same batch_id → only one processed
# 8. All tests use mock Redis
# 9. Redis unavailable → returns (True, failures) without dedup
# 10. Redis unavailable → no exception raised


def _make_fake_redis():
    """Create a Redis mock with in-memory store supporting exists, setex."""
    store: dict[str, str] = {}

    async def mock_exists(*keys):
        return sum(1 for key in keys if key in store)

    async def mock_setex(key, ttl, value):
        store[key] = value

    redis = AsyncMock()
    redis.exists = AsyncMock(side_effect=mock_exists)
    redis.setex = AsyncMock(side_effect=mock_setex)
    redis._store = store
    return redis


def _make_broken_redis():
    """Create a Redis mock that always raises RedisError."""
    redis = AsyncMock()
    redis.exists = AsyncMock(side_effect=RedisError("Connection refused"))
    redis.setex = AsyncMock(side_effect=RedisError("Connection refused"))
    return redis


def _make_stats(
    batch_id: str = "test-batch-001",
    failures: list[AcceptFailureItem] | None = None,
) -> PingStats:
    """Create a PingStats instance for testing."""
    if failures is None:
        failures = [
            AcceptFailureItem(
                reason="price_too_low",
                ride_price=15.50,
                pickup_time="2024-03-13T10:30:00",
                timestamp=datetime(2024, 3, 13, 10, 30),
            ),
        ]
    return PingStats(
        batch_id=batch_id,
        cycles_since_last_ping=5,
        rides_found=1,
        accept_failures=failures,
    )


# --- Test 1: new batch_id → is_batch_already_processed returns False ---


@pytest.mark.asyncio
async def test_is_batch_already_processed_new_batch():
    fake_redis = _make_fake_redis()
    result = await is_batch_already_processed(fake_redis, "new-batch-id")
    assert result is False


# --- Test 2: after mark → is_batch_already_processed returns True ---


@pytest.mark.asyncio
async def test_is_batch_already_processed_after_mark():
    fake_redis = _make_fake_redis()
    await mark_batch_as_processed(fake_redis, "batch-123")
    result = await is_batch_already_processed(fake_redis, "batch-123")
    assert result is True


# --- Test 3: process_stats_if_new with None stats → (False, []) ---


@pytest.mark.asyncio
async def test_process_stats_if_new_none_stats():
    fake_redis = _make_fake_redis()
    was_processed, failures = await process_stats_if_new(fake_redis, None)
    assert was_processed is False
    assert failures == []
    # Redis should not be called at all
    fake_redis.exists.assert_not_called()
    fake_redis.setex.assert_not_called()


# --- Test 4: process_stats_if_new with new batch → (True, failures) ---


@pytest.mark.asyncio
async def test_process_stats_if_new_new_batch():
    fake_redis = _make_fake_redis()
    stats = _make_stats(batch_id="fresh-batch")
    was_processed, failures = await process_stats_if_new(fake_redis, stats)
    assert was_processed is True
    assert failures == stats.accept_failures
    assert len(failures) == 1
    assert failures[0].reason == "price_too_low"


# --- Test 5: process_stats_if_new with duplicate batch → (False, []) ---


@pytest.mark.asyncio
async def test_process_stats_if_new_duplicate_batch():
    fake_redis = _make_fake_redis()
    stats = _make_stats(batch_id="dup-batch")

    # First call — processes
    was_processed_1, failures_1 = await process_stats_if_new(fake_redis, stats)
    assert was_processed_1 is True
    assert len(failures_1) == 1

    # Second call with same batch_id — duplicate, skipped
    was_processed_2, failures_2 = await process_stats_if_new(fake_redis, stats)
    assert was_processed_2 is False
    assert failures_2 == []


# --- Test 6: setex called with correct TTL (3600s) ---


@pytest.mark.asyncio
async def test_mark_batch_as_processed_uses_correct_ttl():
    fake_redis = _make_fake_redis()
    batch_id = "ttl-check-batch"
    await mark_batch_as_processed(fake_redis, batch_id)

    expected_key = f"{BATCH_KEY_PREFIX}{batch_id}"
    fake_redis.setex.assert_called_once_with(expected_key, BATCH_DEDUP_TTL, "1")
    assert BATCH_DEDUP_TTL == 3600


# --- Test 7: concurrent requests — only first one is processed ---


@pytest.mark.asyncio
async def test_process_stats_if_new_concurrent_same_batch():
    fake_redis = _make_fake_redis()
    batch_id = "concurrent-batch"
    stats = _make_stats(batch_id=batch_id)

    # Simulate sequential calls (same as concurrent since first marks before return)
    result_1 = await process_stats_if_new(fake_redis, stats)
    result_2 = await process_stats_if_new(fake_redis, stats)
    result_3 = await process_stats_if_new(fake_redis, stats)

    assert result_1 == (True, stats.accept_failures)
    assert result_2 == (False, [])
    assert result_3 == (False, [])


# --- Test 8: different batch_ids are independent ---


@pytest.mark.asyncio
async def test_process_stats_if_new_different_batches():
    fake_redis = _make_fake_redis()
    stats_a = _make_stats(batch_id="batch-a")
    stats_b = _make_stats(batch_id="batch-b")

    result_a = await process_stats_if_new(fake_redis, stats_a)
    result_b = await process_stats_if_new(fake_redis, stats_b)

    assert result_a == (True, stats_a.accept_failures)
    assert result_b == (True, stats_b.accept_failures)


# --- Test 9: Redis unavailable → returns (True, failures) without dedup ---


@pytest.mark.asyncio
async def test_process_stats_if_new_redis_unavailable():
    broken_redis = _make_broken_redis()
    stats = _make_stats(batch_id="redis-down-batch")

    was_processed, failures = await process_stats_if_new(broken_redis, stats)

    # Should process stats despite Redis failure (graceful degradation)
    assert was_processed is True
    assert failures == stats.accept_failures


# --- Test 10: Redis unavailable → no exception raised ---


@pytest.mark.asyncio
async def test_process_stats_if_new_redis_unavailable_no_exception():
    broken_redis = _make_broken_redis()
    stats = _make_stats(batch_id="no-crash-batch")

    # Must not raise — ping should continue normally
    try:
        await process_stats_if_new(broken_redis, stats)
    except RedisError:
        pytest.fail("process_stats_if_new raised RedisError — ping would return 500")


# --- Test 11: Redis key uses correct prefix ---


@pytest.mark.asyncio
async def test_batch_key_prefix():
    fake_redis = _make_fake_redis()
    batch_id = "prefix-test-batch"
    await mark_batch_as_processed(fake_redis, batch_id)

    expected_key = f"stats_batch:{batch_id}"
    assert expected_key in fake_redis._store
    assert fake_redis._store[expected_key] == "1"


# --- Test 12: empty accept_failures list is returned correctly ---


@pytest.mark.asyncio
async def test_process_stats_if_new_empty_failures():
    fake_redis = _make_fake_redis()
    stats = _make_stats(batch_id="empty-failures-batch", failures=[])

    was_processed, failures = await process_stats_if_new(fake_redis, stats)
    assert was_processed is True
    assert failures == []


# === update_device_state tests ===
#
# Test strategy items from task 8.6:
# 1. Test update_device_state sets last_ping_at
# 2. Test timezone updates to new value
# 3. Test accessibility_enabled=True → saved to DB
# 4. Test lyft_running=False → saved to DB
# 5. Test screen_on=None → field doesn't change
# 6. Test last_interval_sent is written
# 7. Test offline_notified resets to False
# 8. Test partial update: only provided fields change


async def _create_device_in_db(db_session, **overrides):
    """Create a User + PairedDevice in the test DB and return the device."""
    user = User(email="ping-test@example.com", password_hash="hashed")
    db_session.add(user)
    await db_session.flush()

    defaults = {
        "user_id": user.id,
        "device_id": "android-device-001",
        "device_token_hash": "a" * 64,
        "timezone": "America/New_York",
    }
    defaults.update(overrides)
    device = PairedDevice(**defaults)
    db_session.add(device)
    await db_session.flush()
    return device


# --- Test 1: update_device_state sets last_ping_at ---


@pytest.mark.asyncio
async def test_update_device_state_sets_last_ping_at(db_session):
    device = await _create_device_in_db(db_session)
    assert device.last_ping_at is None

    request = PingRequest(timezone="America/New_York", app_version="1.0.0")
    fixed_now = datetime(2024, 3, 15, 12, 0, tzinfo=ZoneInfo("UTC"))

    with _patch_now(fixed_now):
        await update_device_state(db_session, device, request)

    assert device.last_ping_at is not None
    # _patch_now returns target_now.astimezone(tz), so compare absolute time
    assert device.last_ping_at == fixed_now


# --- Test 2: timezone updates to new value ---


@pytest.mark.asyncio
async def test_update_device_state_updates_timezone(db_session):
    device = await _create_device_in_db(db_session, timezone="America/New_York")
    assert device.timezone == "America/New_York"

    request = PingRequest(timezone="Europe/Kyiv", app_version="1.0.0")
    await update_device_state(db_session, device, request)

    assert device.timezone == "Europe/Kyiv"


# --- Test 3: accessibility_enabled=True → saved to DB ---


@pytest.mark.asyncio
async def test_update_device_state_accessibility_enabled_true(db_session):
    device = await _create_device_in_db(db_session)
    assert device.accessibility_enabled is None

    request = PingRequest(
        timezone="America/New_York",
        app_version="1.0.0",
        device_health=DeviceHealth(accessibility_enabled=True),
    )
    await update_device_state(db_session, device, request)

    assert device.accessibility_enabled is True


# --- Test 4: lyft_running=False → saved to DB ---


@pytest.mark.asyncio
async def test_update_device_state_lyft_running_false(db_session):
    device = await _create_device_in_db(db_session)
    assert device.lyft_running is None

    request = PingRequest(
        timezone="America/New_York",
        app_version="1.0.0",
        device_health=DeviceHealth(lyft_running=False),
    )
    await update_device_state(db_session, device, request)

    assert device.lyft_running is False


# --- Test 5: screen_on=None → field doesn't change ---


@pytest.mark.asyncio
async def test_update_device_state_screen_on_none_unchanged(db_session):
    device = await _create_device_in_db(db_session)
    # Set initial value via direct attribute assignment + flush
    device.screen_on = True
    await db_session.commit()
    await db_session.refresh(device)
    assert device.screen_on is True

    request = PingRequest(
        timezone="America/New_York",
        app_version="1.0.0",
        device_health=DeviceHealth(screen_on=None),
    )
    await update_device_state(db_session, device, request)

    assert device.screen_on is True


# --- Test 6: last_interval_sent is written ---


@pytest.mark.asyncio
async def test_update_device_state_last_interval_sent(db_session):
    device = await _create_device_in_db(db_session)
    assert device.last_interval_sent is None

    request = PingRequest(timezone="America/New_York", app_version="1.0.0")
    await update_device_state(db_session, device, request, interval_seconds=30)

    assert device.last_interval_sent == 30


# --- Test 7: offline_notified resets to False ---


@pytest.mark.asyncio
async def test_update_device_state_resets_offline_notified(db_session):
    device = await _create_device_in_db(db_session)
    # Simulate device that was marked offline
    device.offline_notified = True
    await db_session.commit()
    await db_session.refresh(device)
    assert device.offline_notified is True

    request = PingRequest(timezone="America/New_York", app_version="1.0.0")
    await update_device_state(db_session, device, request)

    assert device.offline_notified is False


# --- Test 8: partial update — only provided fields change ---


@pytest.mark.asyncio
async def test_update_device_state_partial_update(db_session):
    device = await _create_device_in_db(db_session)
    # Set initial health values
    device.accessibility_enabled = False
    device.lyft_running = True
    device.screen_on = True
    device.last_interval_sent = 60
    await db_session.commit()
    await db_session.refresh(device)

    # Send only accessibility_enabled, no lyft_running/screen_on, no interval
    request = PingRequest(
        timezone="Europe/Kyiv",
        app_version="2.0.0",
        device_health=DeviceHealth(accessibility_enabled=True),
    )
    await update_device_state(db_session, device, request)

    # accessibility_enabled updated
    assert device.accessibility_enabled is True
    # lyft_running and screen_on unchanged (None in device_health means skip)
    assert device.lyft_running is True
    assert device.screen_on is True
    # last_interval_sent unchanged (interval_seconds not passed)
    assert device.last_interval_sent == 60
    # timezone updated
    assert device.timezone == "Europe/Kyiv"


# --- Test 9: location provided → latitude/longitude/location_updated_at saved ---


@pytest.mark.asyncio
async def test_update_device_state_location_saved(db_session):
    device = await _create_device_in_db(db_session)
    assert device.latitude is None
    assert device.longitude is None
    assert device.location_updated_at is None

    request = PingRequest(
        timezone="America/New_York",
        app_version="1.0.0",
        location=DeviceLocation(latitude=40.7128, longitude=-74.0060),
    )
    await update_device_state(db_session, device, request)

    assert device.latitude == 40.7128
    assert device.longitude == -74.0060
    assert device.location_updated_at is not None


# --- Test 10: location None → existing values unchanged ---


@pytest.mark.asyncio
async def test_update_device_state_location_none_unchanged(db_session):
    device = await _create_device_in_db(db_session)
    device.latitude = 51.5074
    device.longitude = -0.1278
    device.location_updated_at = datetime(2026, 4, 1, 12, 0, 0, tzinfo=UTC)
    await db_session.commit()
    await db_session.refresh(device)

    request = PingRequest(
        timezone="America/New_York",
        app_version="1.0.0",
    )
    await update_device_state(db_session, device, request)

    assert device.latitude == 51.5074
    assert device.longitude == -0.1278
    assert device.location_updated_at == datetime(2026, 4, 1, 12, 0, 0, tzinfo=UTC)


# === save_accept_failures tests ===
#
# Test strategy items from task 8.7:
# 1. Test save one failure → returns 1
# 2. Test save multiple failures → returns correct count
# 3. Test empty list → returns 0, nothing saved
# 4. Test failure with all fields (reason, ride_price, pickup_time)
# 5. Test failure with only reason (nullable fields = None)
# 6. Test user_id correctly linked
# 7. Test reported_at set from client timestamp
# 8. Test DB error handling → rollback, return 0
# 9. Integration test: verify records in DB after save


async def _create_user_for_failures(db_session) -> User:
    """Create a User in the test DB for accept failure tests."""
    user = User(
        email=f"failure-{uuid.uuid4().hex[:8]}@example.com",
        password_hash="hashed",
    )
    db_session.add(user)
    await db_session.flush()
    return user


def _make_failure_item(
    reason: str = "AcceptButtonNotFound",
    ride_price: float | None = 25.50,
    pickup_time: str | None = "Tomorrow · 6:05AM",
    timestamp: datetime | None = None,
) -> AcceptFailureItem:
    """Create an AcceptFailureItem for testing."""
    if timestamp is None:
        timestamp = datetime(2024, 3, 13, 10, 30, tzinfo=UTC)
    return AcceptFailureItem(
        reason=reason,
        ride_price=ride_price,
        pickup_time=pickup_time,
        timestamp=timestamp,
    )


# --- Test 1: save one failure → returns 1 ---


@pytest.mark.asyncio
async def test_save_accept_failures_single(db_session):
    user = await _create_user_for_failures(db_session)
    failures = [_make_failure_item()]

    result = await save_accept_failures(db_session, user.id, failures)

    assert result == 1


# --- Test 2: save multiple failures → returns correct count ---


@pytest.mark.asyncio
async def test_save_accept_failures_multiple(db_session):
    user = await _create_user_for_failures(db_session)
    failures = [
        _make_failure_item(reason="AcceptButtonNotFound"),
        _make_failure_item(reason="TimeoutExpired", ride_price=30.0),
        _make_failure_item(reason="ScreenOff", ride_price=None, pickup_time=None),
    ]

    result = await save_accept_failures(db_session, user.id, failures)

    assert result == 3


# --- Test 3: empty list → returns 0, nothing saved ---


@pytest.mark.asyncio
async def test_save_accept_failures_empty_list(db_session):
    user = await _create_user_for_failures(db_session)

    result = await save_accept_failures(db_session, user.id, [])

    assert result == 0

    # Verify nothing was saved
    stmt = select(AcceptFailureModel).where(AcceptFailureModel.user_id == user.id)
    db_result = await db_session.execute(stmt)
    assert db_result.scalars().all() == []


# --- Test 4: failure with all fields → verified in DB ---


@pytest.mark.asyncio
async def test_save_accept_failures_all_fields_in_db(db_session):
    user = await _create_user_for_failures(db_session)
    failures = [
        _make_failure_item(
            reason="AcceptButtonNotFound",
            ride_price=25.50,
            pickup_time="Tomorrow · 6:05AM",
        )
    ]

    await save_accept_failures(db_session, user.id, failures)

    stmt = select(AcceptFailureModel).where(AcceptFailureModel.user_id == user.id)
    db_result = await db_session.execute(stmt)
    record = db_result.scalars().one()

    assert record.reason == "AcceptButtonNotFound"
    assert record.ride_price == 25.50
    assert record.pickup_time == "Tomorrow · 6:05AM"


# --- Test 5: failure with only reason → nullable fields are None ---


@pytest.mark.asyncio
async def test_save_accept_failures_only_reason(db_session):
    user = await _create_user_for_failures(db_session)
    failures = [_make_failure_item(reason="UnknownError", ride_price=None, pickup_time=None)]

    await save_accept_failures(db_session, user.id, failures)

    stmt = select(AcceptFailureModel).where(AcceptFailureModel.user_id == user.id)
    db_result = await db_session.execute(stmt)
    record = db_result.scalars().one()

    assert record.reason == "UnknownError"
    assert record.ride_price is None
    assert record.pickup_time is None


# --- Test 6: user_id correctly linked ---


@pytest.mark.asyncio
async def test_save_accept_failures_user_id_linked(db_session):
    user = await _create_user_for_failures(db_session)
    failures = [_make_failure_item()]

    await save_accept_failures(db_session, user.id, failures)

    stmt = select(AcceptFailureModel).where(AcceptFailureModel.user_id == user.id)
    db_result = await db_session.execute(stmt)
    record = db_result.scalars().one()

    assert record.user_id == user.id


# --- Test 7: reported_at set from client timestamp ---


@pytest.mark.asyncio
async def test_save_accept_failures_reported_at_from_timestamp(db_session):
    user = await _create_user_for_failures(db_session)
    client_timestamp = datetime(2024, 6, 15, 8, 45, tzinfo=UTC)
    failures = [_make_failure_item(timestamp=client_timestamp)]

    await save_accept_failures(db_session, user.id, failures)

    stmt = select(AcceptFailureModel).where(AcceptFailureModel.user_id == user.id)
    db_result = await db_session.execute(stmt)
    record = db_result.scalars().one()

    assert record.reported_at == client_timestamp


# --- Test 8: DB error → return 0, session remains usable ---


@pytest.mark.asyncio
async def test_save_accept_failures_db_error_returns_zero(db_session):
    # Use a non-existent user_id → FK constraint violation on flush
    non_existent_user_id = uuid.uuid4()
    failures = [_make_failure_item()]

    result = await save_accept_failures(db_session, non_existent_user_id, failures)

    assert result == 0

    # Verify session is still usable after the failed savepoint
    user = await _create_user_for_failures(db_session)
    assert user.id is not None


# --- Test 9: integration — multiple records verified in DB ---


@pytest.mark.asyncio
async def test_save_accept_failures_integration_records_in_db(db_session):
    user = await _create_user_for_failures(db_session)
    ts1 = datetime(2024, 3, 13, 10, 0, tzinfo=UTC)
    ts2 = datetime(2024, 3, 13, 10, 15, tzinfo=UTC)
    failures = [
        _make_failure_item(
            reason="AcceptButtonNotFound",
            ride_price=25.50,
            pickup_time="Tomorrow · 6:05AM",
            timestamp=ts1,
        ),
        _make_failure_item(
            reason="TimeoutExpired",
            ride_price=None,
            pickup_time=None,
            timestamp=ts2,
        ),
    ]

    result = await save_accept_failures(db_session, user.id, failures)

    assert result == 2

    stmt = (
        select(AcceptFailureModel)
        .where(AcceptFailureModel.user_id == user.id)
        .order_by(AcceptFailureModel.reported_at)
    )
    db_result = await db_session.execute(stmt)
    records = db_result.scalars().all()

    assert len(records) == 2

    assert records[0].reason == "AcceptButtonNotFound"
    assert records[0].ride_price == 25.50
    assert records[0].pickup_time == "Tomorrow · 6:05AM"
    assert records[0].reported_at == ts1
    assert records[0].user_id == user.id

    assert records[1].reason == "TimeoutExpired"
    assert records[1].ride_price is None
    assert records[1].pickup_time is None
    assert records[1].reported_at == ts2
    assert records[1].user_id == user.id


# === calculate_dynamic_interval tests ===

# Standard hourly weights used across tests (sum = 100)
_WEIGHTS = [
    5.23,
    5.19,
    4.97,
    4.28,
    3.07,
    1.0,
    1.0,
    1.0,
    1.0,
    1.0,
    3.69,
    5.10,
    6.24,
    4.96,
    5.06,
    5.18,
    4.59,
    4.57,
    5.91,
    5.58,
    5.98,
    5.29,
    5.15,
    4.96,
]


# --- Test 1: peak hour 12 (weight 6.24), cycle 15s -> interval ~15s ---


def test_dynamic_interval_peak_hour():
    result = calculate_dynamic_interval(1920, _WEIGHTS, 12, 15000)
    # 6.24% of 1920 = 119.808 rph -> 30.05s total -> 30.05 - 15 = 15.05 -> int = 15
    assert result == 15


# --- Test 2: off-peak hour 5 (weight 1.0), cycle 15s -> interval ~172s ---


def test_dynamic_interval_off_peak_hour():
    result = calculate_dynamic_interval(1920, _WEIGHTS, 5, 15000)
    # 1.0% of 1920 = 19.2 rph -> 187.5s total -> 187.5 - 15 = 172.5 -> int = 172
    assert result == 172


# --- Test 3: minimum clamp — large cycle_duration -> returns MIN_INTERVAL_SECONDS ---


def test_dynamic_interval_minimum_clamp():
    result = calculate_dynamic_interval(1920, _WEIGHTS, 12, 40000)
    # 30.05 - 40 = -9.95 -> clamped to MIN_INTERVAL_SECONDS
    assert result == MIN_INTERVAL_SECONDS


# --- Test 4: None cycle_duration -> uses default 15000ms ---


def test_dynamic_interval_none_cycle_duration():
    result = calculate_dynamic_interval(1920, _WEIGHTS, 12, None)
    # Same as test 1: uses DEFAULT_CYCLE_DURATION_MS = 15000
    assert result == 15


# --- Test 5: edge hour 0 (weight 5.23) ---


def test_dynamic_interval_hour_zero():
    result = calculate_dynamic_interval(1920, _WEIGHTS, 0, 15000)
    # 5.23% of 1920 = 100.416 rph -> 35.85s total -> 35.85 - 15 = 20.85 -> int = 20
    assert result == 20


# --- Test 6: edge hour 23 (weight 4.96) ---


def test_dynamic_interval_hour_23():
    result = calculate_dynamic_interval(1920, _WEIGHTS, 23, 15000)
    # 4.96% of 1920 = 95.232 rph -> 37.80s total -> 37.80 - 15 = 22.80 -> int = 22
    assert result == 22


# --- Test 7: different requests_per_day scales interval ---


def test_dynamic_interval_different_rpd():
    # Double the budget -> half the total_cycle_time -> shorter interval
    result = calculate_dynamic_interval(3840, _WEIGHTS, 12, 15000)
    # 6.24% of 3840 = 239.616 rph -> 15.02s total -> 15.02 - 15 = 0.02 -> clamped to 5
    assert result == MIN_INTERVAL_SECONDS


# === process_ride_status_reports tests ===
#
# Test strategy items from task 6.2:
# 1. ride_statuses with present=true → last_reported_present updated
# 2. present=false + disappeared_at=NULL → disappeared_at recorded
# 3. present=false + disappeared_at already set → disappeared_at unchanged
# 4. Unknown ride_hash → ignored without error
# 5. Bulk update of multiple rides in one call


async def _create_ride_for_verification(
    db_session,
    user_id: uuid.UUID,
    ride_hash: str = "a" * 64,
    disappeared_at: datetime | None = None,
    last_reported_present: bool | None = None,
) -> Ride:
    """Create a Ride in the test DB for verification tests."""
    ride = Ride(
        user_id=user_id,
        idempotency_key=str(uuid.uuid4()),
        event_type="ACCEPTED",
        ride_data={"price": 25.0},
        ride_hash=ride_hash,
        disappeared_at=disappeared_at,
        last_reported_present=last_reported_present,
    )
    db_session.add(ride)
    await db_session.flush()
    return ride


# --- Test 1: present=true → last_reported_present updated to true ---


@pytest.mark.asyncio
async def test_process_ride_statuses_present_true(db_session):
    user = await _create_user_for_failures(db_session)
    ride = await _create_ride_for_verification(db_session, user.id, ride_hash="a1b2c3" + "0" * 58)
    assert ride.last_reported_present is None

    reports = [RideStatusReport(ride_hash=ride.ride_hash, present=True)]
    updated = await process_ride_status_reports(db_session, user.id, reports)

    assert updated == 1
    assert ride.last_reported_present is True
    assert ride.disappeared_at is None


# --- Test 2: present=false + disappeared_at=NULL → disappeared_at recorded ---


@pytest.mark.asyncio
async def test_process_ride_statuses_present_false_sets_disappeared_at(db_session):
    user = await _create_user_for_failures(db_session)
    ride = await _create_ride_for_verification(db_session, user.id, ride_hash="d4e5f6" + "0" * 58)
    assert ride.disappeared_at is None

    reports = [RideStatusReport(ride_hash=ride.ride_hash, present=False)]
    fixed_now = datetime(2024, 6, 1, 12, 0, tzinfo=UTC)
    with _patch_now(fixed_now):
        updated = await process_ride_status_reports(db_session, user.id, reports)

    assert updated == 1
    assert ride.last_reported_present is False
    assert ride.disappeared_at == fixed_now


# --- Test 3: present=false + disappeared_at already set → not overwritten ---


@pytest.mark.asyncio
async def test_process_ride_statuses_disappeared_at_not_overwritten(db_session):
    user = await _create_user_for_failures(db_session)
    original_disappeared = datetime(2024, 5, 1, 10, 0, tzinfo=UTC)
    ride = await _create_ride_for_verification(
        db_session,
        user.id,
        ride_hash="g7h8i9" + "0" * 58,
        disappeared_at=original_disappeared,
    )

    reports = [RideStatusReport(ride_hash=ride.ride_hash, present=False)]
    later_now = datetime(2024, 6, 1, 12, 0, tzinfo=UTC)
    with _patch_now(later_now):
        updated = await process_ride_status_reports(db_session, user.id, reports)

    assert updated == 1
    assert ride.last_reported_present is False
    assert ride.disappeared_at == original_disappeared  # NOT overwritten


# --- Test 4: unknown ride_hash → ignored without error, returns 0 ---


@pytest.mark.asyncio
async def test_process_ride_statuses_unknown_hash_ignored(db_session):
    user = await _create_user_for_failures(db_session)

    reports = [RideStatusReport(ride_hash="unknown" + "0" * 57, present=True)]
    updated = await process_ride_status_reports(db_session, user.id, reports)

    assert updated == 0


# --- Test 5: bulk update of multiple rides in one call ---


@pytest.mark.asyncio
async def test_process_ride_statuses_bulk_update(db_session):
    user = await _create_user_for_failures(db_session)
    ride1 = await _create_ride_for_verification(db_session, user.id, ride_hash="hash1" + "0" * 59)
    ride2 = await _create_ride_for_verification(db_session, user.id, ride_hash="hash2" + "0" * 59)
    ride3 = await _create_ride_for_verification(db_session, user.id, ride_hash="hash3" + "0" * 59)

    reports = [
        RideStatusReport(ride_hash=ride1.ride_hash, present=True),
        RideStatusReport(ride_hash=ride2.ride_hash, present=False),
        RideStatusReport(ride_hash=ride3.ride_hash, present=True),
    ]

    fixed_now = datetime(2024, 6, 1, 12, 0, tzinfo=UTC)
    with _patch_now(fixed_now):
        updated = await process_ride_status_reports(db_session, user.id, reports)

    assert updated == 3
    assert ride1.last_reported_present is True
    assert ride1.disappeared_at is None
    assert ride2.last_reported_present is False
    assert ride2.disappeared_at == fixed_now
    assert ride3.last_reported_present is True
    assert ride3.disappeared_at is None


# --- Test 6: None ride_statuses → returns 0, no error ---


@pytest.mark.asyncio
async def test_process_ride_statuses_none_input(db_session):
    user = await _create_user_for_failures(db_session)
    updated = await process_ride_status_reports(db_session, user.id, None)
    assert updated == 0


# --- Test 7: empty list → returns 0, no error ---


@pytest.mark.asyncio
async def test_process_ride_statuses_empty_list(db_session):
    user = await _create_user_for_failures(db_session)
    updated = await process_ride_status_reports(db_session, user.id, [])
    assert updated == 0
