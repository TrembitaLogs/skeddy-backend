"""Tests for pickup_time parsing and verification_deadline calculation.

Test strategy from task 5.2:
1. Parse pickup_time with 'America/New_York' → correct UTC
2. verification_deadline = pickup_time - 30 min (default)
3. verification_deadline = now() when pickup_time is too close
4. Fallback to UTC on invalid timezone + warning log with ride_id and received_timezone
5. ride_hash and verification_status='PENDING' saved correctly
"""

from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from freezegun import freeze_time

from app.services.ride_service import (
    _parse_time_part,
    _resolve_date,
    calculate_verification_deadline,
    parse_pickup_time,
)

# ===========================================================================
# _parse_time_part
# ===========================================================================


class TestParseTimePart:
    def test_morning_am(self):
        assert _parse_time_part("6:05AM") == (6, 5)

    def test_afternoon_pm(self):
        assert _parse_time_part("3:30PM") == (15, 30)

    def test_noon(self):
        assert _parse_time_part("12:00PM") == (12, 0)

    def test_midnight(self):
        assert _parse_time_part("12:00AM") == (0, 0)

    def test_lowercase(self):
        assert _parse_time_part("9:15am") == (9, 15)

    def test_with_space_before_ampm(self):
        assert _parse_time_part("6:05 AM") == (6, 5)

    def test_invalid_format(self):
        assert _parse_time_part("25:00") is None

    def test_invalid_hour_13(self):
        assert _parse_time_part("13:00AM") is None

    def test_invalid_minute_60(self):
        assert _parse_time_part("6:60AM") is None

    def test_empty_string(self):
        assert _parse_time_part("") is None


# ===========================================================================
# _resolve_date
# ===========================================================================


class TestResolveDate:
    @freeze_time("2026-02-24 10:00:00", tz_offset=0)
    def test_today(self):
        tz = ZoneInfo("UTC")
        now = datetime.now(tz)
        result = _resolve_date("Today", now)
        assert result == now.date()

    @freeze_time("2026-02-24 10:00:00", tz_offset=0)
    def test_tomorrow(self):
        tz = ZoneInfo("UTC")
        now = datetime.now(tz)
        result = _resolve_date("Tomorrow", now)
        assert result == now.date() + timedelta(days=1)

    @freeze_time("2026-02-24 10:00:00", tz_offset=0)  # Tuesday
    def test_weekday_future(self):
        """Wednesday (tomorrow) from Tuesday → 1 day ahead."""
        tz = ZoneInfo("UTC")
        now = datetime.now(tz)
        result = _resolve_date("Wed", now)
        assert result == now.date() + timedelta(days=1)

    @freeze_time("2026-02-24 10:00:00", tz_offset=0)  # Tuesday
    def test_weekday_same_day_means_next_week(self):
        """Tuesday from Tuesday → next Tuesday (7 days)."""
        tz = ZoneInfo("UTC")
        now = datetime.now(tz)
        result = _resolve_date("Tue", now)
        assert result == now.date() + timedelta(days=7)

    @freeze_time("2026-02-24 10:00:00", tz_offset=0)  # Tuesday
    def test_weekday_past_this_week(self):
        """Monday from Tuesday → next Monday (6 days)."""
        tz = ZoneInfo("UTC")
        now = datetime.now(tz)
        result = _resolve_date("Mon", now)
        assert result == now.date() + timedelta(days=6)

    @freeze_time("2026-02-24 10:00:00", tz_offset=0)  # Tuesday
    def test_full_weekday_name(self):
        """Full name 'Wednesday' should match via first 3 chars."""
        tz = ZoneInfo("UTC")
        now = datetime.now(tz)
        result = _resolve_date("Wednesday", now)
        assert result == now.date() + timedelta(days=1)

    @freeze_time("2026-02-24 10:00:00", tz_offset=0)
    def test_month_day_future(self):
        tz = ZoneInfo("UTC")
        now = datetime.now(tz)
        result = _resolve_date("Mar 5", now)
        from datetime import date

        assert result == date(2026, 3, 5)

    @freeze_time("2026-02-24 10:00:00", tz_offset=0)
    def test_month_day_past_wraps_to_next_year(self):
        tz = ZoneInfo("UTC")
        now = datetime.now(tz)
        result = _resolve_date("Jan 15", now)
        from datetime import date

        assert result == date(2027, 1, 15)

    @freeze_time("2026-02-24 10:00:00", tz_offset=0)
    def test_invalid_date_string(self):
        tz = ZoneInfo("UTC")
        now = datetime.now(tz)
        assert _resolve_date("xyz", now) is None

    @freeze_time("2026-02-24 10:00:00", tz_offset=0)
    def test_invalid_month(self):
        tz = ZoneInfo("UTC")
        now = datetime.now(tz)
        assert _resolve_date("Foo 15", now) is None


# ===========================================================================
# parse_pickup_time
# ===========================================================================


class TestParsePickupTime:
    @freeze_time("2026-02-24 10:00:00", tz_offset=0)
    def test_tomorrow_new_york(self):
        """Test 1: Parse pickup_time with 'America/New_York' → correct UTC."""
        tz = ZoneInfo("America/New_York")
        result = parse_pickup_time("Tomorrow · 6:05AM", tz)
        assert result is not None

        # Feb 25 2026, 6:05 AM EST = 11:05 AM UTC
        expected = datetime(2026, 2, 25, 11, 5, tzinfo=ZoneInfo("UTC"))
        assert result == expected

    @freeze_time("2026-02-24 10:00:00", tz_offset=0)
    def test_today_utc(self):
        tz = ZoneInfo("UTC")
        result = parse_pickup_time("Today · 3:30PM", tz)
        assert result is not None
        expected = datetime(2026, 2, 24, 15, 30, tzinfo=ZoneInfo("UTC"))
        assert result == expected

    @freeze_time("2026-02-24 10:00:00", tz_offset=0)
    def test_weekday_format(self):
        """Wed from Tuesday → Feb 25."""
        tz = ZoneInfo("UTC")
        result = parse_pickup_time("Wed · 10:00AM", tz)
        assert result is not None
        expected = datetime(2026, 2, 25, 10, 0, tzinfo=ZoneInfo("UTC"))
        assert result == expected

    @freeze_time("2026-02-24 10:00:00", tz_offset=0)
    def test_month_day_format(self):
        tz = ZoneInfo("UTC")
        result = parse_pickup_time("Mar 5 · 2:00PM", tz)
        assert result is not None
        expected = datetime(2026, 3, 5, 14, 0, tzinfo=ZoneInfo("UTC"))
        assert result == expected

    @freeze_time("2026-02-24 10:00:00", tz_offset=0)
    def test_dash_separator(self):
        tz = ZoneInfo("UTC")
        result = parse_pickup_time("Tomorrow - 6:05AM", tz)
        assert result is not None

    @freeze_time("2026-02-24 10:00:00", tz_offset=0)
    def test_no_spaces_around_separator(self):
        tz = ZoneInfo("UTC")
        result = parse_pickup_time("Tomorrow·6:05AM", tz)
        assert result is not None

    def test_invalid_no_separator(self):
        tz = ZoneInfo("UTC")
        result = parse_pickup_time("some random text", tz)
        assert result is None

    def test_invalid_time_part(self):
        tz = ZoneInfo("UTC")
        result = parse_pickup_time("Tomorrow · 25:00", tz)
        assert result is None

    def test_invalid_date_part(self):
        tz = ZoneInfo("UTC")
        result = parse_pickup_time("xyz · 6:05AM", tz)
        assert result is None

    def test_empty_string(self):
        tz = ZoneInfo("UTC")
        assert parse_pickup_time("", tz) is None

    @freeze_time("2026-06-15 10:00:00", tz_offset=0)
    def test_dst_aware_los_angeles(self):
        """PDT (UTC-7) during summer."""
        tz = ZoneInfo("America/Los_Angeles")
        result = parse_pickup_time("Tomorrow · 9:00AM", tz)
        assert result is not None
        # Jun 16, 9:00 AM PDT = 4:00 PM UTC
        expected = datetime(2026, 6, 16, 16, 0, tzinfo=ZoneInfo("UTC"))
        assert result == expected


# ===========================================================================
# calculate_verification_deadline
# ===========================================================================


class TestCalculateVerificationDeadline:
    @freeze_time("2026-02-24 10:00:00", tz_offset=0)
    def test_normal_deadline(self):
        """Test 2: deadline = pickup_time - 30 min (default)."""
        pickup_dt = datetime(2026, 2, 25, 11, 0, tzinfo=ZoneInfo("UTC"))
        result = calculate_verification_deadline(pickup_dt, 30)
        expected = datetime(2026, 2, 25, 10, 30, tzinfo=ZoneInfo("UTC"))
        assert result == expected

    @freeze_time("2026-02-24 10:00:00", tz_offset=0)
    def test_deadline_in_past_returns_now(self):
        """Test 3: deadline = now() when pickup_time is too close."""
        # Pickup in 15 minutes, deadline_minutes=30 → deadline would be in the past
        pickup_dt = datetime(2026, 2, 24, 10, 15, tzinfo=ZoneInfo("UTC"))
        result = calculate_verification_deadline(pickup_dt, 30)
        now_utc = datetime(2026, 2, 24, 10, 0, tzinfo=UTC)
        assert result == now_utc

    @freeze_time("2026-02-24 10:00:00", tz_offset=0)
    def test_none_pickup_returns_now(self):
        """When parsing failed (None), deadline should be now."""
        result = calculate_verification_deadline(None, 30)
        now_utc = datetime(2026, 2, 24, 10, 0, tzinfo=UTC)
        assert result == now_utc

    @freeze_time("2026-02-24 10:00:00", tz_offset=0)
    def test_custom_deadline_minutes(self):
        pickup_dt = datetime(2026, 2, 25, 12, 0, tzinfo=ZoneInfo("UTC"))
        result = calculate_verification_deadline(pickup_dt, 60)
        expected = datetime(2026, 2, 25, 11, 0, tzinfo=ZoneInfo("UTC"))
        assert result == expected

    @freeze_time("2026-02-24 10:00:00", tz_offset=0)
    def test_pickup_exactly_at_deadline_returns_now(self):
        """Pickup in exactly deadline_minutes → deadline = now."""
        pickup_dt = datetime(2026, 2, 24, 10, 30, tzinfo=ZoneInfo("UTC"))
        result = calculate_verification_deadline(pickup_dt, 30)
        now_utc = datetime(2026, 2, 24, 10, 0, tzinfo=UTC)
        assert result == now_utc
