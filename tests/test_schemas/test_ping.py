from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from app.schemas.ping import (
    AcceptFailureItem,
    DeviceHealth,
    PingFiltersResponse,
    PingRequest,
    PingResponse,
    PingStats,
    RideStatusReport,
    VerifyRideItem,
)


class TestPingRequestAllFields:
    """PingRequest with all fields populated."""

    def test_all_fields(self):
        req = PingRequest(
            timezone="America/New_York",
            app_version="1.2.0",
            device_health=DeviceHealth(
                accessibility_enabled=True,
                lyft_running=True,
                screen_on=True,
            ),
            stats=PingStats(
                batch_id="550e8400-e29b-41d4-a716-446655440000",
                cycles_since_last_ping=3,
                rides_found=5,
                accept_failures=[
                    AcceptFailureItem(
                        reason="AcceptButtonNotFound",
                        ride_price=25.50,
                        pickup_time="Tomorrow · 6:05AM",
                        timestamp=datetime(2026, 2, 9, 10, 30, 0, tzinfo=UTC),
                    ),
                ],
            ),
        )
        assert req.timezone == "America/New_York"
        assert req.app_version == "1.2.0"
        assert req.device_health.accessibility_enabled is True
        assert req.device_health.lyft_running is True
        assert req.device_health.screen_on is True
        assert req.stats.batch_id == "550e8400-e29b-41d4-a716-446655440000"
        assert req.stats.cycles_since_last_ping == 3
        assert req.stats.rides_found == 5
        assert len(req.stats.accept_failures) == 1
        assert req.stats.accept_failures[0].reason == "AcceptButtonNotFound"
        assert req.stats.accept_failures[0].ride_price == 25.50

    def test_serialization_round_trip(self):
        ts = datetime(2026, 2, 9, 10, 30, 0, tzinfo=UTC)
        req = PingRequest(
            timezone="Europe/Kyiv",
            app_version="2.0.0",
            device_health=DeviceHealth(
                accessibility_enabled=False,
                lyft_running=False,
                screen_on=False,
            ),
            stats=PingStats(
                batch_id="test-batch",
                cycles_since_last_ping=1,
                rides_found=0,
                accept_failures=[
                    AcceptFailureItem(
                        reason="Timeout",
                        ride_price=None,
                        pickup_time=None,
                        timestamp=ts,
                    ),
                ],
            ),
        )
        data = req.model_dump(mode="json")
        restored = PingRequest.model_validate(data)
        assert restored.timezone == req.timezone
        assert restored.stats.batch_id == req.stats.batch_id
        assert restored.stats.accept_failures[0].reason == "Timeout"


class TestPingRequestMinimalFields:
    """PingRequest with only required fields (timezone, app_version)."""

    def test_minimal_required(self):
        req = PingRequest(timezone="UTC", app_version="1.0.0")
        assert req.timezone == "UTC"
        assert req.app_version == "1.0.0"
        assert req.device_health is None
        assert req.stats is None

    def test_missing_timezone_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            PingRequest(app_version="1.0.0")
        assert "timezone" in str(exc_info.value)

    def test_missing_app_version_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            PingRequest(timezone="UTC")
        assert "app_version" in str(exc_info.value)

    def test_empty_timezone_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            PingRequest(timezone="", app_version="1.0.0")
        assert "timezone" in str(exc_info.value)

    def test_empty_app_version_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            PingRequest(timezone="UTC", app_version="")
        assert "app_version" in str(exc_info.value)


class TestPingStats:
    """PingStats with batch_id and accept_failures."""

    def test_valid_stats(self):
        stats = PingStats(
            batch_id="550e8400-e29b-41d4-a716-446655440000",
            cycles_since_last_ping=1,
            rides_found=0,
        )
        assert stats.batch_id == "550e8400-e29b-41d4-a716-446655440000"
        assert stats.cycles_since_last_ping == 1
        assert stats.rides_found == 0
        assert stats.accept_failures == []

    def test_empty_accept_failures_default(self):
        stats = PingStats(
            batch_id="batch-1",
            cycles_since_last_ping=0,
            rides_found=0,
        )
        assert stats.accept_failures == []

    def test_negative_cycles_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            PingStats(
                batch_id="batch-1",
                cycles_since_last_ping=-1,
                rides_found=0,
            )
        assert "cycles_since_last_ping" in str(exc_info.value)

    def test_negative_rides_found_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            PingStats(
                batch_id="batch-1",
                cycles_since_last_ping=0,
                rides_found=-1,
            )
        assert "rides_found" in str(exc_info.value)

    def test_missing_batch_id_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            PingStats(cycles_since_last_ping=0, rides_found=0)
        assert "batch_id" in str(exc_info.value)

    def test_missing_cycles_since_last_ping_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            PingStats(batch_id="b", rides_found=0)
        assert "cycles_since_last_ping" in str(exc_info.value)

    def test_missing_rides_found_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            PingStats(batch_id="b", cycles_since_last_ping=0)
        assert "rides_found" in str(exc_info.value)


class TestAcceptFailureItem:
    """AcceptFailureItem with various nullable field combinations."""

    def test_all_fields(self):
        ts = datetime(2026, 2, 9, 10, 30, 0, tzinfo=UTC)
        item = AcceptFailureItem(
            reason="AcceptButtonNotFound",
            ride_price=25.50,
            pickup_time="Tomorrow · 6:05AM",
            timestamp=ts,
        )
        assert item.reason == "AcceptButtonNotFound"
        assert item.ride_price == 25.50
        assert item.pickup_time == "Tomorrow · 6:05AM"
        assert item.timestamp == ts

    def test_nullable_ride_price(self):
        ts = datetime(2026, 2, 9, 10, 0, 0, tzinfo=UTC)
        item = AcceptFailureItem(
            reason="Timeout",
            ride_price=None,
            pickup_time="Today · 3:00PM",
            timestamp=ts,
        )
        assert item.ride_price is None

    def test_nullable_pickup_time(self):
        ts = datetime(2026, 2, 9, 10, 0, 0, tzinfo=UTC)
        item = AcceptFailureItem(
            reason="ScreenOff",
            ride_price=15.0,
            pickup_time=None,
            timestamp=ts,
        )
        assert item.pickup_time is None

    def test_both_nullable_fields_none(self):
        ts = datetime(2026, 2, 9, 10, 0, 0, tzinfo=UTC)
        item = AcceptFailureItem(
            reason="Unknown",
            timestamp=ts,
        )
        assert item.ride_price is None
        assert item.pickup_time is None

    def test_missing_reason_raises(self):
        ts = datetime(2026, 2, 9, 10, 0, 0, tzinfo=UTC)
        with pytest.raises(ValidationError) as exc_info:
            AcceptFailureItem(timestamp=ts)
        assert "reason" in str(exc_info.value)

    def test_missing_timestamp_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            AcceptFailureItem(reason="Timeout")
        assert "timestamp" in str(exc_info.value)


class TestPingResponse:
    """PingResponse serialization with PingFiltersResponse."""

    def test_search_active(self):
        resp = PingResponse(
            search=True,
            interval_seconds=30,
            force_update=False,
            filters=PingFiltersResponse(min_price=20.0),
        )
        data = resp.model_dump(mode="json")
        assert data["search"] is True
        assert data["interval_seconds"] == 30
        assert data["force_update"] is False
        assert data["update_url"] is None
        assert data["filters"]["min_price"] == 20.0

    def test_search_stopped(self):
        resp = PingResponse(
            search=False,
            interval_seconds=60,
            filters=PingFiltersResponse(min_price=25.0),
        )
        assert resp.search is False
        assert resp.interval_seconds == 60
        assert resp.force_update is False
        assert resp.update_url is None
        assert resp.filters.min_price == 25.0

    def test_force_update(self):
        resp = PingResponse(
            search=False,
            interval_seconds=300,
            force_update=True,
            update_url="https://skeddy.net/download/search-app.apk",
            filters=PingFiltersResponse(min_price=20.0),
        )
        data = resp.model_dump(mode="json")
        assert data["search"] is False
        assert data["force_update"] is True
        assert data["update_url"] == "https://skeddy.net/download/search-app.apk"
        assert data["interval_seconds"] == 300

    def test_force_update_default_false(self):
        resp = PingResponse(
            search=True,
            interval_seconds=30,
            filters=PingFiltersResponse(min_price=20.0),
        )
        assert resp.force_update is False

    def test_update_url_default_none(self):
        resp = PingResponse(
            search=True,
            interval_seconds=30,
            filters=PingFiltersResponse(min_price=20.0),
        )
        assert resp.update_url is None

    def test_filters_only_min_price(self):
        resp = PingResponse(
            search=True,
            interval_seconds=30,
            filters=PingFiltersResponse(min_price=42.0),
        )
        data = resp.model_dump(mode="json")
        assert data["filters"] == {"min_price": 42.0}

    def test_missing_search_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            PingResponse(
                interval_seconds=30,
                filters=PingFiltersResponse(min_price=20.0),
            )
        assert "search" in str(exc_info.value)

    def test_missing_interval_seconds_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            PingResponse(
                search=True,
                filters=PingFiltersResponse(min_price=20.0),
            )
        assert "interval_seconds" in str(exc_info.value)

    def test_missing_filters_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            PingResponse(
                search=True,
                interval_seconds=30,
            )
        assert "filters" in str(exc_info.value)


class TestDeviceHealth:
    """DeviceHealth with optional fields."""

    def test_all_fields(self):
        health = DeviceHealth(
            accessibility_enabled=True,
            lyft_running=True,
            screen_on=True,
        )
        assert health.accessibility_enabled is True
        assert health.lyft_running is True
        assert health.screen_on is True

    def test_all_defaults_none(self):
        health = DeviceHealth()
        assert health.accessibility_enabled is None
        assert health.lyft_running is None
        assert health.screen_on is None

    def test_partial_fields(self):
        health = DeviceHealth(accessibility_enabled=False)
        assert health.accessibility_enabled is False
        assert health.lyft_running is None
        assert health.screen_on is None


class TestPingRequestInvalidJson:
    """Invalid JSON data raises ValidationError."""

    def test_invalid_type_timezone(self):
        with pytest.raises(ValidationError):
            PingRequest(timezone=123, app_version="1.0.0")

    def test_invalid_type_app_version(self):
        with pytest.raises(ValidationError):
            PingRequest(timezone="UTC", app_version=123)

    def test_invalid_stats_type(self):
        with pytest.raises(ValidationError):
            PingRequest(timezone="UTC", app_version="1.0.0", stats="not-an-object")

    def test_invalid_device_health_type(self):
        with pytest.raises(ValidationError):
            PingRequest(timezone="UTC", app_version="1.0.0", device_health="not-an-object")

    def test_completely_empty_raises(self):
        with pytest.raises(ValidationError):
            PingRequest()


class TestRideStatusReport:
    """RideStatusReport validation."""

    def test_valid_report(self):
        report = RideStatusReport(ride_hash="a" * 64, present=True)
        assert report.ride_hash == "a" * 64
        assert report.present is True

    def test_present_false(self):
        report = RideStatusReport(ride_hash="b" * 64, present=False)
        assert report.present is False

    def test_empty_ride_hash_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            RideStatusReport(ride_hash="", present=True)
        assert "ride_hash" in str(exc_info.value)

    def test_missing_ride_hash_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            RideStatusReport(present=True)
        assert "ride_hash" in str(exc_info.value)

    def test_missing_present_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            RideStatusReport(ride_hash="a" * 64)
        assert "present" in str(exc_info.value)


class TestVerifyRideItem:
    """VerifyRideItem validation."""

    def test_valid_item(self):
        item = VerifyRideItem(ride_hash="c76966d3" + "a" * 56)
        assert item.ride_hash == "c76966d3" + "a" * 56

    def test_serialization(self):
        item = VerifyRideItem(ride_hash="abc123")
        data = item.model_dump(mode="json")
        assert data == {"ride_hash": "abc123"}

    def test_missing_ride_hash_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            VerifyRideItem()
        assert "ride_hash" in str(exc_info.value)


class TestPingRequestRideStatuses:
    """PingRequest with ride_statuses field."""

    def test_backward_compatible_without_ride_statuses(self):
        """Old Search App sends no ride_statuses — should parse fine."""
        req = PingRequest(timezone="UTC", app_version="1.0.0")
        assert req.ride_statuses is None

    def test_empty_ride_statuses_list(self):
        """Search App sends empty ride_statuses when nothing to report."""
        req = PingRequest(timezone="UTC", app_version="1.0.0", ride_statuses=[])
        assert req.ride_statuses == []

    def test_with_ride_statuses(self):
        req = PingRequest(
            timezone="America/New_York",
            app_version="1.2.0",
            ride_statuses=[
                {"ride_hash": "a" * 64, "present": True},
                {"ride_hash": "b" * 64, "present": False},
            ],
        )
        assert len(req.ride_statuses) == 2
        assert req.ride_statuses[0].ride_hash == "a" * 64
        assert req.ride_statuses[0].present is True
        assert req.ride_statuses[1].present is False

    def test_ride_statuses_invalid_item_raises(self):
        with pytest.raises(ValidationError):
            PingRequest(
                timezone="UTC",
                app_version="1.0.0",
                ride_statuses=[{"ride_hash": "", "present": True}],
            )

    def test_serialization_round_trip_with_ride_statuses(self):
        req = PingRequest(
            timezone="UTC",
            app_version="1.0.0",
            ride_statuses=[
                RideStatusReport(ride_hash="c" * 64, present=True),
            ],
        )
        data = req.model_dump(mode="json")
        restored = PingRequest.model_validate(data)
        assert len(restored.ride_statuses) == 1
        assert restored.ride_statuses[0].ride_hash == "c" * 64
        assert restored.ride_statuses[0].present is True


class TestPingResponseVerifyRides:
    """PingResponse with verify_rides field."""

    def test_backward_compatible_without_verify_rides(self):
        """Response without verify_rides — defaults to None."""
        resp = PingResponse(
            search=True,
            interval_seconds=30,
            filters=PingFiltersResponse(min_price=20.0),
        )
        assert resp.verify_rides is None

    def test_empty_verify_rides(self):
        resp = PingResponse(
            search=True,
            interval_seconds=30,
            filters=PingFiltersResponse(min_price=20.0),
            verify_rides=[],
        )
        assert resp.verify_rides == []

    def test_with_verify_rides(self):
        resp = PingResponse(
            search=True,
            interval_seconds=30,
            filters=PingFiltersResponse(min_price=20.0),
            verify_rides=[
                VerifyRideItem(ride_hash="a" * 64),
                VerifyRideItem(ride_hash="b" * 64),
            ],
        )
        assert len(resp.verify_rides) == 2
        assert resp.verify_rides[0].ride_hash == "a" * 64

    def test_verify_rides_serialization(self):
        resp = PingResponse(
            search=False,
            interval_seconds=60,
            filters=PingFiltersResponse(min_price=25.0),
            verify_rides=[VerifyRideItem(ride_hash="d" * 64)],
        )
        data = resp.model_dump(mode="json")
        assert data["verify_rides"] == [{"ride_hash": "d" * 64}]
        assert data["search"] is False

    def test_verify_rides_independent_of_search_flag(self):
        """verify_rides sent regardless of search: true/false (PRD section 7)."""
        resp_active = PingResponse(
            search=True,
            interval_seconds=30,
            filters=PingFiltersResponse(min_price=20.0),
            verify_rides=[VerifyRideItem(ride_hash="e" * 64)],
        )
        resp_inactive = PingResponse(
            search=False,
            interval_seconds=60,
            filters=PingFiltersResponse(min_price=20.0),
            verify_rides=[VerifyRideItem(ride_hash="e" * 64)],
        )
        assert len(resp_active.verify_rides) == 1
        assert len(resp_inactive.verify_rides) == 1
