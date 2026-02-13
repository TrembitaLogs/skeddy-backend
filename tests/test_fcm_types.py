import uuid
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from app.schemas.fcm import (
    NotificationType,
    RideAcceptedData,
    SearchOfflineData,
    create_ride_accepted_payload,
    create_search_offline_payload,
)


class TestNotificationType:
    """Test NotificationType enum values and behavior."""

    def test_ride_accepted_value(self):
        assert NotificationType.RIDE_ACCEPTED == "RIDE_ACCEPTED"

    def test_search_offline_value(self):
        assert NotificationType.SEARCH_OFFLINE == "SEARCH_OFFLINE"

    def test_usable_as_string(self):
        """StrEnum values can be used directly as strings."""
        result = f"type={NotificationType.RIDE_ACCEPTED}"
        assert result == "type=RIDE_ACCEPTED"

    def test_enum_has_exactly_two_members(self):
        assert len(NotificationType) == 2


class TestRideAcceptedData:
    """Test RideAcceptedData Pydantic validation."""

    def test_valid_data(self):
        ride_id = uuid.uuid4()
        data = RideAcceptedData(
            ride_id=ride_id,
            price=25.50,
            pickup_time="Tomorrow · 6:05AM",
            pickup_location="Maida Ter & Maida Way",
            dropoff_location="East Rd & Leonardville Rd",
        )
        assert data.ride_id == ride_id
        assert data.price == 25.50
        assert data.pickup_time == "Tomorrow · 6:05AM"

    def test_accepts_uuid_string_for_ride_id(self):
        """Pydantic coerces valid UUID strings."""
        ride_id_str = "550e8400-e29b-41d4-a716-446655440000"
        data = RideAcceptedData(
            ride_id=ride_id_str,
            price=30.0,
            pickup_time="Today · 3:00PM",
            pickup_location="Main St",
            dropoff_location="Oak Ave",
        )
        assert data.ride_id == uuid.UUID(ride_id_str)

    def test_accepts_int_for_price(self):
        """Pydantic coerces int to float."""
        data = RideAcceptedData(
            ride_id=uuid.uuid4(),
            price=30,
            pickup_time="Today · 3:00PM",
            pickup_location="Main St",
            dropoff_location="Oak Ave",
        )
        assert data.price == 30.0
        assert isinstance(data.price, float)

    def test_rejects_invalid_uuid(self):
        with pytest.raises(ValidationError):
            RideAcceptedData(
                ride_id="not-a-uuid",
                price=25.50,
                pickup_time="Tomorrow · 6:05AM",
                pickup_location="Main St",
                dropoff_location="Oak Ave",
            )

    def test_rejects_missing_required_fields(self):
        with pytest.raises(ValidationError):
            RideAcceptedData(
                ride_id=uuid.uuid4(),
                price=25.50,
            )


class TestSearchOfflineData:
    """Test SearchOfflineData Pydantic validation."""

    def test_valid_data(self):
        now = datetime.now(UTC)
        data = SearchOfflineData(
            device_id="android_device_123",
            last_ping_at=now,
        )
        assert data.device_id == "android_device_123"
        assert data.last_ping_at == now

    def test_accepts_iso_string_for_datetime(self):
        """Pydantic coerces ISO datetime strings."""
        data = SearchOfflineData(
            device_id="device_abc",
            last_ping_at="2026-02-09T14:28:00Z",
        )
        assert isinstance(data.last_ping_at, datetime)

    def test_rejects_invalid_datetime(self):
        with pytest.raises(ValidationError):
            SearchOfflineData(
                device_id="device_abc",
                last_ping_at="not-a-datetime",
            )

    def test_rejects_missing_required_fields(self):
        with pytest.raises(ValidationError):
            SearchOfflineData(device_id="device_abc")


class TestCreateRideAcceptedPayload:
    """Test create_ride_accepted_payload returns dict with string values."""

    def test_returns_dict_with_string_values(self):
        ride_id = uuid.uuid4()
        result = create_ride_accepted_payload(
            ride_id=ride_id,
            price=25.50,
            pickup_time="Tomorrow · 6:05AM",
            pickup_location="Maida Ter & Maida Way",
            dropoff_location="East Rd & Leonardville Rd",
        )
        for value in result.values():
            assert isinstance(value, str), f"Expected str, got {type(value)}: {value}"

    def test_uuid_serialized_correctly(self):
        ride_id = uuid.UUID("550e8400-e29b-41d4-a716-446655440000")
        result = create_ride_accepted_payload(
            ride_id=ride_id,
            price=25.50,
            pickup_time="Tomorrow · 6:05AM",
            pickup_location="Main St",
            dropoff_location="Oak Ave",
        )
        assert result["ride_id"] == "550e8400-e29b-41d4-a716-446655440000"

    def test_price_serialized_as_string(self):
        result = create_ride_accepted_payload(
            ride_id=uuid.uuid4(),
            price=25.50,
            pickup_time="Tomorrow · 6:05AM",
            pickup_location="Main St",
            dropoff_location="Oak Ave",
        )
        assert result["price"] == "25.5"

    def test_contains_all_required_keys(self):
        result = create_ride_accepted_payload(
            ride_id=uuid.uuid4(),
            price=25.50,
            pickup_time="Tomorrow · 6:05AM",
            pickup_location="Main St",
            dropoff_location="Oak Ave",
        )
        expected_keys = {"ride_id", "price", "pickup_time", "pickup_location", "dropoff_location"}
        assert set(result.keys()) == expected_keys

    def test_pickup_time_preserved_as_is(self):
        """pickup_time is free-form text from Lyft UI, passed through unchanged."""
        result = create_ride_accepted_payload(
            ride_id=uuid.uuid4(),
            price=25.50,
            pickup_time="Tomorrow · 6:05AM",
            pickup_location="Main St",
            dropoff_location="Oak Ave",
        )
        assert result["pickup_time"] == "Tomorrow · 6:05AM"

    def test_rejects_invalid_data(self):
        """Pydantic validation runs before payload creation."""
        with pytest.raises(ValidationError):
            create_ride_accepted_payload(
                ride_id="not-a-uuid",
                price=25.50,
                pickup_time="Tomorrow · 6:05AM",
                pickup_location="Main St",
                dropoff_location="Oak Ave",
            )


class TestCreateSearchOfflinePayload:
    """Test create_search_offline_payload returns dict with string values."""

    def test_returns_dict_with_string_values(self):
        result = create_search_offline_payload(
            device_id="android_device_123",
            last_ping_at=datetime(2026, 2, 9, 14, 28, 0, tzinfo=UTC),
        )
        for value in result.values():
            assert isinstance(value, str), f"Expected str, got {type(value)}: {value}"

    def test_datetime_serialized_to_iso(self):
        dt = datetime(2026, 2, 9, 14, 28, 0, tzinfo=UTC)
        result = create_search_offline_payload(
            device_id="android_device_123",
            last_ping_at=dt,
        )
        assert result["last_ping_at"] == "2026-02-09T14:28:00+00:00"

    def test_device_id_passed_as_string(self):
        result = create_search_offline_payload(
            device_id="some-android-device-id",
            last_ping_at=datetime.now(UTC),
        )
        assert result["device_id"] == "some-android-device-id"

    def test_contains_all_required_keys(self):
        result = create_search_offline_payload(
            device_id="android_device_123",
            last_ping_at=datetime.now(UTC),
        )
        expected_keys = {"device_id", "last_ping_at"}
        assert set(result.keys()) == expected_keys

    def test_rejects_invalid_datetime(self):
        """Pydantic validation runs before payload creation."""
        with pytest.raises(ValidationError):
            create_search_offline_payload(
                device_id="android_device_123",
                last_ping_at="not-a-datetime",
            )
