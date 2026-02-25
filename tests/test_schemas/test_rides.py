import uuid
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from app.schemas.rides import (
    CreateRideRequest,
    CreateRideResponse,
    RideData,
    RideEventResponse,
)


class TestCreateRideRequestValid:
    """Valid CreateRideRequest parses correctly."""

    def test_all_fields(self):
        req = CreateRideRequest(
            idempotency_key="550e8400-e29b-41d4-a716-446655440000",
            event_type="ACCEPTED",
            ride_data=RideData(
                price=25.50,
                pickup_time="Tomorrow · 6:05AM",
                pickup_location="Maida Ter & Maida Way",
                dropoff_location="East Rd & Leonardville Rd",
                duration="9 min",
                distance="3.6 mi",
                rider_name="Kathleen",
            ),
            ride_hash="a" * 64,
            timezone="America/New_York",
        )
        assert req.idempotency_key == "550e8400-e29b-41d4-a716-446655440000"
        assert req.event_type == "ACCEPTED"
        assert req.ride_data.price == 25.50
        assert req.ride_data.pickup_time == "Tomorrow · 6:05AM"
        assert req.ride_data.pickup_location == "Maida Ter & Maida Way"
        assert req.ride_data.dropoff_location == "East Rd & Leonardville Rd"
        assert req.ride_data.duration == "9 min"
        assert req.ride_data.distance == "3.6 mi"
        assert req.ride_data.rider_name == "Kathleen"

    def test_minimal_ride_data(self):
        req = CreateRideRequest(
            idempotency_key=str(uuid.uuid4()),
            event_type="ACCEPTED",
            ride_data=RideData(
                price=10.0,
                pickup_time="Today · 3:00PM",
                pickup_location="Main St",
                dropoff_location="Oak Ave",
            ),
            ride_hash="a" * 64,
            timezone="Europe/Kyiv",
        )
        assert req.ride_data.duration is None
        assert req.ride_data.distance is None
        assert req.ride_data.rider_name is None

    def test_serialization_round_trip(self):
        req = CreateRideRequest(
            idempotency_key=str(uuid.uuid4()),
            event_type="ACCEPTED",
            ride_data=RideData(
                price=50.0,
                pickup_time="Tomorrow · 8:00AM",
                pickup_location="Start",
                dropoff_location="End",
                duration="15 min",
                distance="7.2 mi",
                rider_name="John",
            ),
            ride_hash="a" * 64,
            timezone="UTC",
        )
        data = req.model_dump(mode="json")
        restored = CreateRideRequest.model_validate(data)
        assert restored.idempotency_key == req.idempotency_key
        assert restored.ride_data.price == req.ride_data.price
        assert restored.ride_data.rider_name == req.ride_data.rider_name


class TestCreateRideRequestInvalidIdempotencyKey:
    """Invalid idempotency_key (not UUID) raises ValidationError."""

    def test_not_uuid_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            CreateRideRequest(
                idempotency_key="not-a-uuid",
                event_type="ACCEPTED",
                ride_data=RideData(
                    price=20.0,
                    pickup_time="Today",
                    pickup_location="A",
                    dropoff_location="B",
                ),
            )
        assert "idempotency_key" in str(exc_info.value)

    def test_empty_string_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            CreateRideRequest(
                idempotency_key="",
                event_type="ACCEPTED",
                ride_data=RideData(
                    price=20.0,
                    pickup_time="Today",
                    pickup_location="A",
                    dropoff_location="B",
                ),
            )
        assert "idempotency_key" in str(exc_info.value)

    def test_random_string_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            CreateRideRequest(
                idempotency_key="abc123xyz",
                event_type="ACCEPTED",
                ride_data=RideData(
                    price=20.0,
                    pickup_time="Today",
                    pickup_location="A",
                    dropoff_location="B",
                ),
            )
        assert "idempotency_key" in str(exc_info.value)

    def test_uuid_without_dashes_accepted(self):
        key = uuid.uuid4().hex
        req = CreateRideRequest(
            idempotency_key=key,
            event_type="ACCEPTED",
            ride_data=RideData(
                price=20.0,
                pickup_time="Today",
                pickup_location="A",
                dropoff_location="B",
            ),
            ride_hash="a" * 64,
            timezone="America/New_York",
        )
        assert req.idempotency_key == key


class TestCreateRideRequestInvalidEventType:
    """Invalid event_type (not 'ACCEPTED') raises ValidationError."""

    def test_rejected_event_type(self):
        with pytest.raises(ValidationError) as exc_info:
            CreateRideRequest(
                idempotency_key=str(uuid.uuid4()),
                event_type="REJECTED",
                ride_data=RideData(
                    price=20.0,
                    pickup_time="Today",
                    pickup_location="A",
                    dropoff_location="B",
                ),
            )
        assert "event_type" in str(exc_info.value)

    def test_lowercase_accepted_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            CreateRideRequest(
                idempotency_key=str(uuid.uuid4()),
                event_type="accepted",
                ride_data=RideData(
                    price=20.0,
                    pickup_time="Today",
                    pickup_location="A",
                    dropoff_location="B",
                ),
            )
        assert "event_type" in str(exc_info.value)

    def test_empty_event_type_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            CreateRideRequest(
                idempotency_key=str(uuid.uuid4()),
                event_type="",
                ride_data=RideData(
                    price=20.0,
                    pickup_time="Today",
                    pickup_location="A",
                    dropoff_location="B",
                ),
            )
        assert "event_type" in str(exc_info.value)


class TestRideEventResponseSerialization:
    """RideEventResponse serializes UUID and datetime correctly."""

    def test_serializes_uuid_and_datetime(self):
        ride_id = uuid.uuid4()
        ts = datetime(2026, 2, 9, 14, 30, 0, tzinfo=UTC)
        resp = RideEventResponse(
            id=ride_id,
            event_type="ACCEPTED",
            ride_data={
                "price": 25.50,
                "pickup_time": "Tomorrow · 6:05AM",
                "pickup_location": "Maida Ter & Maida Way",
                "dropoff_location": "East Rd & Leonardville Rd",
            },
            created_at=ts,
        )
        data = resp.model_dump(mode="json")
        assert isinstance(data["id"], str)
        assert data["id"] == str(ride_id)
        assert isinstance(data["created_at"], str)
        assert data["event_type"] == "ACCEPTED"
        assert data["ride_data"]["price"] == 25.50

    def test_accepts_string_uuid(self):
        ride_id = uuid.uuid4()
        ts = datetime(2026, 2, 9, 14, 30, 0, tzinfo=UTC)
        resp = RideEventResponse(
            id=str(ride_id),
            event_type="ACCEPTED",
            ride_data={"price": 10.0},
            created_at=ts,
        )
        assert resp.id == ride_id

    def test_invalid_uuid_raises(self):
        ts = datetime(2026, 2, 9, 14, 30, 0, tzinfo=UTC)
        with pytest.raises(ValidationError):
            RideEventResponse(
                id="not-a-uuid",
                event_type="ACCEPTED",
                ride_data={"price": 10.0},
                created_at=ts,
            )

    def test_ride_data_as_dict(self):
        ride_id = uuid.uuid4()
        ts = datetime(2026, 2, 9, 14, 30, 0, tzinfo=UTC)
        ride_data = {
            "price": 25.50,
            "pickup_time": "Tomorrow · 6:05AM",
            "pickup_location": "Maida Ter & Maida Way",
            "dropoff_location": "East Rd & Leonardville Rd",
            "duration": "9 min",
            "distance": "3.6 mi",
            "rider_name": "Kathleen",
        }
        resp = RideEventResponse(
            id=ride_id,
            event_type="ACCEPTED",
            ride_data=ride_data,
            created_at=ts,
        )
        assert resp.ride_data == ride_data


class TestCreateRideResponse:
    """CreateRideResponse serialization."""

    def test_serializes_ride_id(self):
        ride_id = uuid.uuid4()
        resp = CreateRideResponse(ride_id=ride_id)
        data = resp.model_dump(mode="json")
        assert data["ok"] is True
        assert isinstance(data["ride_id"], str)
        assert data["ride_id"] == str(ride_id)

    def test_ok_defaults_true(self):
        resp = CreateRideResponse(ride_id=uuid.uuid4())
        assert resp.ok is True

    def test_accepts_string_uuid(self):
        ride_id = uuid.uuid4()
        resp = CreateRideResponse(ride_id=str(ride_id))
        assert resp.ride_id == ride_id


class TestRideDataValidation:
    """RideData required and optional field validation."""

    def test_missing_price_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            RideData(
                pickup_time="Today",
                pickup_location="A",
                dropoff_location="B",
            )
        assert "price" in str(exc_info.value)

    def test_missing_pickup_time_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            RideData(
                price=20.0,
                pickup_location="A",
                dropoff_location="B",
            )
        assert "pickup_time" in str(exc_info.value)

    def test_missing_pickup_location_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            RideData(
                price=20.0,
                pickup_time="Today",
                dropoff_location="B",
            )
        assert "pickup_location" in str(exc_info.value)

    def test_missing_dropoff_location_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            RideData(
                price=20.0,
                pickup_time="Today",
                pickup_location="A",
            )
        assert "dropoff_location" in str(exc_info.value)

    def test_optional_fields_default_none(self):
        data = RideData(
            price=20.0,
            pickup_time="Today",
            pickup_location="A",
            dropoff_location="B",
        )
        assert data.duration is None
        assert data.distance is None
        assert data.rider_name is None
