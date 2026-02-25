"""Tests for unified event feed schemas (task 10.2).

Covers:
1. RideEventResponse serialization with all billing fields
2. CreditEventResponse serialization
3. UnifiedEventResponse discriminator: ride → RideEventResponse
4. UnifiedEventResponse discriminator: credit → CreditEventResponse
5. EventsListResponse with mixed events and cursor
"""

import uuid
from datetime import UTC, datetime

import pytest
from pydantic import TypeAdapter, ValidationError

from app.schemas.rides import (
    CreditEventResponse,
    EventsListResponse,
    RideEventResponse,
    UnifiedEventResponse,
)

# ---------------------------------------------------------------------------
# 1. RideEventResponse serialization with all fields
# ---------------------------------------------------------------------------


class TestRideEventResponseBillingFields:
    """RideEventResponse includes event_kind and billing fields."""

    def test_all_billing_fields(self):
        ride_id = uuid.uuid4()
        ts = datetime(2026, 2, 21, 10, 30, 0, tzinfo=UTC)
        resp = RideEventResponse(
            id=ride_id,
            event_type="ACCEPTED",
            ride_data={"price": 45.0, "pickup_location": "123 Main St"},
            credits_charged=2,
            credits_refunded=0,
            verification_status="CONFIRMED",
            created_at=ts,
        )
        data = resp.model_dump(mode="json")
        assert data["event_kind"] == "ride"
        assert data["id"] == str(ride_id)
        assert data["event_type"] == "ACCEPTED"
        assert data["credits_charged"] == 2
        assert data["credits_refunded"] == 0
        assert data["verification_status"] == "CONFIRMED"
        assert data["ride_data"]["price"] == 45.0

    def test_event_kind_always_ride(self):
        resp = RideEventResponse(
            id=uuid.uuid4(),
            event_type="ACCEPTED",
            ride_data={"price": 10.0},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        assert resp.event_kind == "ride"

    def test_event_kind_literal_rejects_wrong_value(self):
        with pytest.raises(ValidationError):
            RideEventResponse(
                event_kind="credit",
                id=uuid.uuid4(),
                event_type="ACCEPTED",
                ride_data={"price": 10.0},
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
            )

    def test_billing_defaults(self):
        resp = RideEventResponse(
            id=uuid.uuid4(),
            event_type="ACCEPTED",
            ride_data={"price": 10.0},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        assert resp.credits_charged == 0
        assert resp.credits_refunded == 0
        assert resp.verification_status == "PENDING"

    def test_cancelled_ride_with_refund(self):
        resp = RideEventResponse(
            id=uuid.uuid4(),
            event_type="ACCEPTED",
            ride_data={"price": 50.0},
            credits_charged=3,
            credits_refunded=3,
            verification_status="CANCELLED",
            created_at=datetime(2026, 2, 22, 8, 0, 0, tzinfo=UTC),
        )
        data = resp.model_dump(mode="json")
        assert data["credits_charged"] == 3
        assert data["credits_refunded"] == 3
        assert data["verification_status"] == "CANCELLED"

    def test_serialization_round_trip(self):
        ride_id = uuid.uuid4()
        ts = datetime(2026, 2, 21, 10, 30, 0, tzinfo=UTC)
        original = RideEventResponse(
            id=ride_id,
            event_type="ACCEPTED",
            ride_data={"price": 25.0, "pickup_time": "Tomorrow · 6:05AM"},
            credits_charged=1,
            credits_refunded=0,
            verification_status="PENDING",
            created_at=ts,
        )
        data = original.model_dump(mode="json")
        restored = RideEventResponse.model_validate(data)
        assert restored.id == original.id
        assert restored.event_kind == "ride"
        assert restored.credits_charged == original.credits_charged
        assert restored.verification_status == original.verification_status


# ---------------------------------------------------------------------------
# 2. CreditEventResponse serialization
# ---------------------------------------------------------------------------


class TestCreditEventResponseSerialization:
    """CreditEventResponse serializes correctly for all credit types."""

    def test_purchase_event(self):
        tx_id = uuid.uuid4()
        ts = datetime(2026, 2, 21, 9, 0, 0, tzinfo=UTC)
        resp = CreditEventResponse(
            id=tx_id,
            credit_type="PURCHASE",
            amount=50,
            balance_after=59,
            created_at=ts,
        )
        data = resp.model_dump(mode="json")
        assert data["event_kind"] == "credit"
        assert data["id"] == str(tx_id)
        assert data["credit_type"] == "PURCHASE"
        assert data["amount"] == 50
        assert data["balance_after"] == 59
        assert data["description"] is None

    def test_registration_bonus_event(self):
        resp = CreditEventResponse(
            id=uuid.uuid4(),
            credit_type="REGISTRATION_BONUS",
            amount=10,
            balance_after=10,
            created_at=datetime(2026, 2, 20, 8, 0, 0, tzinfo=UTC),
        )
        data = resp.model_dump(mode="json")
        assert data["credit_type"] == "REGISTRATION_BONUS"
        assert data["amount"] == 10
        assert data["balance_after"] == 10

    def test_admin_adjustment_with_description(self):
        resp = CreditEventResponse(
            id=uuid.uuid4(),
            credit_type="ADMIN_ADJUSTMENT",
            amount=-5,
            balance_after=37,
            description="Refund for service issue",
            created_at=datetime(2026, 2, 22, 12, 0, 0, tzinfo=UTC),
        )
        data = resp.model_dump(mode="json")
        assert data["credit_type"] == "ADMIN_ADJUSTMENT"
        assert data["amount"] == -5
        assert data["description"] == "Refund for service issue"

    def test_event_kind_always_credit(self):
        resp = CreditEventResponse(
            id=uuid.uuid4(),
            credit_type="PURCHASE",
            amount=10,
            balance_after=10,
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        assert resp.event_kind == "credit"

    def test_event_kind_literal_rejects_wrong_value(self):
        with pytest.raises(ValidationError):
            CreditEventResponse(
                event_kind="ride",
                id=uuid.uuid4(),
                credit_type="PURCHASE",
                amount=10,
                balance_after=10,
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
            )

    def test_description_defaults_none(self):
        resp = CreditEventResponse(
            id=uuid.uuid4(),
            credit_type="PURCHASE",
            amount=25,
            balance_after=25,
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        assert resp.description is None

    def test_serialization_round_trip(self):
        original = CreditEventResponse(
            id=uuid.uuid4(),
            credit_type="ADMIN_ADJUSTMENT",
            amount=5,
            balance_after=47,
            description="Compensation",
            created_at=datetime(2026, 2, 21, 9, 0, 0, tzinfo=UTC),
        )
        data = original.model_dump(mode="json")
        restored = CreditEventResponse.model_validate(data)
        assert restored.id == original.id
        assert restored.credit_type == original.credit_type
        assert restored.amount == original.amount
        assert restored.description == original.description


# ---------------------------------------------------------------------------
# 3 & 4. UnifiedEventResponse discriminator
# ---------------------------------------------------------------------------


_unified_adapter = TypeAdapter(UnifiedEventResponse)


class TestUnifiedEventResponseDiscriminator:
    """Discriminated union resolves correct type based on event_kind."""

    def test_ride_event_resolves_to_ride(self):
        ride_data = {
            "event_kind": "ride",
            "id": str(uuid.uuid4()),
            "event_type": "ACCEPTED",
            "ride_data": {"price": 45.0},
            "credits_charged": 2,
            "credits_refunded": 0,
            "verification_status": "CONFIRMED",
            "created_at": "2026-02-21T10:30:00Z",
        }
        result = _unified_adapter.validate_python(ride_data)
        assert isinstance(result, RideEventResponse)
        assert result.event_kind == "ride"
        assert result.credits_charged == 2

    def test_credit_event_resolves_to_credit(self):
        credit_data = {
            "event_kind": "credit",
            "id": str(uuid.uuid4()),
            "credit_type": "PURCHASE",
            "amount": 50,
            "balance_after": 59,
            "created_at": "2026-02-21T09:00:00Z",
        }
        result = _unified_adapter.validate_python(credit_data)
        assert isinstance(result, CreditEventResponse)
        assert result.event_kind == "credit"
        assert result.amount == 50

    def test_invalid_event_kind_raises(self):
        bad_data = {
            "event_kind": "unknown",
            "id": str(uuid.uuid4()),
            "created_at": "2026-02-21T09:00:00Z",
        }
        with pytest.raises(ValidationError):
            _unified_adapter.validate_python(bad_data)

    def test_missing_event_kind_raises(self):
        data = {
            "id": str(uuid.uuid4()),
            "event_type": "ACCEPTED",
            "ride_data": {"price": 10.0},
            "created_at": "2026-02-21T10:30:00Z",
        }
        with pytest.raises(ValidationError):
            _unified_adapter.validate_python(data)

    def test_ride_discriminator_with_model_instance(self):
        ride = RideEventResponse(
            id=uuid.uuid4(),
            event_type="ACCEPTED",
            ride_data={"price": 10.0},
            credits_charged=1,
            verification_status="PENDING",
            created_at=datetime(2026, 2, 21, 10, 0, tzinfo=UTC),
        )
        data = ride.model_dump(mode="json")
        result = _unified_adapter.validate_python(data)
        assert isinstance(result, RideEventResponse)

    def test_credit_discriminator_with_model_instance(self):
        credit = CreditEventResponse(
            id=uuid.uuid4(),
            credit_type="REGISTRATION_BONUS",
            amount=10,
            balance_after=10,
            created_at=datetime(2026, 2, 20, 8, 0, tzinfo=UTC),
        )
        data = credit.model_dump(mode="json")
        result = _unified_adapter.validate_python(data)
        assert isinstance(result, CreditEventResponse)


# ---------------------------------------------------------------------------
# 5. EventsListResponse with mixed events and cursor
# ---------------------------------------------------------------------------


class TestEventsListResponse:
    """EventsListResponse holds mixed events with cursor pagination."""

    def _make_ride_event(self, ts: datetime, **kwargs) -> dict:
        defaults = {
            "event_kind": "ride",
            "id": str(uuid.uuid4()),
            "event_type": "ACCEPTED",
            "ride_data": {"price": 25.0},
            "credits_charged": 1,
            "credits_refunded": 0,
            "verification_status": "CONFIRMED",
            "created_at": ts.isoformat(),
        }
        defaults.update(kwargs)
        return defaults

    def _make_credit_event(self, ts: datetime, **kwargs) -> dict:
        defaults = {
            "event_kind": "credit",
            "id": str(uuid.uuid4()),
            "credit_type": "PURCHASE",
            "amount": 50,
            "balance_after": 59,
            "created_at": ts.isoformat(),
        }
        defaults.update(kwargs)
        return defaults

    def test_mixed_events_with_cursor(self):
        ride = self._make_ride_event(datetime(2026, 2, 21, 10, 30, tzinfo=UTC))
        credit = self._make_credit_event(datetime(2026, 2, 21, 9, 0, tzinfo=UTC))

        resp = EventsListResponse(
            events=[ride, credit],
            next_cursor="2026-02-20T08:00:00Z_credit_" + str(uuid.uuid4()),
            has_more=True,
        )
        assert len(resp.events) == 2
        assert isinstance(resp.events[0], RideEventResponse)
        assert isinstance(resp.events[1], CreditEventResponse)
        assert resp.has_more is True
        assert resp.next_cursor is not None

    def test_empty_events(self):
        resp = EventsListResponse(
            events=[],
            next_cursor=None,
            has_more=False,
        )
        assert len(resp.events) == 0
        assert resp.next_cursor is None
        assert resp.has_more is False

    def test_last_page(self):
        ride = self._make_ride_event(datetime(2026, 2, 21, 10, 30, tzinfo=UTC))
        resp = EventsListResponse(
            events=[ride],
            has_more=False,
        )
        assert len(resp.events) == 1
        assert resp.next_cursor is None
        assert resp.has_more is False

    def test_only_ride_events(self):
        rides = [
            self._make_ride_event(datetime(2026, 2, 21, 10, 30, tzinfo=UTC)),
            self._make_ride_event(datetime(2026, 2, 21, 9, 0, tzinfo=UTC)),
        ]
        resp = EventsListResponse(events=rides, has_more=False)
        assert all(isinstance(e, RideEventResponse) for e in resp.events)

    def test_only_credit_events(self):
        credits = [
            self._make_credit_event(
                datetime(2026, 2, 21, 9, 0, tzinfo=UTC),
                credit_type="PURCHASE",
            ),
            self._make_credit_event(
                datetime(2026, 2, 20, 8, 0, tzinfo=UTC),
                credit_type="REGISTRATION_BONUS",
                amount=10,
                balance_after=10,
            ),
        ]
        resp = EventsListResponse(events=credits, has_more=False)
        assert all(isinstance(e, CreditEventResponse) for e in resp.events)

    def test_serialization_round_trip(self):
        ride = self._make_ride_event(datetime(2026, 2, 21, 10, 30, tzinfo=UTC))
        credit = self._make_credit_event(datetime(2026, 2, 21, 9, 0, tzinfo=UTC))
        cursor = "2026-02-20T08:00:00Z_credit_" + str(uuid.uuid4())

        original = EventsListResponse(
            events=[ride, credit],
            next_cursor=cursor,
            has_more=True,
        )
        data = original.model_dump(mode="json")
        restored = EventsListResponse.model_validate(data)
        assert len(restored.events) == 2
        assert isinstance(restored.events[0], RideEventResponse)
        assert isinstance(restored.events[1], CreditEventResponse)
        assert restored.next_cursor == cursor
        assert restored.has_more is True
