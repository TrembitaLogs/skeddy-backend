"""E2E tests for Events unified feed with cursor pagination.

Verifies the unified event feed (GET /rides/events): mixed ride + credit
events, correct event_kind discrimination, cursor-based pagination,
descending timestamp ordering, and proper field presence.

Test strategy (task 14.7):
1. Test events returns mixed feed (ride + credit event_kinds)
2. Test cursor pagination (multi-page traversal, no duplicates)
3. Test events ordered by created_at DESC
4. Test credit event fields (REGISTRATION_BONUS visible, RIDE_CHARGE excluded)
5. Test ride event includes billing fields (credits_charged, verification_status)
"""

import uuid
from datetime import UTC, datetime, timedelta

from tests.billing.helpers import make_ride_hash

RIDES_URL = "/api/v1/rides"
EVENTS_URL = "/api/v1/rides/events"


def _make_ride_request(
    *, price: float, ride_hash: str | None = None, timezone: str = "America/New_York"
) -> dict:
    """Build a valid POST /rides request body."""
    return {
        "idempotency_key": str(uuid.uuid4()),
        "event_type": "ACCEPTED",
        "ride_hash": ride_hash or make_ride_hash(),
        "timezone": timezone,
        "ride_data": {
            "price": price,
            "pickup_time": "Tomorrow \u00b7 6:05AM",
            "pickup_location": "123 Main St",
            "dropoff_location": "456 Oak Ave",
        },
    }


# ---------------------------------------------------------------------------
# Test: Mixed feed with ride + credit events
# ---------------------------------------------------------------------------


async def test_events_returns_mixed_feed(device_headers, billing_app_config, db_session):
    """GET /rides/events -> response contains both ride and credit event_kinds.

    After registration (REGISTRATION_BONUS) and creating rides (RIDE_CHARGE),
    the events feed should include:
    - event_kind="ride" cards (from rides table)
    - event_kind="credit" cards (REGISTRATION_BONUS from credit_transactions)
    RIDE_CHARGE transactions are NOT shown as separate credit events
    (they are embedded in ride cards via credits_charged).
    """
    # Create 2 rides to generate ride events + RIDE_CHARGE transactions
    for _ in range(2):
        resp = await device_headers.client.post(
            RIDES_URL,
            json=_make_ride_request(price=25.00),
            headers=device_headers.headers,
        )
        assert resp.status_code == 201

    # GET events feed
    resp = await device_headers.client.get(
        EVENTS_URL,
        headers=device_headers.auth_headers,
    )
    assert resp.status_code == 200

    data = resp.json()
    events = data["events"]
    assert len(events) > 0

    # Collect unique event_kinds
    event_kinds = {e["event_kind"] for e in events}
    assert "ride" in event_kinds, "Expected ride events in feed"
    assert "credit" in event_kinds, "Expected credit events in feed (REGISTRATION_BONUS)"

    # Verify ride events have billing fields
    ride_events = [e for e in events if e["event_kind"] == "ride"]
    for re_event in ride_events:
        assert "credits_charged" in re_event
        assert "credits_refunded" in re_event
        assert "verification_status" in re_event
        assert re_event["event_type"] == "ACCEPTED"
        assert "ride_data" in re_event

    # Verify credit events have correct fields
    credit_events = [e for e in events if e["event_kind"] == "credit"]
    for ce_event in credit_events:
        assert "credit_type" in ce_event
        assert "amount" in ce_event
        assert "balance_after" in ce_event
        # RIDE_CHARGE and RIDE_REFUND should NOT appear as credit events
        assert ce_event["credit_type"] not in ("RIDE_CHARGE", "RIDE_REFUND")


# ---------------------------------------------------------------------------
# Test: REGISTRATION_BONUS appears as credit event
# ---------------------------------------------------------------------------


async def test_registration_bonus_in_events(device_headers, billing_app_config, db_session):
    """GET /rides/events -> REGISTRATION_BONUS visible as credit event.

    The registration bonus created during user signup should appear
    in the events feed with correct amount and balance_after.
    """
    resp = await device_headers.client.get(
        EVENTS_URL,
        headers=device_headers.auth_headers,
    )
    assert resp.status_code == 200

    events = resp.json()["events"]
    bonus_events = [
        e
        for e in events
        if e["event_kind"] == "credit" and e["credit_type"] == "REGISTRATION_BONUS"
    ]
    assert len(bonus_events) == 1

    bonus = bonus_events[0]
    assert bonus["amount"] == 10  # registration bonus
    assert bonus["balance_after"] == 10


# ---------------------------------------------------------------------------
# Test: Cursor-based pagination
# ---------------------------------------------------------------------------


async def test_events_cursor_pagination(device_headers, billing_app_config, db_session):
    """GET /rides/events with cursor -> proper multi-page pagination.

    Creates multiple rides to generate enough events for pagination.
    Verifies: page sizes, next_cursor presence, has_more flag,
    no duplicate events across pages.
    """
    # Create 4 rides (+ 1 REGISTRATION_BONUS = 5 ride events + credit events)
    for _ in range(4):
        resp = await device_headers.client.post(
            RIDES_URL,
            json=_make_ride_request(price=15.00),  # Tier 1: 1 credit each
            headers=device_headers.headers,
        )
        assert resp.status_code == 201

    # Total events: 4 ride events + 1 REGISTRATION_BONUS = 5 events

    # First page: limit=3
    resp = await device_headers.client.get(
        f"{EVENTS_URL}?limit=3",
        headers=device_headers.auth_headers,
    )
    assert resp.status_code == 200

    page1 = resp.json()
    assert len(page1["events"]) == 3
    assert page1["has_more"] is True
    assert page1["next_cursor"] is not None

    page1_ids = {e["id"] for e in page1["events"]}

    # Second page: using cursor from first page
    cursor = page1["next_cursor"]
    resp = await device_headers.client.get(
        f"{EVENTS_URL}?limit=3&cursor={cursor}",
        headers=device_headers.auth_headers,
    )
    assert resp.status_code == 200

    page2 = resp.json()
    assert len(page2["events"]) == 2  # remaining events
    assert page2["has_more"] is False

    page2_ids = {e["id"] for e in page2["events"]}

    # No duplicate events between pages
    assert page1_ids.isdisjoint(page2_ids), "Duplicate events found across pages"

    # All 5 events accounted for
    assert len(page1_ids | page2_ids) == 5


# ---------------------------------------------------------------------------
# Test: Events ordered by created_at DESC
# ---------------------------------------------------------------------------


async def test_events_ordered_by_timestamp(device_headers, billing_app_config, db_session):
    """Events are sorted by created_at DESC (newest first).

    Creates multiple rides at different times and verifies the feed
    returns them in strictly descending order.
    """
    # Create 3 rides
    for _ in range(3):
        resp = await device_headers.client.post(
            RIDES_URL,
            json=_make_ride_request(price=15.00),
            headers=device_headers.headers,
        )
        assert resp.status_code == 201

    resp = await device_headers.client.get(
        EVENTS_URL,
        headers=device_headers.auth_headers,
    )
    assert resp.status_code == 200

    events = resp.json()["events"]
    assert len(events) >= 3

    # Verify descending timestamp order
    timestamps = [e["created_at"] for e in events]
    assert timestamps == sorted(timestamps, reverse=True), (
        f"Events not in descending order: {timestamps}"
    )


# ---------------------------------------------------------------------------
# Test: Ride event includes billing fields
# ---------------------------------------------------------------------------


async def test_ride_event_includes_billing_fields(device_headers, billing_app_config, db_session):
    """Ride events in the feed include credits_charged and verification_status.

    After creating a ride with $25 price (Tier 2 -> 2 credits), the ride
    event should show credits_charged=2, credits_refunded=0,
    verification_status='PENDING'.
    """
    resp = await device_headers.client.post(
        RIDES_URL,
        json=_make_ride_request(price=25.00),
        headers=device_headers.headers,
    )
    assert resp.status_code == 201
    ride_id = resp.json()["ride_id"]

    resp = await device_headers.client.get(
        EVENTS_URL,
        headers=device_headers.auth_headers,
    )
    assert resp.status_code == 200

    events = resp.json()["events"]
    ride_events = [e for e in events if e["event_kind"] == "ride" and e["id"] == ride_id]
    assert len(ride_events) == 1

    ride_event = ride_events[0]
    assert ride_event["credits_charged"] == 2
    assert ride_event["credits_refunded"] == 0
    assert ride_event["verification_status"] == "PENDING"
    assert ride_event["event_type"] == "ACCEPTED"
    assert ride_event["ride_data"]["price"] == 25.00


# ---------------------------------------------------------------------------
# Test: has_more=false and next_cursor=null when no more data
# ---------------------------------------------------------------------------


async def test_events_no_more_data(device_headers, billing_app_config, db_session):
    """When all events fit in one page, has_more=false and next_cursor=null.

    After registration there's only 1 event (REGISTRATION_BONUS).
    Requesting with limit=20 should return all events with no more pages.
    """
    resp = await device_headers.client.get(
        f"{EVENTS_URL}?limit=20",
        headers=device_headers.auth_headers,
    )
    assert resp.status_code == 200

    data = resp.json()
    assert data["has_more"] is False
    assert data["next_cursor"] is None
    assert len(data["events"]) >= 1  # at least REGISTRATION_BONUS


# ---------------------------------------------------------------------------
# Test: since filter works for both event types
# ---------------------------------------------------------------------------


async def test_events_since_filter(device_headers, billing_app_config, db_session):
    """GET /rides/events?since=... filters both ride and credit events.

    Using a 'since' timestamp far in the future should return no events.
    Using a 'since' timestamp in the past should return all events.
    """
    # Create a ride to have more data
    resp = await device_headers.client.post(
        RIDES_URL,
        json=_make_ride_request(price=15.00),
        headers=device_headers.headers,
    )
    assert resp.status_code == 201

    # Since far future -> no events
    # Use Z suffix instead of +00:00 to avoid URL encoding issues (+ becomes space)
    future_dt = datetime.now(UTC) + timedelta(days=365)
    future = future_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    resp = await device_headers.client.get(
        f"{EVENTS_URL}?since={future}",
        headers=device_headers.auth_headers,
    )
    assert resp.status_code == 200
    assert len(resp.json()["events"]) == 0
    assert resp.json()["has_more"] is False

    # Since far past -> all events
    past_dt = datetime.now(UTC) - timedelta(days=365)
    past = past_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    resp = await device_headers.client.get(
        f"{EVENTS_URL}?since={past}",
        headers=device_headers.auth_headers,
    )
    assert resp.status_code == 200
    assert len(resp.json()["events"]) >= 2  # at least 1 ride + 1 bonus
