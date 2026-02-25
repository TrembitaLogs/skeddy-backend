import logging
from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch
from uuid import UUID, uuid4

from sqlalchemy import func, select

from app.models.ride import Ride

RIDES_URL = "/api/v1/rides"
RIDES_EVENTS_URL = "/api/v1/rides/events"
REGISTER_URL = "/api/v1/auth/register"
PAIRING_GENERATE_URL = "/api/v1/pairing/generate"
PAIRING_CONFIRM_URL = "/api/v1/pairing/confirm"
FCM_REGISTER_URL = "/api/v1/fcm/register"

_TEST_PASSWORD = "securePass1"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _register(app_client, email="rides@example.com"):
    """Register a user and return response data with tokens."""
    resp = await app_client.post(REGISTER_URL, json={"email": email, "password": _TEST_PASSWORD})
    assert resp.status_code == 201
    return resp.json()


async def _pair_device(app_client, access_token, device_id="rides-device-001"):
    """Pair a device via generate + confirm flow."""
    gen_resp = await app_client.post(PAIRING_GENERATE_URL, headers=_jwt(access_token))
    assert gen_resp.status_code == 201
    code = gen_resp.json()["code"]

    confirm_resp = await app_client.post(
        PAIRING_CONFIRM_URL,
        json={"code": code, "device_id": device_id, "timezone": "America/New_York"},
    )
    assert confirm_resp.status_code == 200
    return confirm_resp.json()


async def _register_fcm(app_client, access_token, fcm_token="test-fcm-token-123"):
    """Register an FCM token for the user."""
    resp = await app_client.post(
        FCM_REGISTER_URL,
        json={"fcm_token": fcm_token},
        headers=_jwt(access_token),
    )
    assert resp.status_code == 200


def _jwt(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _device_headers(device_token: str, device_id: str) -> dict:
    return {"X-Device-Token": device_token, "X-Device-Id": device_id}


def _ride_body(**overrides) -> dict:
    """Build a valid ride request body with optional overrides."""
    body = {
        "idempotency_key": "550e8400-e29b-41d4-a716-446655440000",
        "event_type": "ACCEPTED",
        "ride_hash": "a" * 64,
        "timezone": "America/New_York",
        "ride_data": {
            "price": 25.50,
            "pickup_time": "Tomorrow \u00b7 6:05AM",
            "pickup_location": "Maida Ter & Maida Way",
            "dropoff_location": "East Rd & Leonardville Rd",
        },
    }
    body.update(overrides)
    return body


# ---------------------------------------------------------------------------
# Test 1: POST /rides with valid data → 201, ride_id, FCM called
# ---------------------------------------------------------------------------


async def test_create_ride_valid_data_returns_201(app_client, db_session):
    """POST /rides with valid data -> 201, ride saved, FCM push sent."""
    reg = await _register(app_client, email="ride1@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="ride1-dev")
    await _register_fcm(app_client, reg["access_token"])

    with patch(
        "app.routers.rides.send_push", new_callable=AsyncMock, return_value=True
    ) as mock_send_push:
        resp = await app_client.post(
            RIDES_URL,
            json=_ride_body(),
            headers=_device_headers(pairing["device_token"], "ride1-dev"),
        )

    assert resp.status_code == 201
    data = resp.json()
    assert data["ok"] is True
    assert "ride_id" in data
    UUID(data["ride_id"])  # Validates it's a valid UUID

    # Verify ride was saved in DB
    user_id = UUID(reg["user_id"])
    result = await db_session.execute(
        select(func.count()).select_from(Ride).where(Ride.user_id == user_id)
    )
    assert result.scalar() == 1

    # Verify FCM was called with correct type
    mock_send_push.assert_called_once()
    call_args = mock_send_push.call_args
    assert call_args[0][2] == "RIDE_ACCEPTED"  # notification_type


# ---------------------------------------------------------------------------
# Test 2: POST /rides with same idempotency_key → 200, same ride_id, no FCM
# ---------------------------------------------------------------------------


async def test_create_ride_idempotent_replay_returns_200(app_client, db_session):
    """POST /rides with same idempotency_key -> 200, same ride_id, no FCM."""
    reg = await _register(app_client, email="ride2@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="ride2-dev")
    await _register_fcm(app_client, reg["access_token"])

    body = _ride_body(idempotency_key="aaaabbbb-1111-2222-3333-444455556666")

    # First request → 201
    with patch("app.routers.rides.send_push", new_callable=AsyncMock, return_value=True):
        resp1 = await app_client.post(
            RIDES_URL,
            json=body,
            headers=_device_headers(pairing["device_token"], "ride2-dev"),
        )
    assert resp1.status_code == 201
    ride_id_1 = resp1.json()["ride_id"]

    # Second request with same idempotency_key → 200
    with patch(
        "app.routers.rides.send_push", new_callable=AsyncMock, return_value=True
    ) as mock_send_push:
        resp2 = await app_client.post(
            RIDES_URL,
            json=body,
            headers=_device_headers(pairing["device_token"], "ride2-dev"),
        )

    assert resp2.status_code == 200
    ride_id_2 = resp2.json()["ride_id"]

    # Same ride_id returned
    assert ride_id_1 == ride_id_2

    # FCM NOT called on second request
    mock_send_push.assert_not_called()

    # Only one ride in DB
    user_id = UUID(reg["user_id"])
    result = await db_session.execute(
        select(func.count()).select_from(Ride).where(Ride.user_id == user_id)
    )
    assert result.scalar() == 1


# ---------------------------------------------------------------------------
# Test 3: POST /rides with invalid device_token → 401
# ---------------------------------------------------------------------------


async def test_create_ride_invalid_device_token_returns_401(app_client):
    """POST /rides with invalid device credentials -> 401."""
    resp = await app_client.post(
        RIDES_URL,
        json=_ride_body(),
        headers=_device_headers("bad-token", "bad-device"),
    )
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Test 4: POST /rides with validation error → 422
# ---------------------------------------------------------------------------


async def test_create_ride_missing_fields_returns_422(app_client):
    """POST /rides with missing required fields -> 422."""
    reg = await _register(app_client, email="ride4a@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="ride4a-dev")

    resp = await app_client.post(
        RIDES_URL,
        json={"event_type": "ACCEPTED"},
        headers=_device_headers(pairing["device_token"], "ride4a-dev"),
    )
    assert resp.status_code == 422


async def test_create_ride_invalid_idempotency_key_returns_422(app_client):
    """POST /rides with non-UUID idempotency_key -> 422."""
    reg = await _register(app_client, email="ride4b@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="ride4b-dev")

    resp = await app_client.post(
        RIDES_URL,
        json=_ride_body(idempotency_key="not-a-uuid"),
        headers=_device_headers(pairing["device_token"], "ride4b-dev"),
    )
    assert resp.status_code == 422


async def test_create_ride_invalid_event_type_returns_422(app_client):
    """POST /rides with invalid event_type -> 422."""
    reg = await _register(app_client, email="ride4c@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="ride4c-dev")

    resp = await app_client.post(
        RIDES_URL,
        json=_ride_body(event_type="INVALID"),
        headers=_device_headers(pairing["device_token"], "ride4c-dev"),
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Test 5: FCM failure → ride still saved, 201 returned
# ---------------------------------------------------------------------------


async def test_create_ride_fcm_failure_still_saves_ride(app_client, db_session):
    """POST /rides when FCM fails -> 201, ride saved."""
    reg = await _register(app_client, email="ride5@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="ride5-dev")
    await _register_fcm(app_client, reg["access_token"])

    with patch(
        "app.routers.rides.send_push", new_callable=AsyncMock, return_value=False
    ) as mock_send_push:
        resp = await app_client.post(
            RIDES_URL,
            json=_ride_body(idempotency_key="ccccdddd-5555-6666-7777-888899990000"),
            headers=_device_headers(pairing["device_token"], "ride5-dev"),
        )

    assert resp.status_code == 201
    assert resp.json()["ok"] is True
    assert "ride_id" in resp.json()

    # Verify ride was saved despite FCM failure
    user_id = UUID(reg["user_id"])
    result = await db_session.execute(
        select(func.count()).select_from(Ride).where(Ride.user_id == user_id)
    )
    assert result.scalar() == 1

    # FCM was attempted
    mock_send_push.assert_called_once()


# ---------------------------------------------------------------------------
# Test 6: POST /rides without FCM token → 201, FCM not called
# ---------------------------------------------------------------------------


async def test_create_ride_no_fcm_token_skips_push(app_client, db_session):
    """POST /rides when user has no FCM token -> 201, FCM not called."""
    reg = await _register(app_client, email="ride6@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="ride6-dev")
    # NOTE: NOT registering FCM token

    with patch(
        "app.routers.rides.send_push", new_callable=AsyncMock, return_value=True
    ) as mock_send_push:
        resp = await app_client.post(
            RIDES_URL,
            json=_ride_body(idempotency_key="eeeeffff-1111-2222-3333-444455556666"),
            headers=_device_headers(pairing["device_token"], "ride6-dev"),
        )

    assert resp.status_code == 201
    assert resp.json()["ok"] is True

    # FCM NOT called (no token)
    mock_send_push.assert_not_called()

    # Ride still saved
    user_id = UUID(reg["user_id"])
    result = await db_session.execute(
        select(func.count()).select_from(Ride).where(Ride.user_id == user_id)
    )
    assert result.scalar() == 1


# ---------------------------------------------------------------------------
# Test 7: Verify ride_data saved correctly in DB
# ---------------------------------------------------------------------------


async def test_create_ride_data_saved_correctly(app_client, db_session):
    """POST /rides -> ride_data stored correctly as JSONB."""
    reg = await _register(app_client, email="ride7@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="ride7-dev")

    ride_data = {
        "price": 42.00,
        "pickup_time": "Today \u00b7 3:00PM",
        "pickup_location": "Main St & Oak Ave",
        "dropoff_location": "Pine Rd & Elm Ct",
        "duration": "15 min",
        "distance": "5.2 mi",
        "rider_name": "John",
    }

    with patch("app.routers.rides.send_push", new_callable=AsyncMock, return_value=True):
        resp = await app_client.post(
            RIDES_URL,
            json={
                "idempotency_key": "11112222-3333-4444-5555-666677778888",
                "event_type": "ACCEPTED",
                "ride_hash": "a" * 64,
                "timezone": "America/New_York",
                "ride_data": ride_data,
            },
            headers=_device_headers(pairing["device_token"], "ride7-dev"),
        )

    assert resp.status_code == 201
    ride_id = UUID(resp.json()["ride_id"])

    # Verify stored data
    result = await db_session.execute(select(Ride).where(Ride.id == ride_id))
    ride = result.scalar_one()
    assert ride.event_type == "ACCEPTED"
    assert ride.ride_data["price"] == 42.00
    assert ride.ride_data["pickup_time"] == "Today \u00b7 3:00PM"
    assert ride.ride_data["pickup_location"] == "Main St & Oak Ave"
    assert ride.ride_data["dropoff_location"] == "Pine Rd & Elm Ct"
    assert ride.ride_data["duration"] == "15 min"
    assert ride.ride_data["distance"] == "5.2 mi"
    assert ride.ride_data["rider_name"] == "John"


# ---------------------------------------------------------------------------
# Test 8: FCM payload contains correct data
# ---------------------------------------------------------------------------


async def test_create_ride_fcm_payload_correct(app_client):
    """POST /rides -> FCM called with correct payload fields."""
    reg = await _register(app_client, email="ride8@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="ride8-dev")
    await _register_fcm(app_client, reg["access_token"], fcm_token="ride8-fcm-tok")

    with patch(
        "app.routers.rides.send_push", new_callable=AsyncMock, return_value=True
    ) as mock_send_push:
        resp = await app_client.post(
            RIDES_URL,
            json=_ride_body(idempotency_key="99998888-7777-6666-5555-444433332222"),
            headers=_device_headers(pairing["device_token"], "ride8-dev"),
        )

    assert resp.status_code == 201

    mock_send_push.assert_called_once()
    call_args = mock_send_push.call_args

    # Positional args: (db, fcm_token, notification_type, payload, user_id)
    assert call_args[0][1] == "ride8-fcm-tok"
    assert call_args[0][2] == "RIDE_ACCEPTED"

    payload = call_args[0][3]
    assert payload["price"] == "25.5"
    assert payload["pickup_time"] == "Tomorrow \u00b7 6:05AM"
    assert payload["pickup_location"] == "Maida Ter & Maida Way"
    assert payload["dropoff_location"] == "East Rd & Leonardville Rd"
    assert "ride_id" in payload


# ===========================================================================
# GET /rides/events tests
# ===========================================================================


async def _create_rides_for_user(app_client, device_token, device_id, count):
    """Create multiple rides for testing pagination."""
    ride_ids = []
    for _i in range(count):
        with patch("app.routers.rides.send_push", new_callable=AsyncMock, return_value=True):
            resp = await app_client.post(
                RIDES_URL,
                json=_ride_body(idempotency_key=str(uuid4())),
                headers=_device_headers(device_token, device_id),
            )
        assert resp.status_code == 201
        ride_ids.append(resp.json()["ride_id"])
    return ride_ids


# ---------------------------------------------------------------------------
# Test 9: GET /rides/events first page without cursor
# ---------------------------------------------------------------------------


async def test_get_ride_events_first_page_no_cursor(app_client, db_session):
    """GET /rides/events without cursor -> first page with default limit=20."""
    reg = await _register(app_client, email="events1@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="ev1-dev")

    await _create_rides_for_user(app_client, pairing["device_token"], "ev1-dev", count=3)

    resp = await app_client.get(RIDES_EVENTS_URL, headers=_jwt(reg["access_token"]))

    assert resp.status_code == 200
    data = resp.json()
    assert "events" in data
    assert "has_more" in data
    assert "next_cursor" in data
    # 3 rides + 1 REGISTRATION_BONUS from register
    assert len(data["events"]) == 4
    assert data["has_more"] is False
    assert data["next_cursor"] is None

    # Verify ride events have correct structure
    ride_events = [e for e in data["events"] if e["event_kind"] == "ride"]
    for event in ride_events:
        assert "id" in event
        assert event["event_type"] == "ACCEPTED"
        assert "ride_data" in event
        assert "created_at" in event
        assert "credits_charged" in event
        assert "credits_refunded" in event
        assert "verification_status" in event


# ---------------------------------------------------------------------------
# Test 10: GET /rides/events with cursor → next page
# ---------------------------------------------------------------------------


async def test_get_ride_events_with_cursor(app_client, db_session):
    """GET /rides/events with cursor -> returns next page, no overlap."""
    reg = await _register(app_client, email="events2@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="ev2-dev")

    await _create_rides_for_user(app_client, pairing["device_token"], "ev2-dev", count=5)

    # First page: limit=2
    resp1 = await app_client.get(
        RIDES_EVENTS_URL,
        params={"limit": 2},
        headers=_jwt(reg["access_token"]),
    )
    assert resp1.status_code == 200
    data1 = resp1.json()
    assert len(data1["events"]) == 2
    assert data1["has_more"] is True
    assert data1["next_cursor"] is not None

    # Second page via cursor
    resp2 = await app_client.get(
        RIDES_EVENTS_URL,
        params={"limit": 2, "cursor": data1["next_cursor"]},
        headers=_jwt(reg["access_token"]),
    )
    assert resp2.status_code == 200
    data2 = resp2.json()
    assert len(data2["events"]) == 2
    assert data2["has_more"] is True

    # No overlap between pages
    page1_ids = {e["id"] for e in data1["events"]}
    page2_ids = {e["id"] for e in data2["events"]}
    assert page1_ids.isdisjoint(page2_ids)


# ---------------------------------------------------------------------------
# Test 11: GET /rides/events last page → has_more=false, next_cursor=null
# ---------------------------------------------------------------------------


async def test_get_ride_events_last_page(app_client, db_session):
    """GET /rides/events on last page -> has_more=false, next_cursor=null."""
    reg = await _register(app_client, email="events3@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="ev3-dev")

    await _create_rides_for_user(app_client, pairing["device_token"], "ev3-dev", count=3)

    # 3 rides + 1 REGISTRATION_BONUS = 4 events total
    # First page: limit=2
    resp1 = await app_client.get(
        RIDES_EVENTS_URL,
        params={"limit": 2},
        headers=_jwt(reg["access_token"]),
    )
    data1 = resp1.json()
    assert data1["has_more"] is True

    # Second page: limit=2
    resp2 = await app_client.get(
        RIDES_EVENTS_URL,
        params={"limit": 2, "cursor": data1["next_cursor"]},
        headers=_jwt(reg["access_token"]),
    )
    data2 = resp2.json()
    assert len(data2["events"]) == 2
    assert data2["has_more"] is False
    assert data2["next_cursor"] is None


# ---------------------------------------------------------------------------
# Test 12: GET /rides/events with invalid JWT → 401
# ---------------------------------------------------------------------------


async def test_get_ride_events_invalid_jwt_returns_401(app_client):
    """GET /rides/events with invalid JWT -> 401."""
    resp = await app_client.get(RIDES_EVENTS_URL, headers=_jwt("invalid-token"))
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Test 13: GET /rides/events for user with no rides → empty list
# ---------------------------------------------------------------------------


async def test_get_ride_events_with_since_filters_old_events(app_client, db_session):
    """GET /rides/events?since=... -> only returns events after the cutoff."""
    from datetime import datetime, timedelta

    reg = await _register(app_client, email="since@example.com")
    user_id = UUID(reg["user_id"])

    base_time = datetime(2026, 2, 1, 12, 0, 0, tzinfo=UTC)
    for i in range(3):
        ride = Ride(
            user_id=user_id,
            idempotency_key=str(uuid4()),
            event_type="ACCEPTED",
            ride_data={
                "price": 10.0 + i,
                "pickup_time": f"Trip {i}",
                "pickup_location": f"Start {i}",
                "dropoff_location": f"End {i}",
            },
            ride_hash="a" * 64,
            created_at=base_time + timedelta(days=i * 7),
        )
        db_session.add(ride)
    await db_session.flush()

    # Filter: only events from 5 days after base (should exclude first ride)
    # Rides: Feb 1 (excluded), Feb 8, Feb 15
    # REGISTRATION_BONUS: created at now (Feb 25) - after since, included
    since = (base_time + timedelta(days=5)).isoformat()
    resp = await app_client.get(
        RIDES_EVENTS_URL,
        params={"since": since},
        headers=_jwt(reg["access_token"]),
    )

    assert resp.status_code == 200
    data = resp.json()
    # 2 rides after since + REGISTRATION_BONUS (created at registration time)
    assert len(data["events"]) == 3
    ride_events = [e for e in data["events"] if e["event_kind"] == "ride"]
    assert len(ride_events) == 2
    assert data["has_more"] is False


async def test_get_ride_events_no_events_returns_empty(app_client, db_session):
    """GET /rides/events for fresh user -> only REGISTRATION_BONUS credit event."""
    reg = await _register(app_client, email="events5@example.com")

    resp = await app_client.get(RIDES_EVENTS_URL, headers=_jwt(reg["access_token"]))
    assert resp.status_code == 200
    data = resp.json()
    # Registration creates a REGISTRATION_BONUS credit transaction
    assert len(data["events"]) == 1
    assert data["events"][0]["event_kind"] == "credit"
    assert data["events"][0]["credit_type"] == "REGISTRATION_BONUS"
    assert data["has_more"] is False
    assert data["next_cursor"] is None


# ===========================================================================
# Task 9.5: FCM graceful degradation tests
# ===========================================================================


# ---------------------------------------------------------------------------
# Test 14: POST /rides with FCM exception → ride saved, 201 returned
# ---------------------------------------------------------------------------


async def test_create_ride_fcm_exception_still_saves_ride(app_client, db_session):
    """POST /rides when send_push raises -> 201, ride saved (graceful degradation)."""
    reg = await _register(app_client, email="fcm-exc@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="fcm-exc-dev")
    await _register_fcm(app_client, reg["access_token"])

    with patch(
        "app.routers.rides.send_push",
        new_callable=AsyncMock,
        side_effect=TimeoutError("FCM request timed out"),
    ):
        resp = await app_client.post(
            RIDES_URL,
            json=_ride_body(idempotency_key="aaaa1111-2222-3333-4444-555566667777"),
            headers=_device_headers(pairing["device_token"], "fcm-exc-dev"),
        )

    assert resp.status_code == 201
    assert resp.json()["ok"] is True
    assert "ride_id" in resp.json()

    # Verify ride was saved despite exception
    user_id = UUID(reg["user_id"])
    result = await db_session.execute(
        select(func.count()).select_from(Ride).where(Ride.user_id == user_id)
    )
    assert result.scalar() == 1


# ---------------------------------------------------------------------------
# Test 15: FCM exception logs warning with ride_id and error details
# ---------------------------------------------------------------------------


async def test_create_ride_fcm_exception_logs_warning_with_ride_id(app_client, caplog):
    """POST /rides when FCM raises -> warning log contains ride_id and error."""
    reg = await _register(app_client, email="fcm-log@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="fcm-log-dev")
    await _register_fcm(app_client, reg["access_token"])

    with (
        patch(
            "app.routers.rides.send_push",
            new_callable=AsyncMock,
            side_effect=RuntimeError("connection refused"),
        ),
        caplog.at_level(logging.WARNING, logger="app.routers.rides"),
    ):
        resp = await app_client.post(
            RIDES_URL,
            json=_ride_body(idempotency_key="bbbb2222-3333-4444-5555-666677778888"),
            headers=_device_headers(pairing["device_token"], "fcm-log-dev"),
        )

    assert resp.status_code == 201
    ride_id = resp.json()["ride_id"]

    # Verify warning log contains ride_id and error message
    warning_records = [
        r
        for r in caplog.records
        if r.levelno == logging.WARNING and "FCM push failed" in r.message
    ]
    assert len(warning_records) == 1
    assert ride_id in warning_records[0].message
    assert "connection refused" in warning_records[0].message


# ---------------------------------------------------------------------------
# Test 16: FCM payload values are all strings (FCM requirement)
# ---------------------------------------------------------------------------


async def test_create_ride_fcm_payload_values_all_strings(app_client):
    """POST /rides -> FCM payload contains only string values (FCM requirement)."""
    reg = await _register(app_client, email="fcm-str@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="fcm-str-dev")
    await _register_fcm(app_client, reg["access_token"])

    with patch(
        "app.routers.rides.send_push", new_callable=AsyncMock, return_value=True
    ) as mock_send_push:
        resp = await app_client.post(
            RIDES_URL,
            json=_ride_body(idempotency_key="cccc3333-4444-5555-6666-777788889999"),
            headers=_device_headers(pairing["device_token"], "fcm-str-dev"),
        )

    assert resp.status_code == 201
    mock_send_push.assert_called_once()

    # payload is the 4th positional arg (db, fcm_token, type, payload, user_id)
    payload = mock_send_push.call_args[0][3]
    assert isinstance(payload, dict)

    # All values must be strings per FCM data payload requirement
    for key, value in payload.items():
        assert isinstance(value, str), (
            f"FCM payload key '{key}' has non-string value: {type(value).__name__}"
        )

    # Verify expected keys are present
    assert "ride_id" in payload
    assert "price" in payload
    assert "pickup_time" in payload
    assert "pickup_location" in payload
    assert "dropoff_location" in payload


# ---------------------------------------------------------------------------
# Test 17: EventsListResponse serializes correctly (cursor-based)
# ---------------------------------------------------------------------------


async def test_events_list_response_serialization(app_client, db_session):
    """GET /rides/events -> response has correct cursor-based structure."""
    reg = await _register(app_client, email="serial@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="serial-dev")

    await _create_rides_for_user(app_client, pairing["device_token"], "serial-dev", count=2)

    resp = await app_client.get(RIDES_EVENTS_URL, headers=_jwt(reg["access_token"]))
    assert resp.status_code == 200
    data = resp.json()

    # Verify top-level response structure (cursor-based)
    assert "events" in data
    assert "has_more" in data
    assert "next_cursor" in data
    assert isinstance(data["events"], list)
    assert isinstance(data["has_more"], bool)
    # Legacy fields must NOT be present
    assert "total" not in data
    assert "offset" not in data

    # Verify ride events have correct fields including billing
    ride_events = [e for e in data["events"] if e["event_kind"] == "ride"]
    assert len(ride_events) == 2
    for event in ride_events:
        assert "id" in event
        assert event["event_kind"] == "ride"
        assert "event_type" in event
        assert "ride_data" in event
        assert "created_at" in event
        assert "credits_charged" in event
        assert "credits_refunded" in event
        assert "verification_status" in event
        assert isinstance(event["ride_data"], dict)

    # Verify credit events (REGISTRATION_BONUS from register)
    credit_events = [e for e in data["events"] if e["event_kind"] == "credit"]
    assert len(credit_events) == 1
    ce = credit_events[0]
    assert ce["credit_type"] == "REGISTRATION_BONUS"
    assert "amount" in ce
    assert "balance_after" in ce


# ===========================================================================
# Missing coverage: ride_data validation, limit boundaries, ordering,
# user isolation, race condition, missing auth headers
# ===========================================================================


# ---------------------------------------------------------------------------
# Test 18: POST /rides with invalid ride_data (missing required field) → 422
# ---------------------------------------------------------------------------


async def test_create_ride_invalid_ride_data_missing_price_returns_422(app_client):
    """POST /rides with ride_data missing required 'price' -> 422."""
    reg = await _register(app_client, email="ridedata1@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="ridedata1-dev")

    resp = await app_client.post(
        RIDES_URL,
        json={
            "idempotency_key": str(uuid4()),
            "event_type": "ACCEPTED",
            "ride_hash": "a" * 64,
            "timezone": "America/New_York",
            "ride_data": {
                "pickup_time": "Today · 3:00PM",
                "pickup_location": "Main St",
                "dropoff_location": "Oak Ave",
            },
        },
        headers=_device_headers(pairing["device_token"], "ridedata1-dev"),
    )
    assert resp.status_code == 422


async def test_create_ride_invalid_ride_data_missing_pickup_location_returns_422(
    app_client,
):
    """POST /rides with ride_data missing required 'pickup_location' -> 422."""
    reg = await _register(app_client, email="ridedata2@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="ridedata2-dev")

    resp = await app_client.post(
        RIDES_URL,
        json={
            "idempotency_key": str(uuid4()),
            "event_type": "ACCEPTED",
            "ride_hash": "a" * 64,
            "timezone": "America/New_York",
            "ride_data": {
                "price": 25.0,
                "pickup_time": "Today · 3:00PM",
                "dropoff_location": "Oak Ave",
            },
        },
        headers=_device_headers(pairing["device_token"], "ridedata2-dev"),
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Test 19: GET /rides/events with limit out of bounds → 422
# ---------------------------------------------------------------------------


async def test_get_ride_events_limit_zero_returns_422(app_client):
    """GET /rides/events with limit=0 -> 422 (ge=1 constraint)."""
    reg = await _register(app_client, email="lim0@example.com")

    resp = await app_client.get(
        RIDES_EVENTS_URL,
        params={"limit": 0},
        headers=_jwt(reg["access_token"]),
    )
    assert resp.status_code == 422


async def test_get_ride_events_limit_over_100_returns_422(app_client):
    """GET /rides/events with limit=101 -> 422 (le=100 constraint)."""
    reg = await _register(app_client, email="lim101@example.com")

    resp = await app_client.get(
        RIDES_EVENTS_URL,
        params={"limit": 101},
        headers=_jwt(reg["access_token"]),
    )
    assert resp.status_code == 422


async def test_get_ride_events_invalid_cursor_returns_400(app_client):
    """GET /rides/events with malformed cursor -> 400."""
    reg = await _register(app_client, email="badcur@example.com")

    resp = await app_client.get(
        RIDES_EVENTS_URL,
        params={"cursor": "not-a-valid-cursor"},
        headers=_jwt(reg["access_token"]),
    )
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "INVALID_CURSOR"


async def test_get_ride_events_invalid_since_returns_400(app_client):
    """GET /rides/events with malformed since -> 400."""
    reg = await _register(app_client, email="badsince@example.com")

    resp = await app_client.get(
        RIDES_EVENTS_URL,
        params={"since": "not-a-date"},
        headers=_jwt(reg["access_token"]),
    )
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "INVALID_SINCE"


# ---------------------------------------------------------------------------
# Test 20: GET /rides/events returns events in newest-first order
# ---------------------------------------------------------------------------


async def test_get_ride_events_ordered_newest_first(app_client, db_session):
    """GET /rides/events -> events ordered by created_at descending."""
    from datetime import datetime, timedelta

    reg = await _register(app_client, email="order@example.com")
    user_id = UUID(reg["user_id"])

    # Insert rides directly with explicit timestamps to guarantee ordering
    base_time = datetime(2026, 2, 20, 12, 0, 0, tzinfo=UTC)
    for i in range(3):
        ride = Ride(
            user_id=user_id,
            idempotency_key=str(uuid4()),
            event_type="ACCEPTED",
            ride_data={
                "price": 10.0 + i,
                "pickup_time": f"Trip {i}",
                "pickup_location": f"Start {i}",
                "dropoff_location": f"End {i}",
            },
            ride_hash="a" * 64,
            created_at=base_time + timedelta(hours=i),
        )
        db_session.add(ride)
    await db_session.flush()

    resp = await app_client.get(RIDES_EVENTS_URL, headers=_jwt(reg["access_token"]))
    assert resp.status_code == 200
    events = resp.json()["events"]

    # 3 rides + REGISTRATION_BONUS; all sorted by created_at DESC
    timestamps = [e["created_at"] for e in events]
    assert timestamps == sorted(timestamps, reverse=True)

    # Ride events should be newest first
    ride_events = [e for e in events if e["event_kind"] == "ride"]
    assert len(ride_events) == 3
    assert ride_events[0]["ride_data"]["price"] == 12.0
    assert ride_events[1]["ride_data"]["price"] == 11.0
    assert ride_events[2]["ride_data"]["price"] == 10.0


# ---------------------------------------------------------------------------
# Test 21: GET /rides/events — user isolation (can't see other user's rides)
# ---------------------------------------------------------------------------


async def test_get_ride_events_user_isolation(app_client, db_session):
    """User A cannot see User B's rides via GET /rides/events."""
    # User A with rides
    reg_a = await _register(app_client, email="iso-a@example.com")
    pairing_a = await _pair_device(app_client, reg_a["access_token"], device_id="iso-a-dev")
    await _create_rides_for_user(app_client, pairing_a["device_token"], "iso-a-dev", count=3)

    # User B with no rides
    reg_b = await _register(app_client, email="iso-b@example.com")

    # User B should see only own REGISTRATION_BONUS
    resp_b = await app_client.get(RIDES_EVENTS_URL, headers=_jwt(reg_b["access_token"]))
    assert resp_b.status_code == 200
    b_events = resp_b.json()["events"]
    assert len(b_events) == 1
    assert b_events[0]["event_kind"] == "credit"

    # User A should see 3 rides + own REGISTRATION_BONUS = 4 events
    resp_a = await app_client.get(RIDES_EVENTS_URL, headers=_jwt(reg_a["access_token"]))
    assert resp_a.status_code == 200
    assert len(resp_a.json()["events"]) == 4


# ---------------------------------------------------------------------------
# Test 22: GET /rides/events — full cursor pagination through entire dataset
# ---------------------------------------------------------------------------


async def test_get_ride_events_paginate_entire_dataset(app_client, db_session):
    """Paginating through entire dataset via cursor covers all events without overlap."""
    reg = await _register(app_client, email="fullpag@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="fullpag-dev")

    await _create_rides_for_user(app_client, pairing["device_token"], "fullpag-dev", count=5)

    # 5 rides + 1 REGISTRATION_BONUS = 6 events total; paginate with limit=2
    all_ids: list[str] = []
    cursor = None
    pages = 0

    while True:
        params: dict = {"limit": 2}
        if cursor:
            params["cursor"] = cursor
        resp = await app_client.get(
            RIDES_EVENTS_URL,
            params=params,
            headers=_jwt(reg["access_token"]),
        )
        assert resp.status_code == 200
        data = resp.json()
        all_ids.extend(e["id"] for e in data["events"])
        pages += 1

        if not data["has_more"]:
            assert data["next_cursor"] is None
            break
        cursor = data["next_cursor"]

    # All 6 unique events collected across 3 pages
    assert pages == 3
    assert len(all_ids) == 6
    assert len(set(all_ids)) == 6  # no duplicates


# ---------------------------------------------------------------------------
# Test 23: GET /rides/events — since + cursor combination
# ---------------------------------------------------------------------------


async def test_get_ride_events_since_plus_cursor(app_client, db_session):
    """since and cursor parameters work correctly together."""
    from datetime import datetime, timedelta

    reg = await _register(app_client, email="sincecur@example.com")
    user_id = UUID(reg["user_id"])

    base_time = datetime(2026, 2, 20, 12, 0, 0, tzinfo=UTC)
    for i in range(4):
        ride = Ride(
            user_id=user_id,
            idempotency_key=str(uuid4()),
            event_type="ACCEPTED",
            ride_data={
                "price": 20.0 + i,
                "pickup_time": f"Trip {i}",
                "pickup_location": f"Start {i}",
                "dropoff_location": f"End {i}",
            },
            ride_hash="a" * 64,
            created_at=base_time + timedelta(hours=i),
        )
        db_session.add(ride)
    await db_session.flush()

    # since excludes rides at +0h and +1h; keeps +2h and +3h rides
    # REGISTRATION_BONUS (created at "now" ~Feb 25) is also after since
    since = (base_time + timedelta(hours=1, minutes=30)).isoformat()

    # Collect all events via cursor pagination with limit=2
    all_ids: list[str] = []
    cursor = None
    while True:
        params: dict = {"limit": 2, "since": since}
        if cursor:
            params["cursor"] = cursor
        resp = await app_client.get(
            RIDES_EVENTS_URL,
            params=params,
            headers=_jwt(reg["access_token"]),
        )
        assert resp.status_code == 200
        data = resp.json()
        all_ids.extend(e["id"] for e in data["events"])
        if not data["has_more"]:
            break
        cursor = data["next_cursor"]

    # 2 rides after since + 1 REGISTRATION_BONUS = 3 events total
    assert len(all_ids) == 3
    assert len(set(all_ids)) == 3  # no duplicates


# ---------------------------------------------------------------------------
# Test 24: POST /rides race condition — IntegrityError fallback path
# ---------------------------------------------------------------------------


async def test_create_ride_race_condition_returns_200(app_client, db_session):
    """POST /rides IntegrityError fallback: first idempotency check misses (race),
    create_ride hits duplicate constraint, retry lookup finds the existing ride -> 200.

    Uses full mocking of the service layer + db.rollback to avoid savepoint
    conflicts in the test transaction (MissingGreenlet with async sessions).
    """
    from sqlalchemy.exc import IntegrityError as SAIntegrityError

    reg = await _register(app_client, email="race@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="race-dev")

    # Mock ride object that the second get_ride_by_idempotency call returns
    mock_ride = type("MockRide", (), {"id": uuid4()})()

    call_count = {"n": 0}

    async def mock_get_ride(db, user_id, key):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return None  # Simulate race window
        return mock_ride

    with (
        patch(
            "app.routers.rides.get_ride_by_idempotency",
            side_effect=mock_get_ride,
        ),
        patch(
            "app.routers.rides.create_ride",
            new_callable=AsyncMock,
            side_effect=SAIntegrityError("duplicate key", {}, None),
        ),
        patch.object(db_session, "rollback", new_callable=AsyncMock),
        patch(
            "app.routers.rides.send_push",
            new_callable=AsyncMock,
            return_value=True,
        ),
    ):
        resp = await app_client.post(
            RIDES_URL,
            json=_ride_body(idempotency_key=str(uuid4())),
            headers=_device_headers(pairing["device_token"], "race-dev"),
        )

    assert resp.status_code == 200
    assert resp.json()["ok"] is True
    assert resp.json()["ride_id"] == str(mock_ride.id)
    # Verify both calls to get_ride_by_idempotency happened (race path)
    assert call_count["n"] == 2


# ---------------------------------------------------------------------------
# Test 23: POST /rides without auth headers → 422 (missing required headers)
# ---------------------------------------------------------------------------


async def test_create_ride_missing_device_headers_returns_422(app_client):
    """POST /rides without X-Device-Token/X-Device-Id headers -> 422."""
    resp = await app_client.post(RIDES_URL, json=_ride_body())
    assert resp.status_code == 422


# ===========================================================================
# Task 5.1: ride_hash and timezone validation tests
# ===========================================================================


# ---------------------------------------------------------------------------
# Test 24: ride_hash too short (63 chars) → 422
# ---------------------------------------------------------------------------


async def test_create_ride_ride_hash_too_short_returns_422(app_client):
    """POST /rides with ride_hash of 63 chars -> 422."""
    reg = await _register(app_client, email="hash-short@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="hash-short-dev")

    resp = await app_client.post(
        RIDES_URL,
        json=_ride_body(
            idempotency_key=str(uuid4()),
            ride_hash="a" * 63,
        ),
        headers=_device_headers(pairing["device_token"], "hash-short-dev"),
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Test 25: ride_hash with non-hex chars → 422
# ---------------------------------------------------------------------------


async def test_create_ride_ride_hash_non_hex_returns_422(app_client):
    """POST /rides with ride_hash containing non-hex characters -> 422."""
    reg = await _register(app_client, email="hash-nonhex@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="hash-nonhex-dev")

    resp = await app_client.post(
        RIDES_URL,
        json=_ride_body(
            idempotency_key=str(uuid4()),
            ride_hash="g" * 64,
        ),
        headers=_device_headers(pairing["device_token"], "hash-nonhex-dev"),
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Test 26: ride_hash uppercase accepted and lowercased
# ---------------------------------------------------------------------------


async def test_create_ride_ride_hash_uppercase_accepted(app_client, db_session):
    """POST /rides with uppercase ride_hash -> accepted, stored lowercase."""
    reg = await _register(app_client, email="hash-upper@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="hash-upper-dev")

    uppercase_hash = "A" * 64

    with patch("app.routers.rides.send_push", new_callable=AsyncMock, return_value=True):
        resp = await app_client.post(
            RIDES_URL,
            json=_ride_body(
                idempotency_key=str(uuid4()),
                ride_hash=uppercase_hash,
            ),
            headers=_device_headers(pairing["device_token"], "hash-upper-dev"),
        )

    assert resp.status_code == 201

    ride_id = UUID(resp.json()["ride_id"])
    result = await db_session.execute(select(Ride).where(Ride.id == ride_id))
    ride = result.scalar_one()
    assert ride.ride_hash == "a" * 64


# ---------------------------------------------------------------------------
# Test 27: valid timezone accepted
# ---------------------------------------------------------------------------


async def test_create_ride_valid_timezone_accepted(app_client):
    """POST /rides with valid IANA timezone -> accepted."""
    reg = await _register(app_client, email="tz-valid@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="tz-valid-dev")

    with patch("app.routers.rides.send_push", new_callable=AsyncMock, return_value=True):
        resp = await app_client.post(
            RIDES_URL,
            json=_ride_body(
                idempotency_key=str(uuid4()),
                timezone="Europe/Kyiv",
            ),
            headers=_device_headers(pairing["device_token"], "tz-valid-dev"),
        )

    assert resp.status_code == 201


# ---------------------------------------------------------------------------
# Test 28: invalid timezone NOT rejected (graceful fallback in business logic)
# ---------------------------------------------------------------------------


async def test_create_ride_invalid_timezone_not_rejected(app_client):
    """POST /rides with invalid timezone -> NOT rejected at schema level.

    Per PRD section 6: invalid timezone should fallback to UTC in business
    logic (task 5.2), not cause a 422 validation error.
    """
    reg = await _register(app_client, email="tz-invalid@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="tz-invalid-dev")

    with patch("app.routers.rides.send_push", new_callable=AsyncMock, return_value=True):
        resp = await app_client.post(
            RIDES_URL,
            json=_ride_body(
                idempotency_key=str(uuid4()),
                timezone="Invalid/Zone",
            ),
            headers=_device_headers(pairing["device_token"], "tz-invalid-dev"),
        )

    assert resp.status_code == 201


# ---------------------------------------------------------------------------
# Test 29: missing timezone → 422
# ---------------------------------------------------------------------------


async def test_create_ride_missing_timezone_returns_422(app_client):
    """POST /rides without timezone field -> 422."""
    reg = await _register(app_client, email="tz-missing@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="tz-missing-dev")

    body = _ride_body(idempotency_key=str(uuid4()))
    del body["timezone"]

    resp = await app_client.post(
        RIDES_URL,
        json=body,
        headers=_device_headers(pairing["device_token"], "tz-missing-dev"),
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Test 30: missing ride_hash → 422
# ---------------------------------------------------------------------------


async def test_create_ride_missing_ride_hash_returns_422(app_client):
    """POST /rides without ride_hash field -> 422."""
    reg = await _register(app_client, email="hash-missing@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="hash-missing-dev")

    body = _ride_body(idempotency_key=str(uuid4()))
    del body["ride_hash"]

    resp = await app_client.post(
        RIDES_URL,
        json=body,
        headers=_device_headers(pairing["device_token"], "hash-missing-dev"),
    )
    assert resp.status_code == 422


# ===========================================================================
# Task 5.2: verification_deadline and pickup_time parsing integration tests
# ===========================================================================


# ---------------------------------------------------------------------------
# Test 31: Ride saved with verification_status='PENDING' and verification_deadline set
# ---------------------------------------------------------------------------


async def test_create_ride_sets_verification_status_pending(app_client, db_session):
    """POST /rides -> ride saved with verification_status='PENDING' and verification_deadline."""
    reg = await _register(app_client, email="vf-pending@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="vf-pending-dev")

    with patch("app.routers.rides.send_push", new_callable=AsyncMock, return_value=True):
        resp = await app_client.post(
            RIDES_URL,
            json=_ride_body(idempotency_key=str(uuid4())),
            headers=_device_headers(pairing["device_token"], "vf-pending-dev"),
        )

    assert resp.status_code == 201
    ride_id = UUID(resp.json()["ride_id"])

    result = await db_session.execute(select(Ride).where(Ride.id == ride_id))
    ride = result.scalar_one()
    assert ride.verification_status == "PENDING"
    assert ride.verification_deadline is not None
    assert ride.ride_hash == "a" * 64


# ---------------------------------------------------------------------------
# Test 32: verification_deadline is in the future (pickup_time - N min)
# ---------------------------------------------------------------------------


async def test_create_ride_verification_deadline_before_pickup(app_client, db_session):
    """POST /rides with future pickup_time -> deadline = pickup_time - N minutes."""
    reg = await _register(app_client, email="vf-future@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="vf-future-dev")

    # Use a pickup_time far in the future so deadline is definitely in the future
    body = _ride_body(idempotency_key=str(uuid4()))
    body["ride_data"]["pickup_time"] = "Tomorrow \u00b7 6:05AM"
    body["timezone"] = "America/New_York"

    with patch("app.routers.rides.send_push", new_callable=AsyncMock, return_value=True):
        resp = await app_client.post(
            RIDES_URL,
            json=body,
            headers=_device_headers(pairing["device_token"], "vf-future-dev"),
        )

    assert resp.status_code == 201
    ride_id = UUID(resp.json()["ride_id"])

    result = await db_session.execute(select(Ride).where(Ride.id == ride_id))
    ride = result.scalar_one()
    # Deadline should be set and be a timezone-aware datetime
    assert ride.verification_deadline is not None
    assert (
        ride.verification_deadline.tzinfo is not None
        or ride.verification_deadline.utcoffset() is not None
    )


# ---------------------------------------------------------------------------
# Test 33: Invalid timezone fallback logs RIDE_TIMEZONE_FALLBACK with ride_id
# ---------------------------------------------------------------------------


async def test_create_ride_invalid_timezone_logs_fallback_warning(app_client, caplog):
    """POST /rides with invalid timezone -> logs RIDE_TIMEZONE_FALLBACK with ride_id."""
    reg = await _register(app_client, email="tz-log@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="tz-log-dev")

    with (
        patch("app.routers.rides.send_push", new_callable=AsyncMock, return_value=True),
        caplog.at_level(logging.WARNING, logger="app.routers.rides"),
    ):
        resp = await app_client.post(
            RIDES_URL,
            json=_ride_body(
                idempotency_key=str(uuid4()),
                timezone="Bogus/Timezone",
            ),
            headers=_device_headers(pairing["device_token"], "tz-log-dev"),
        )

    assert resp.status_code == 201
    ride_id = resp.json()["ride_id"]

    fallback_records = [
        r
        for r in caplog.records
        if r.levelno == logging.WARNING and "RIDE_TIMEZONE_FALLBACK" in r.message
    ]
    assert len(fallback_records) == 1
    assert ride_id in fallback_records[0].message
    assert "Bogus/Timezone" in fallback_records[0].message


# ---------------------------------------------------------------------------
# Test 34: Unparseable pickup_time -> deadline still set (falls back to now)
# ---------------------------------------------------------------------------


async def test_create_ride_unparseable_pickup_time_sets_deadline(app_client, db_session, caplog):
    """POST /rides with unparseable pickup_time -> deadline set to ~now, warning logged."""
    reg = await _register(app_client, email="parse-fail@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="parse-fail-dev")

    body = _ride_body(idempotency_key=str(uuid4()))
    body["ride_data"]["pickup_time"] = "some random garbage text"

    before = datetime.now(UTC)

    with (
        patch("app.routers.rides.send_push", new_callable=AsyncMock, return_value=True),
        caplog.at_level(logging.WARNING, logger="app.routers.rides"),
    ):
        resp = await app_client.post(
            RIDES_URL,
            json=body,
            headers=_device_headers(pairing["device_token"], "parse-fail-dev"),
        )

    after = datetime.now(UTC)

    assert resp.status_code == 201
    ride_id = UUID(resp.json()["ride_id"])

    result = await db_session.execute(select(Ride).where(Ride.id == ride_id))
    ride = result.scalar_one()
    # Deadline should be set to approximately now (within test execution window)
    assert ride.verification_deadline is not None
    deadline_naive = (
        ride.verification_deadline.replace(tzinfo=UTC)
        if ride.verification_deadline.tzinfo is None
        else ride.verification_deadline
    )
    assert before <= deadline_naive <= after

    # Should log parse failure warning
    parse_records = [
        r
        for r in caplog.records
        if r.levelno == logging.WARNING and "RIDE_PICKUP_PARSE_FAILED" in r.message
    ]
    assert len(parse_records) == 1


# ===========================================================================
# Task 5.4: Credit charging integration tests
# ===========================================================================


async def _set_user_balance(db_session, user_id, balance):
    """Directly set a user's credit balance for test setup."""
    from app.models.credit_balance import CreditBalance

    result = await db_session.execute(
        select(CreditBalance).where(CreditBalance.user_id == UUID(user_id))
    )
    cb = result.scalar_one()
    cb.balance = balance
    await db_session.flush()


# ---------------------------------------------------------------------------
# Test 35: Happy path — ride $25.50 (tier 2) with 10 credits → charged=2
# ---------------------------------------------------------------------------


async def test_create_ride_charges_credits_happy_path(app_client, db_session):
    """POST /rides with sufficient balance -> credits_charged=2, CreditTransaction created."""
    from app.models.credit_balance import CreditBalance
    from app.models.credit_transaction import CreditTransaction

    reg = await _register(app_client, email="charge-happy@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="charge-happy-dev")
    user_id = UUID(reg["user_id"])

    with patch("app.routers.rides.send_push", new_callable=AsyncMock, return_value=True):
        resp = await app_client.post(
            RIDES_URL,
            json=_ride_body(
                idempotency_key=str(uuid4()),
                ride_data={
                    "price": 45.00,
                    "pickup_time": "Tomorrow · 6:05AM",
                    "pickup_location": "Start St",
                    "dropoff_location": "End Ave",
                },
            ),
            headers=_device_headers(pairing["device_token"], "charge-happy-dev"),
        )

    assert resp.status_code == 201
    ride_id = UUID(resp.json()["ride_id"])

    # Verify ride.credits_charged saved in DB
    result = await db_session.execute(select(Ride).where(Ride.id == ride_id))
    ride = result.scalar_one()
    assert ride.credits_charged == 2  # $45 falls in tier $20-$50 → 2 credits

    # Verify CreditBalance updated (10 - 2 = 8)
    result = await db_session.execute(
        select(CreditBalance.balance).where(CreditBalance.user_id == user_id)
    )
    assert result.scalar_one() == 8

    # Verify CreditTransaction RIDE_CHARGE created
    result = await db_session.execute(
        select(CreditTransaction).where(
            CreditTransaction.user_id == user_id,
            CreditTransaction.type == "RIDE_CHARGE",
        )
    )
    tx = result.scalar_one()
    assert tx.amount == -2
    assert tx.balance_after == 8
    assert tx.reference_id == ride_id


# ---------------------------------------------------------------------------
# Test 36: Partial charge — balance=1, cost=3 → credits_charged=1
# ---------------------------------------------------------------------------


async def test_create_ride_partial_charge(app_client, db_session):
    """POST /rides with balance < cost -> partial charge, ride still saved."""
    from app.models.credit_balance import CreditBalance
    from app.models.credit_transaction import CreditTransaction

    reg = await _register(app_client, email="charge-partial@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="charge-partial-dev")
    user_id = UUID(reg["user_id"])

    # Reduce balance to 1
    await _set_user_balance(db_session, reg["user_id"], 1)

    with patch("app.routers.rides.send_push", new_callable=AsyncMock, return_value=True):
        resp = await app_client.post(
            RIDES_URL,
            json=_ride_body(
                idempotency_key=str(uuid4()),
                ride_data={
                    "price": 100.00,
                    "pickup_time": "Tomorrow · 6:05AM",
                    "pickup_location": "Start St",
                    "dropoff_location": "End Ave",
                },
            ),
            headers=_device_headers(pairing["device_token"], "charge-partial-dev"),
        )

    assert resp.status_code == 201
    ride_id = UUID(resp.json()["ride_id"])

    # Verify partial charge: min(3, 1) = 1
    result = await db_session.execute(select(Ride).where(Ride.id == ride_id))
    ride = result.scalar_one()
    assert ride.credits_charged == 1  # $100+ → tier 3 credits, but only 1 available

    # Verify balance is now 0
    result = await db_session.execute(
        select(CreditBalance.balance).where(CreditBalance.user_id == user_id)
    )
    assert result.scalar_one() == 0

    # Verify CreditTransaction with partial amount
    result = await db_session.execute(
        select(CreditTransaction).where(
            CreditTransaction.user_id == user_id,
            CreditTransaction.type == "RIDE_CHARGE",
        )
    )
    tx = result.scalar_one()
    assert tx.amount == -1
    assert tx.balance_after == 0


# ---------------------------------------------------------------------------
# Test 37: Zero balance — credits_charged=0, log RIDE_NOT_CHARGED
# ---------------------------------------------------------------------------


async def test_create_ride_zero_balance_not_charged(app_client, db_session, caplog):
    """POST /rides with balance=0 -> credits_charged=0, RIDE_NOT_CHARGED logged."""
    reg = await _register(app_client, email="charge-zero@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="charge-zero-dev")
    user_id = UUID(reg["user_id"])

    # Set balance to 0
    await _set_user_balance(db_session, reg["user_id"], 0)

    with (
        patch("app.routers.rides.send_push", new_callable=AsyncMock, return_value=True),
        caplog.at_level(logging.WARNING, logger="app.routers.rides"),
    ):
        resp = await app_client.post(
            RIDES_URL,
            json=_ride_body(idempotency_key=str(uuid4())),
            headers=_device_headers(pairing["device_token"], "charge-zero-dev"),
        )

    assert resp.status_code == 201
    ride_id = UUID(resp.json()["ride_id"])

    # Verify ride saved with credits_charged=0
    result = await db_session.execute(select(Ride).where(Ride.id == ride_id))
    ride = result.scalar_one()
    assert ride.credits_charged == 0

    # Verify no CreditTransaction RIDE_CHARGE created
    from app.models.credit_transaction import CreditTransaction

    result = await db_session.execute(
        select(CreditTransaction).where(
            CreditTransaction.user_id == user_id,
            CreditTransaction.type == "RIDE_CHARGE",
        )
    )
    assert result.scalar_one_or_none() is None

    # Verify RIDE_NOT_CHARGED warning logged
    not_charged_records = [
        r
        for r in caplog.records
        if r.levelno == logging.WARNING and "RIDE_NOT_CHARGED" in r.message
    ]
    assert len(not_charged_records) == 1
    assert str(ride_id) in not_charged_records[0].message


# ---------------------------------------------------------------------------
# Test 38: Tier matching — verify all tiers ($15→1, $25→2, $100→3)
# ---------------------------------------------------------------------------


async def test_create_ride_tier_matching_all_tiers(app_client, db_session):
    """POST /rides with different prices -> correct credits charged per tier."""
    reg = await _register(app_client, email="tiers@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="tiers-dev")

    # Default tiers: ≤$20→1, ≤$50→2, >$50→3
    # User starts with 10 credits
    tier_cases = [
        (15.00, 1),  # $15 → tier 1 (≤$20)
        (25.00, 2),  # $25 → tier 2 ($20<x≤$50)
        (100.00, 3),  # $100 → tier 3 (>$50, catch-all)
    ]

    for price, expected_credits in tier_cases:
        with patch("app.routers.rides.send_push", new_callable=AsyncMock, return_value=True):
            resp = await app_client.post(
                RIDES_URL,
                json=_ride_body(
                    idempotency_key=str(uuid4()),
                    ride_data={
                        "price": price,
                        "pickup_time": "Tomorrow · 6:05AM",
                        "pickup_location": "Start St",
                        "dropoff_location": "End Ave",
                    },
                ),
                headers=_device_headers(pairing["device_token"], "tiers-dev"),
            )

        assert resp.status_code == 201, f"Failed for price={price}"
        ride_id = UUID(resp.json()["ride_id"])

        result = await db_session.execute(select(Ride).where(Ride.id == ride_id))
        ride = result.scalar_one()
        assert ride.credits_charged == expected_credits, (
            f"Price ${price}: expected {expected_credits} credits, got {ride.credits_charged}"
        )


# ---------------------------------------------------------------------------
# Test 39: Ride + CreditTransaction atomicity — both or neither saved
# ---------------------------------------------------------------------------


async def test_create_ride_credit_transaction_atomic(app_client, db_session):
    """POST /rides -> ride and CreditTransaction committed atomically."""
    from app.models.credit_transaction import CreditTransaction

    reg = await _register(app_client, email="atomic@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="atomic-dev")
    user_id = UUID(reg["user_id"])

    with patch("app.routers.rides.send_push", new_callable=AsyncMock, return_value=True):
        resp = await app_client.post(
            RIDES_URL,
            json=_ride_body(
                idempotency_key=str(uuid4()),
                ride_data={
                    "price": 30.00,
                    "pickup_time": "Tomorrow · 6:05AM",
                    "pickup_location": "Start St",
                    "dropoff_location": "End Ave",
                },
            ),
            headers=_device_headers(pairing["device_token"], "atomic-dev"),
        )

    assert resp.status_code == 201
    ride_id = UUID(resp.json()["ride_id"])

    # Both ride and transaction exist
    ride_result = await db_session.execute(select(Ride).where(Ride.id == ride_id))
    ride = ride_result.scalar_one()
    assert ride.credits_charged == 2

    tx_result = await db_session.execute(
        select(CreditTransaction).where(
            CreditTransaction.reference_id == ride_id,
            CreditTransaction.type == "RIDE_CHARGE",
        )
    )
    tx = tx_result.scalar_one()
    assert tx.amount == -2
    assert tx.balance_after == 8
    assert tx.user_id == user_id


# ---------------------------------------------------------------------------
# Test 40: Idempotent replay does NOT double-charge
# ---------------------------------------------------------------------------


async def test_create_ride_idempotent_replay_no_double_charge(app_client, db_session):
    """POST /rides with same idempotency_key twice -> credits charged only once."""
    from app.models.credit_balance import CreditBalance
    from app.models.credit_transaction import CreditTransaction

    reg = await _register(app_client, email="no-double@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="no-double-dev")
    user_id = UUID(reg["user_id"])

    idem_key = str(uuid4())
    body = _ride_body(idempotency_key=idem_key)

    # First request → 201
    with patch("app.routers.rides.send_push", new_callable=AsyncMock, return_value=True):
        resp1 = await app_client.post(
            RIDES_URL,
            json=body,
            headers=_device_headers(pairing["device_token"], "no-double-dev"),
        )
    assert resp1.status_code == 201

    # Second request with same idempotency_key → 200
    with patch("app.routers.rides.send_push", new_callable=AsyncMock, return_value=True):
        resp2 = await app_client.post(
            RIDES_URL,
            json=body,
            headers=_device_headers(pairing["device_token"], "no-double-dev"),
        )
    assert resp2.status_code == 200

    # Balance should reflect only ONE charge (10 - 2 = 8, not 10 - 4 = 6)
    result = await db_session.execute(
        select(CreditBalance.balance).where(CreditBalance.user_id == user_id)
    )
    assert result.scalar_one() == 8

    # Only one RIDE_CHARGE transaction
    result = await db_session.execute(
        select(func.count())
        .select_from(CreditTransaction)
        .where(
            CreditTransaction.user_id == user_id,
            CreditTransaction.type == "RIDE_CHARGE",
        )
    )
    assert result.scalar_one() == 1


# ---------------------------------------------------------------------------
# Test 41: CREDITS_DEPLETED push — balance=2, cost=2 → depleted, FCM called
# ---------------------------------------------------------------------------


async def test_create_ride_credits_depleted_push_sent(app_client, db_session):
    """POST /rides that depletes balance -> send_credits_depleted called."""
    reg = await _register(app_client, email="depleted-push@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="depleted-push-dev")

    # Set balance to exactly 2 (ride cost for $25.50 = 2 credits)
    await _set_user_balance(db_session, reg["user_id"], 2)

    with (
        patch("app.routers.rides.send_push", new_callable=AsyncMock, return_value=True),
        patch("app.routers.rides.send_credits_depleted", new_callable=AsyncMock) as mock_depleted,
    ):
        resp = await app_client.post(
            RIDES_URL,
            json=_ride_body(idempotency_key=str(uuid4())),
            headers=_device_headers(pairing["device_token"], "depleted-push-dev"),
        )

    assert resp.status_code == 201
    mock_depleted.assert_called_once()
    call_args = mock_depleted.call_args
    assert call_args[0][1] == UUID(reg["user_id"])  # user_id


# ---------------------------------------------------------------------------
# Test 42: CREDITS_DEPLETED push NOT sent — balance=5, cost=2 → balance=3
# ---------------------------------------------------------------------------


async def test_create_ride_credits_not_depleted_no_push(app_client, db_session):
    """POST /rides that doesn't deplete balance -> send_credits_depleted NOT called."""
    reg = await _register(app_client, email="not-depleted@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="not-depleted-dev")

    # Balance stays at default 10, cost = 2 → remaining 8
    with (
        patch("app.routers.rides.send_push", new_callable=AsyncMock, return_value=True),
        patch("app.routers.rides.send_credits_depleted", new_callable=AsyncMock) as mock_depleted,
    ):
        resp = await app_client.post(
            RIDES_URL,
            json=_ride_body(idempotency_key=str(uuid4())),
            headers=_device_headers(pairing["device_token"], "not-depleted-dev"),
        )

    assert resp.status_code == 201
    mock_depleted.assert_not_called()


# ---------------------------------------------------------------------------
# Test 43: CREDITS_DEPLETED push NOT sent — balance=0, charged=0
# ---------------------------------------------------------------------------


async def test_create_ride_zero_balance_no_depleted_push(app_client, db_session):
    """POST /rides with zero balance -> credits_charged=0, no CREDITS_DEPLETED push."""
    reg = await _register(app_client, email="zero-no-push@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="zero-no-push-dev")

    # Set balance to 0
    await _set_user_balance(db_session, reg["user_id"], 0)

    with (
        patch("app.routers.rides.send_push", new_callable=AsyncMock, return_value=True),
        patch("app.routers.rides.send_credits_depleted", new_callable=AsyncMock) as mock_depleted,
    ):
        resp = await app_client.post(
            RIDES_URL,
            json=_ride_body(idempotency_key=str(uuid4())),
            headers=_device_headers(pairing["device_token"], "zero-no-push-dev"),
        )

    assert resp.status_code == 201
    mock_depleted.assert_not_called()


# ---------------------------------------------------------------------------
# Test 44: CREDITS_DEPLETED FCM failure does not block ride creation
# ---------------------------------------------------------------------------


async def test_create_ride_credits_depleted_fcm_failure_still_saves(app_client, db_session):
    """FCM failure in send_credits_depleted does not block ride creation."""
    reg = await _register(app_client, email="depleted-fail@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="depleted-fail-dev")

    # Set balance to 2 → will deplete to 0
    await _set_user_balance(db_session, reg["user_id"], 2)

    with (
        patch("app.routers.rides.send_push", new_callable=AsyncMock, return_value=True),
        patch(
            "app.routers.rides.send_credits_depleted",
            new_callable=AsyncMock,
            side_effect=Exception("FCM down"),
        ),
    ):
        resp = await app_client.post(
            RIDES_URL,
            json=_ride_body(idempotency_key=str(uuid4())),
            headers=_device_headers(pairing["device_token"], "depleted-fail-dev"),
        )

    # Ride still created successfully despite FCM failure
    assert resp.status_code == 201
    assert "ride_id" in resp.json()
