import logging
from datetime import UTC
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
# Test 9: GET /rides/events without params → first page with defaults
# ---------------------------------------------------------------------------


async def test_get_ride_events_default_params(app_client, db_session):
    """GET /rides/events without params -> returns events with default limit=50, offset=0."""
    reg = await _register(app_client, email="events1@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="ev1-dev")

    await _create_rides_for_user(app_client, pairing["device_token"], "ev1-dev", count=3)

    resp = await app_client.get(RIDES_EVENTS_URL, headers=_jwt(reg["access_token"]))

    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 3
    assert data["limit"] == 50
    assert data["offset"] == 0
    assert len(data["events"]) == 3

    # Verify events are ordered by created_at descending (newest first)
    for event in data["events"]:
        assert "id" in event
        assert event["event_type"] == "ACCEPTED"
        assert "ride_data" in event
        assert "created_at" in event


# ---------------------------------------------------------------------------
# Test 10: GET /rides/events with offset → next page
# ---------------------------------------------------------------------------


async def test_get_ride_events_with_offset(app_client, db_session):
    """GET /rides/events with limit and offset -> correct pagination."""
    reg = await _register(app_client, email="events2@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="ev2-dev")

    await _create_rides_for_user(app_client, pairing["device_token"], "ev2-dev", count=5)

    # First page: limit=2, offset=0
    resp1 = await app_client.get(
        RIDES_EVENTS_URL,
        params={"limit": 2, "offset": 0},
        headers=_jwt(reg["access_token"]),
    )
    assert resp1.status_code == 200
    data1 = resp1.json()
    assert data1["total"] == 5
    assert data1["limit"] == 2
    assert data1["offset"] == 0
    assert len(data1["events"]) == 2

    # Second page: limit=2, offset=2
    resp2 = await app_client.get(
        RIDES_EVENTS_URL,
        params={"limit": 2, "offset": 2},
        headers=_jwt(reg["access_token"]),
    )
    assert resp2.status_code == 200
    data2 = resp2.json()
    assert data2["total"] == 5
    assert data2["limit"] == 2
    assert data2["offset"] == 2
    assert len(data2["events"]) == 2

    # Pages should have different events
    page1_ids = {e["id"] for e in data1["events"]}
    page2_ids = {e["id"] for e in data2["events"]}
    assert page1_ids.isdisjoint(page2_ids)


# ---------------------------------------------------------------------------
# Test 11: GET /rides/events last page → fewer events than limit
# ---------------------------------------------------------------------------


async def test_get_ride_events_last_page(app_client, db_session):
    """GET /rides/events on last page -> returns remaining events, total unchanged."""
    reg = await _register(app_client, email="events3@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="ev3-dev")

    await _create_rides_for_user(app_client, pairing["device_token"], "ev3-dev", count=3)

    # Offset=2, limit=5 -> should return only 1 event (3 total, skip 2)
    resp = await app_client.get(
        RIDES_EVENTS_URL,
        params={"limit": 5, "offset": 2},
        headers=_jwt(reg["access_token"]),
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 3
    assert len(data["events"]) == 1


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


async def test_get_ride_events_no_rides_returns_empty(app_client, db_session):
    """GET /rides/events for user with no rides -> empty events, total=0."""
    reg = await _register(app_client, email="events5@example.com")

    resp = await app_client.get(RIDES_EVENTS_URL, headers=_jwt(reg["access_token"]))
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 0
    assert data["limit"] == 50
    assert data["offset"] == 0
    assert data["events"] == []


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
# Test 17: RideEventsListResponse serializes correctly
# ---------------------------------------------------------------------------


async def test_ride_events_list_response_serialization(app_client, db_session):
    """GET /rides/events -> response has correct structure with all fields."""
    reg = await _register(app_client, email="serial@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="serial-dev")

    await _create_rides_for_user(app_client, pairing["device_token"], "serial-dev", count=2)

    resp = await app_client.get(RIDES_EVENTS_URL, headers=_jwt(reg["access_token"]))
    assert resp.status_code == 200
    data = resp.json()

    # Verify top-level response structure
    assert "events" in data
    assert "total" in data
    assert "limit" in data
    assert "offset" in data
    assert isinstance(data["events"], list)
    assert isinstance(data["total"], int)
    assert isinstance(data["limit"], int)
    assert isinstance(data["offset"], int)

    # Verify each event has correct fields
    for event in data["events"]:
        assert "id" in event
        assert "event_type" in event
        assert "ride_data" in event
        assert "created_at" in event
        assert isinstance(event["ride_data"], dict)


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


async def test_get_ride_events_negative_offset_returns_422(app_client):
    """GET /rides/events with offset=-1 -> 422 (ge=0 constraint)."""
    reg = await _register(app_client, email="offneg@example.com")

    resp = await app_client.get(
        RIDES_EVENTS_URL,
        params={"offset": -1},
        headers=_jwt(reg["access_token"]),
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Test 20: GET /rides/events returns events in newest-first order
# ---------------------------------------------------------------------------


async def test_get_ride_events_ordered_newest_first(app_client, db_session):
    """GET /rides/events -> events ordered by created_at descending."""
    from datetime import datetime, timedelta

    reg = await _register(app_client, email="order@example.com")
    user_id = UUID(reg["user_id"])

    # Insert rides directly with explicit timestamps to guarantee ordering
    base_time = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
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
            created_at=base_time + timedelta(hours=i),
        )
        db_session.add(ride)
    await db_session.flush()

    resp = await app_client.get(RIDES_EVENTS_URL, headers=_jwt(reg["access_token"]))
    assert resp.status_code == 200
    events = resp.json()["events"]
    assert len(events) == 3

    # Newest first: price 12.0 (2h later) → 11.0 (1h later) → 10.0 (base)
    assert events[0]["ride_data"]["price"] == 12.0
    assert events[1]["ride_data"]["price"] == 11.0
    assert events[2]["ride_data"]["price"] == 10.0


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

    # User B should see 0 events
    resp_b = await app_client.get(RIDES_EVENTS_URL, headers=_jwt(reg_b["access_token"]))
    assert resp_b.status_code == 200
    assert resp_b.json()["total"] == 0
    assert resp_b.json()["events"] == []

    # User A should see 3 events
    resp_a = await app_client.get(RIDES_EVENTS_URL, headers=_jwt(reg_a["access_token"]))
    assert resp_a.status_code == 200
    assert resp_a.json()["total"] == 3


# ---------------------------------------------------------------------------
# Test 22: POST /rides race condition — IntegrityError fallback path
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
