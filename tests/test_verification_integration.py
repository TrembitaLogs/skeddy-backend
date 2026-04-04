"""Integration tests for ride verification flows (task 6.6).

Test strategy:
1. E2E flow: ride created via POST /rides -> ping with ride_statuses -> auto-confirm at deadline
2. E2E flow: ride created -> ping reports present=false -> auto-cancel with refund at deadline
3. Concurrent ping: two pings with expired deadline -> only one processes, no double refund
4. Only one CreditTransaction RIDE_REFUND per ride after concurrent processing
5. Only one FCM push per ride after concurrent processing
6. Throttle test: multiple pings in quick succession -> verify_rides throttle works via HTTP
7. Mixed scenario: multiple rides with different statuses, simultaneous processing
8. E2E full lifecycle: create ride -> multiple pings with reports -> deadline -> resolution
9. verify_rides sent regardless of search active/inactive state
"""

import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch
from uuid import UUID

from sqlalchemy import func, select

from app.models.app_config import AppConfig
from app.models.credit_balance import CreditBalance
from app.models.credit_transaction import CreditTransaction, TransactionType
from app.models.ride import Ride
from app.models.user import User

PING_URL = "/api/v1/ping"
RIDES_URL = "/api/v1/rides"
REGISTER_URL = "/api/v1/auth/register"
SEARCH_LOGIN_URL = "/api/v1/auth/search-login"
SEARCH_START_URL = "/api/v1/search/start"

_TEST_PASSWORD = "securePass1"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _register(app_client, email):
    resp = await app_client.post(REGISTER_URL, json={"email": email, "password": _TEST_PASSWORD})
    assert resp.status_code == 201
    return resp.json()


async def _pair_device(app_client, email, device_id):
    """Register a search device via search-login endpoint."""
    resp = await app_client.post(
        SEARCH_LOGIN_URL,
        json={
            "email": email,
            "password": _TEST_PASSWORD,
            "device_id": device_id,
            "timezone": "America/New_York",
        },
    )
    assert resp.status_code == 200
    return resp.json()


async def _verify_email_in_db(db_session, user_id: str):
    result = await db_session.execute(select(User).where(User.id == UUID(user_id)))
    user = result.scalar_one()
    user.email_verified = True
    await db_session.commit()


async def _start_search(app_client, access_token):
    resp = await app_client.post(
        SEARCH_START_URL, headers={"Authorization": f"Bearer {access_token}"}
    )
    assert resp.status_code == 200


def _device_headers(device_token: str, device_id: str) -> dict:
    return {"X-Device-Token": device_token, "X-Device-Id": device_id}


def _ping_body(**overrides) -> dict:
    body = {"timezone": "America/New_York", "app_version": "1.0.0"}
    body.update(overrides)
    return body


def _ride_body(ride_hash: str, **overrides) -> dict:
    body = {
        "idempotency_key": str(uuid.uuid4()),
        "event_type": "ACCEPTED",
        "ride_hash": ride_hash,
        "timezone": "America/New_York",
        "ride_data": {
            "price": 25.50,
            "pickup_time": "Tomorrow · 6:05AM",
            "pickup_location": "Maida Ter & Maida Way",
            "dropoff_location": "East Rd & Leonardville Rd",
        },
    }
    body.update(overrides)
    return body


async def _setup_user(app_client, db_session, email, device_id):
    """Register, pair device, verify email, start search. Returns (reg_data, pairing_data)."""
    reg = await _register(app_client, email)
    pairing = await _pair_device(app_client, email, device_id)
    await _verify_email_in_db(db_session, reg["user_id"])
    await _start_search(app_client, reg["access_token"])
    return reg, pairing


async def _create_ride_via_api(app_client, headers, ride_hash, price=25.50):
    """Create a ride via POST /rides and return response data."""
    body = _ride_body(
        ride_hash,
        ride_data={
            "price": price,
            "pickup_time": "Tomorrow · 6:05AM",
            "pickup_location": "Maida Ter & Maida Way",
            "dropoff_location": "East Rd & Leonardville Rd",
        },
    )
    with (
        patch("app.services.ride_service.send_push", new_callable=AsyncMock, return_value=True),
        patch("app.services.ride_service.send_credits_depleted", new_callable=AsyncMock),
    ):
        resp = await app_client.post(RIDES_URL, json=body, headers=headers)
    assert resp.status_code in (200, 201)
    return resp.json()


async def _set_ride_deadline(db_session, ride_id, deadline):
    """Set verification_deadline directly in DB for testing."""
    result = await db_session.execute(select(Ride).where(Ride.id == ride_id))
    ride = result.scalar_one()
    ride.verification_deadline = deadline
    await db_session.commit()


async def _get_ride(db_session, ride_id) -> Ride:
    result = await db_session.execute(
        select(Ride).where(Ride.id == ride_id).execution_options(populate_existing=True)
    )
    return result.scalar_one()


async def _get_balance(db_session, user_id) -> int:
    result = await db_session.execute(
        select(CreditBalance.balance).where(CreditBalance.user_id == user_id)
    )
    return result.scalar_one()


async def _count_refund_transactions(db_session, ride_id) -> int:
    result = await db_session.execute(
        select(func.count())
        .select_from(CreditTransaction)
        .where(
            CreditTransaction.type == TransactionType.RIDE_REFUND,
            CreditTransaction.reference_id == ride_id,
        )
    )
    return result.scalar()


# ---------------------------------------------------------------------------
# Test 1: E2E flow — ride created -> ping reports present=true -> CONFIRMED
# ---------------------------------------------------------------------------


async def test_e2e_ride_present_true_confirmed(app_client, db_session):
    """Full flow: create ride via API -> ping with present=true -> deadline expires -> CONFIRMED."""
    reg, pairing = await _setup_user(
        app_client, db_session, "e2e-confirm@test.com", "e2e-confirm-dev"
    )
    user_id = UUID(reg["user_id"])
    headers = _device_headers(pairing["device_token"], "e2e-confirm-dev")

    ride_hash = "a1" * 32
    ride_resp = await _create_ride_via_api(app_client, headers, ride_hash)
    ride_id = UUID(ride_resp["ride_id"])

    # Ping 1: report present=true (server asked for verification in verify_rides)
    with patch("app.routers.ping.send_ride_credit_refunded", new_callable=AsyncMock):
        resp1 = await app_client.post(
            PING_URL,
            json=_ping_body(ride_statuses=[{"ride_hash": ride_hash, "present": True}]),
            headers=headers,
        )
    assert resp1.status_code == 200

    # Verify last_reported_present=true
    ride = await _get_ride(db_session, ride_id)
    assert ride.last_reported_present is True
    assert ride.verification_status == "PENDING"

    # Expire the deadline
    await _set_ride_deadline(db_session, ride_id, datetime.now(UTC) - timedelta(hours=1))

    # Ping 2: triggers expired verification processing -> CONFIRMED
    balance_before = await _get_balance(db_session, user_id)
    with patch("app.routers.ping.send_ride_credit_refunded", new_callable=AsyncMock) as mock_fcm:
        resp2 = await app_client.post(PING_URL, json=_ping_body(), headers=headers)
    assert resp2.status_code == 200

    ride = await _get_ride(db_session, ride_id)
    assert ride.verification_status == "CONFIRMED"
    assert ride.verified_at is not None

    # No refund for CONFIRMED rides
    balance_after = await _get_balance(db_session, user_id)
    assert balance_after == balance_before
    mock_fcm.assert_not_called()


# ---------------------------------------------------------------------------
# Test 2: E2E flow — ride created -> ping reports present=false -> CANCELLED + refund
# ---------------------------------------------------------------------------


async def test_e2e_ride_present_false_cancelled_with_refund(app_client, db_session):
    """Full flow: create ride via API -> ping with present=false -> deadline -> CANCELLED + refund."""
    reg, pairing = await _setup_user(
        app_client, db_session, "e2e-cancel@test.com", "e2e-cancel-dev"
    )
    user_id = UUID(reg["user_id"])
    headers = _device_headers(pairing["device_token"], "e2e-cancel-dev")

    ride_hash = "b2" * 32
    ride_resp = await _create_ride_via_api(app_client, headers, ride_hash)
    ride_id = UUID(ride_resp["ride_id"])

    # Get balance after ride creation (credits already charged)
    balance_after_charge = await _get_balance(db_session, user_id)
    ride = await _get_ride(db_session, ride_id)
    credits_charged = ride.credits_charged
    assert credits_charged > 0

    # Ping with present=false
    with patch("app.routers.ping.send_ride_credit_refunded", new_callable=AsyncMock):
        await app_client.post(
            PING_URL,
            json=_ping_body(ride_statuses=[{"ride_hash": ride_hash, "present": False}]),
            headers=headers,
        )

    ride = await _get_ride(db_session, ride_id)
    assert ride.last_reported_present is False
    assert ride.disappeared_at is not None

    # Expire deadline
    await _set_ride_deadline(db_session, ride_id, datetime.now(UTC) - timedelta(hours=1))

    # Ping triggers CANCELLED + refund
    with patch("app.routers.ping.send_ride_credit_refunded", new_callable=AsyncMock) as mock_fcm:
        resp = await app_client.post(PING_URL, json=_ping_body(), headers=headers)
    assert resp.status_code == 200

    ride = await _get_ride(db_session, ride_id)
    assert ride.verification_status == "CANCELLED"
    assert ride.verified_at is not None
    assert ride.credits_refunded == credits_charged

    # Balance restored
    balance_after_refund = await _get_balance(db_session, user_id)
    assert balance_after_refund == balance_after_charge + credits_charged

    # CreditTransaction RIDE_REFUND created
    refund_count = await _count_refund_transactions(db_session, ride_id)
    assert refund_count == 1

    # FCM push sent
    mock_fcm.assert_called_once()
    call_args = mock_fcm.call_args
    assert call_args[0][2] == ride_id
    assert call_args[0][3] == credits_charged


# ---------------------------------------------------------------------------
# Test 3: Concurrent pings — only one processes the expired ride
# ---------------------------------------------------------------------------


async def test_concurrent_pings_no_double_refund(app_client, db_session):
    """Two sequential pings with expired deadline -> only one processes, no double refund."""
    reg, pairing = await _setup_user(
        app_client, db_session, "concurrent@test.com", "concurrent-dev"
    )
    user_id = UUID(reg["user_id"])
    headers = _device_headers(pairing["device_token"], "concurrent-dev")

    ride_hash = "c3" * 32
    ride_resp = await _create_ride_via_api(app_client, headers, ride_hash)
    ride_id = UUID(ride_resp["ride_id"])

    # Report present=false
    with patch("app.routers.ping.send_ride_credit_refunded", new_callable=AsyncMock):
        await app_client.post(
            PING_URL,
            json=_ping_body(ride_statuses=[{"ride_hash": ride_hash, "present": False}]),
            headers=headers,
        )

    # Expire deadline
    await _set_ride_deadline(db_session, ride_id, datetime.now(UTC) - timedelta(hours=1))
    balance_before = await _get_balance(db_session, user_id)
    ride = await _get_ride(db_session, ride_id)
    credits_charged = ride.credits_charged

    # First ping — processes the ride
    with patch("app.routers.ping.send_ride_credit_refunded", new_callable=AsyncMock):
        resp1 = await app_client.post(PING_URL, json=_ping_body(), headers=headers)
    assert resp1.status_code == 200

    ride = await _get_ride(db_session, ride_id)
    assert ride.verification_status == "CANCELLED"
    balance_after_first = await _get_balance(db_session, user_id)
    assert balance_after_first == balance_before + credits_charged

    # Second ping — ride already CANCELLED, nothing to process
    with patch("app.routers.ping.send_ride_credit_refunded", new_callable=AsyncMock) as mock_fcm2:
        resp2 = await app_client.post(PING_URL, json=_ping_body(), headers=headers)
    assert resp2.status_code == 200

    # Balance unchanged after second ping
    balance_after_second = await _get_balance(db_session, user_id)
    assert balance_after_second == balance_after_first

    # Only one RIDE_REFUND transaction
    refund_count = await _count_refund_transactions(db_session, ride_id)
    assert refund_count == 1

    # No FCM push on second ping (ride already processed)
    mock_fcm2.assert_not_called()


# ---------------------------------------------------------------------------
# Test 4: Only one CreditTransaction RIDE_REFUND per ride
# ---------------------------------------------------------------------------


async def test_single_refund_transaction_per_ride(app_client, db_session):
    """After processing, exactly one RIDE_REFUND CreditTransaction exists per ride."""
    reg, pairing = await _setup_user(
        app_client, db_session, "single-refund@test.com", "single-refund-dev"
    )
    user_id = UUID(reg["user_id"])
    headers = _device_headers(pairing["device_token"], "single-refund-dev")

    ride_hash = "d4" * 32
    ride_resp = await _create_ride_via_api(app_client, headers, ride_hash)
    ride_id = UUID(ride_resp["ride_id"])

    # Report present=false and expire deadline
    with patch("app.routers.ping.send_ride_credit_refunded", new_callable=AsyncMock):
        await app_client.post(
            PING_URL,
            json=_ping_body(ride_statuses=[{"ride_hash": ride_hash, "present": False}]),
            headers=headers,
        )
    await _set_ride_deadline(db_session, ride_id, datetime.now(UTC) - timedelta(hours=1))

    # Process through 3 pings to ensure no duplicates
    for _ in range(3):
        with patch("app.routers.ping.send_ride_credit_refunded", new_callable=AsyncMock):
            resp = await app_client.post(PING_URL, json=_ping_body(), headers=headers)
            assert resp.status_code == 200

    # Exactly 1 RIDE_REFUND
    refund_count = await _count_refund_transactions(db_session, ride_id)
    assert refund_count == 1

    # Verify the transaction data
    result = await db_session.execute(
        select(CreditTransaction).where(
            CreditTransaction.type == TransactionType.RIDE_REFUND,
            CreditTransaction.reference_id == ride_id,
        )
    )
    txn = result.scalar_one()
    assert txn.amount > 0
    assert txn.user_id == user_id
    assert txn.reference_id == ride_id


# ---------------------------------------------------------------------------
# Test 5: Only one FCM push per ride during concurrent processing
# ---------------------------------------------------------------------------


async def test_single_fcm_push_per_ride(app_client, db_session):
    """FCM RIDE_CREDIT_REFUNDED is sent exactly once even with multiple pings."""
    _reg, pairing = await _setup_user(
        app_client, db_session, "single-fcm@test.com", "single-fcm-dev"
    )
    headers = _device_headers(pairing["device_token"], "single-fcm-dev")

    ride_hash = "e5" * 32
    ride_resp = await _create_ride_via_api(app_client, headers, ride_hash)
    ride_id = UUID(ride_resp["ride_id"])

    # Report present=false and expire deadline
    with patch("app.routers.ping.send_ride_credit_refunded", new_callable=AsyncMock):
        await app_client.post(
            PING_URL,
            json=_ping_body(ride_statuses=[{"ride_hash": ride_hash, "present": False}]),
            headers=headers,
        )
    await _set_ride_deadline(db_session, ride_id, datetime.now(UTC) - timedelta(hours=1))

    # Track total FCM calls across all pings
    total_fcm_calls = 0
    for _ in range(3):
        with patch(
            "app.routers.ping.send_ride_credit_refunded", new_callable=AsyncMock
        ) as mock_fcm:
            await app_client.post(PING_URL, json=_ping_body(), headers=headers)
            total_fcm_calls += mock_fcm.call_count

    assert total_fcm_calls == 1


# ---------------------------------------------------------------------------
# Test 6: Throttle test — verify_rides throttle works via HTTP
# ---------------------------------------------------------------------------


async def test_verify_rides_throttle_via_http(app_client, db_session):
    """Multiple pings in quick succession -> verify_rides respects throttle interval."""
    reg, pairing = await _setup_user(app_client, db_session, "throttle@test.com", "throttle-dev")
    UUID(reg["user_id"])
    headers = _device_headers(pairing["device_token"], "throttle-dev")

    # Set verification_check_interval_minutes to 60
    db_session.add(AppConfig(key="verification_check_interval_minutes", value="60"))
    await db_session.commit()

    ride_hash = "f6" * 32
    ride_resp = await _create_ride_via_api(app_client, headers, ride_hash)
    UUID(ride_resp["ride_id"])

    # First ping — ride should appear in verify_rides (never checked before)
    with patch("app.routers.ping.send_ride_credit_refunded", new_callable=AsyncMock):
        resp1 = await app_client.post(PING_URL, json=_ping_body(), headers=headers)
    assert resp1.status_code == 200
    data1 = resp1.json()
    verify_hashes_1 = [vr["ride_hash"] for vr in data1.get("verify_rides", [])]
    assert ride_hash in verify_hashes_1

    # Second ping immediately — ride should NOT appear (throttle: < 60 min)
    with patch("app.routers.ping.send_ride_credit_refunded", new_callable=AsyncMock):
        resp2 = await app_client.post(PING_URL, json=_ping_body(), headers=headers)
    assert resp2.status_code == 200
    data2 = resp2.json()
    verify_hashes_2 = [vr["ride_hash"] for vr in data2.get("verify_rides", [])]
    assert ride_hash not in verify_hashes_2

    # Third ping immediately — still throttled
    with patch("app.routers.ping.send_ride_credit_refunded", new_callable=AsyncMock):
        resp3 = await app_client.post(PING_URL, json=_ping_body(), headers=headers)
    assert resp3.status_code == 200
    data3 = resp3.json()
    verify_hashes_3 = [vr["ride_hash"] for vr in data3.get("verify_rides", [])]
    assert ride_hash not in verify_hashes_3


# ---------------------------------------------------------------------------
# Test 7: Mixed scenario — multiple rides with different statuses
# ---------------------------------------------------------------------------


async def test_mixed_rides_different_outcomes(app_client, db_session):
    """Multiple rides: one CONFIRMED (present=true), one CANCELLED (present=false), one still PENDING."""
    reg, pairing = await _setup_user(app_client, db_session, "mixed@test.com", "mixed-dev")
    user_id = UUID(reg["user_id"])
    headers = _device_headers(pairing["device_token"], "mixed-dev")

    # Create 3 rides with different hashes
    hash_confirm = "a1" * 32
    hash_cancel = "b2" * 32
    hash_pending = "c3" * 32

    ride_confirm_resp = await _create_ride_via_api(app_client, headers, hash_confirm)
    ride_cancel_resp = await _create_ride_via_api(app_client, headers, hash_cancel)
    ride_pending_resp = await _create_ride_via_api(app_client, headers, hash_pending)

    ride_confirm_id = UUID(ride_confirm_resp["ride_id"])
    ride_cancel_id = UUID(ride_cancel_resp["ride_id"])
    ride_pending_id = UUID(ride_pending_resp["ride_id"])

    # Report: ride_confirm=present, ride_cancel=absent
    with patch("app.routers.ping.send_ride_credit_refunded", new_callable=AsyncMock):
        await app_client.post(
            PING_URL,
            json=_ping_body(
                ride_statuses=[
                    {"ride_hash": hash_confirm, "present": True},
                    {"ride_hash": hash_cancel, "present": False},
                    # hash_pending intentionally not reported
                ]
            ),
            headers=headers,
        )

    # Expire deadlines for ride_confirm and ride_cancel only
    await _set_ride_deadline(db_session, ride_confirm_id, datetime.now(UTC) - timedelta(hours=1))
    await _set_ride_deadline(db_session, ride_cancel_id, datetime.now(UTC) - timedelta(hours=1))
    # ride_pending keeps its future deadline

    balance_before = await _get_balance(db_session, user_id)
    ride_cancel = await _get_ride(db_session, ride_cancel_id)
    cancel_credits = ride_cancel.credits_charged

    # Ping to trigger processing
    with patch("app.routers.ping.send_ride_credit_refunded", new_callable=AsyncMock) as mock_fcm:
        resp = await app_client.post(PING_URL, json=_ping_body(), headers=headers)
    assert resp.status_code == 200

    # ride_confirm -> CONFIRMED
    ride_confirmed = await _get_ride(db_session, ride_confirm_id)
    assert ride_confirmed.verification_status == "CONFIRMED"
    assert ride_confirmed.credits_refunded == 0

    # ride_cancel -> CANCELLED with refund
    ride_cancelled = await _get_ride(db_session, ride_cancel_id)
    assert ride_cancelled.verification_status == "CANCELLED"
    assert ride_cancelled.credits_refunded == cancel_credits

    # ride_pending -> still PENDING
    ride_still_pending = await _get_ride(db_session, ride_pending_id)
    assert ride_still_pending.verification_status == "PENDING"

    # Balance: only cancel_credits refunded
    balance_after = await _get_balance(db_session, user_id)
    assert balance_after == balance_before + cancel_credits

    # FCM: only one push for the cancelled ride
    mock_fcm.assert_called_once()


# ---------------------------------------------------------------------------
# Test 8: E2E full lifecycle with multiple status reports
# ---------------------------------------------------------------------------


async def test_e2e_full_lifecycle_multiple_reports(app_client, db_session):
    """Ride temporarily disappears (present=false) then reappears (present=true) -> CONFIRMED."""
    _reg, pairing = await _setup_user(
        app_client, db_session, "lifecycle@test.com", "lifecycle-dev"
    )
    headers = _device_headers(pairing["device_token"], "lifecycle-dev")

    ride_hash = "aa" * 32
    ride_resp = await _create_ride_via_api(app_client, headers, ride_hash)
    ride_id = UUID(ride_resp["ride_id"])

    # Report 1: present=true
    with patch("app.routers.ping.send_ride_credit_refunded", new_callable=AsyncMock):
        await app_client.post(
            PING_URL,
            json=_ping_body(ride_statuses=[{"ride_hash": ride_hash, "present": True}]),
            headers=headers,
        )
    ride = await _get_ride(db_session, ride_id)
    assert ride.last_reported_present is True
    assert ride.disappeared_at is None

    # Report 2: present=false (temporarily disappeared)
    with patch("app.routers.ping.send_ride_credit_refunded", new_callable=AsyncMock):
        await app_client.post(
            PING_URL,
            json=_ping_body(ride_statuses=[{"ride_hash": ride_hash, "present": False}]),
            headers=headers,
        )
    ride = await _get_ride(db_session, ride_id)
    assert ride.last_reported_present is False
    assert ride.disappeared_at is not None
    disappeared_at = ride.disappeared_at

    # Report 3: present=true again (reappeared)
    with patch("app.routers.ping.send_ride_credit_refunded", new_callable=AsyncMock):
        await app_client.post(
            PING_URL,
            json=_ping_body(ride_statuses=[{"ride_hash": ride_hash, "present": True}]),
            headers=headers,
        )
    ride = await _get_ride(db_session, ride_id)
    assert ride.last_reported_present is True
    # disappeared_at should NOT be overwritten (audit data)
    assert ride.disappeared_at == disappeared_at

    # Expire deadline -> decision based on last report (true) -> CONFIRMED
    await _set_ride_deadline(db_session, ride_id, datetime.now(UTC) - timedelta(hours=1))

    with patch("app.routers.ping.send_ride_credit_refunded", new_callable=AsyncMock) as mock_fcm:
        resp = await app_client.post(PING_URL, json=_ping_body(), headers=headers)
    assert resp.status_code == 200

    ride = await _get_ride(db_session, ride_id)
    assert ride.verification_status == "CONFIRMED"
    mock_fcm.assert_not_called()


# ---------------------------------------------------------------------------
# Test 9: verify_rides sent regardless of search active/inactive state
# ---------------------------------------------------------------------------


async def test_verify_rides_sent_when_search_inactive(app_client, db_session):
    """verify_rides is returned even when search is inactive (PRD: sent regardless of search state)."""
    reg, pairing = await _setup_user(
        app_client, db_session, "inactive-verify@test.com", "inactive-verify-dev"
    )
    UUID(reg["user_id"])
    headers = _device_headers(pairing["device_token"], "inactive-verify-dev")

    ride_hash = "bb" * 32
    await _create_ride_via_api(app_client, headers, ride_hash)

    # Stop search
    resp_stop = await app_client.post(
        "/api/v1/search/stop",
        headers={"Authorization": f"Bearer {reg['access_token']}"},
    )
    assert resp_stop.status_code == 200

    # Ping — search is inactive, but verify_rides should still contain the ride
    with patch("app.routers.ping.send_ride_credit_refunded", new_callable=AsyncMock):
        resp = await app_client.post(PING_URL, json=_ping_body(), headers=headers)
    assert resp.status_code == 200
    data = resp.json()
    assert data["search"] is False
    verify_hashes = [vr["ride_hash"] for vr in data.get("verify_rides", [])]
    assert ride_hash in verify_hashes


# ---------------------------------------------------------------------------
# Test 10: Atomicity — refund and status change in one transaction
# ---------------------------------------------------------------------------


async def test_refund_atomicity_with_status_change(app_client, db_session):
    """CANCELLED status change and credit refund happen atomically."""
    reg, pairing = await _setup_user(app_client, db_session, "atomic@test.com", "atomic-dev")
    user_id = UUID(reg["user_id"])
    headers = _device_headers(pairing["device_token"], "atomic-dev")

    ride_hash = "cc" * 32
    ride_resp = await _create_ride_via_api(app_client, headers, ride_hash)
    ride_id = UUID(ride_resp["ride_id"])

    ride = await _get_ride(db_session, ride_id)
    credits_charged = ride.credits_charged

    # Report present=false and expire
    with patch("app.routers.ping.send_ride_credit_refunded", new_callable=AsyncMock):
        await app_client.post(
            PING_URL,
            json=_ping_body(ride_statuses=[{"ride_hash": ride_hash, "present": False}]),
            headers=headers,
        )
    await _set_ride_deadline(db_session, ride_id, datetime.now(UTC) - timedelta(hours=1))

    balance_before = await _get_balance(db_session, user_id)

    # Process
    with patch("app.routers.ping.send_ride_credit_refunded", new_callable=AsyncMock):
        resp = await app_client.post(PING_URL, json=_ping_body(), headers=headers)
    assert resp.status_code == 200

    # Both status and balance changed atomically
    ride = await _get_ride(db_session, ride_id)
    assert ride.verification_status == "CANCELLED"
    assert ride.credits_refunded == credits_charged

    balance_after = await _get_balance(db_session, user_id)
    assert balance_after == balance_before + credits_charged

    # RIDE_REFUND transaction references the ride
    result = await db_session.execute(
        select(CreditTransaction).where(
            CreditTransaction.type == TransactionType.RIDE_REFUND,
            CreditTransaction.reference_id == ride_id,
        )
    )
    txn = result.scalar_one()
    assert txn.amount == credits_charged
    assert txn.balance_after == balance_after


# ---------------------------------------------------------------------------
# Test 11: NULL reports (offline Search App) -> CONFIRMED (benefit of doubt)
# ---------------------------------------------------------------------------


async def test_e2e_no_reports_offline_confirmed(app_client, db_session):
    """Ride with no status reports (Search App offline) -> CONFIRMED (benefit of doubt)."""
    reg, pairing = await _setup_user(app_client, db_session, "offline@test.com", "offline-dev")
    user_id = UUID(reg["user_id"])
    headers = _device_headers(pairing["device_token"], "offline-dev")

    ride_hash = "dd" * 32
    ride_resp = await _create_ride_via_api(app_client, headers, ride_hash)
    ride_id = UUID(ride_resp["ride_id"])

    # No ride_statuses reports at all — directly expire the deadline
    await _set_ride_deadline(db_session, ride_id, datetime.now(UTC) - timedelta(hours=1))
    balance_before = await _get_balance(db_session, user_id)

    # Ping triggers processing — NULL reports -> CONFIRMED
    with patch("app.routers.ping.send_ride_credit_refunded", new_callable=AsyncMock) as mock_fcm:
        resp = await app_client.post(PING_URL, json=_ping_body(), headers=headers)
    assert resp.status_code == 200

    ride = await _get_ride(db_session, ride_id)
    assert ride.verification_status == "CONFIRMED"
    assert ride.last_reported_present is None

    # No refund
    balance_after = await _get_balance(db_session, user_id)
    assert balance_after == balance_before
    mock_fcm.assert_not_called()

    # No RIDE_REFUND transaction
    refund_count = await _count_refund_transactions(db_session, ride_id)
    assert refund_count == 0


# ---------------------------------------------------------------------------
# Test 12: Expired verifications processed even during search=false ping
# ---------------------------------------------------------------------------


async def test_expired_verifications_processed_when_search_inactive(app_client, db_session):
    """Expired verification is resolved even when search is inactive."""
    reg, pairing = await _setup_user(
        app_client, db_session, "inactive-exp@test.com", "inactive-exp-dev"
    )
    UUID(reg["user_id"])
    headers = _device_headers(pairing["device_token"], "inactive-exp-dev")

    ride_hash = "ee" * 32
    ride_resp = await _create_ride_via_api(app_client, headers, ride_hash)
    ride_id = UUID(ride_resp["ride_id"])

    # Report present=false
    with patch("app.routers.ping.send_ride_credit_refunded", new_callable=AsyncMock):
        await app_client.post(
            PING_URL,
            json=_ping_body(ride_statuses=[{"ride_hash": ride_hash, "present": False}]),
            headers=headers,
        )

    # Stop search
    await app_client.post(
        "/api/v1/search/stop",
        headers={"Authorization": f"Bearer {reg['access_token']}"},
    )

    # Expire deadline
    await _set_ride_deadline(db_session, ride_id, datetime.now(UTC) - timedelta(hours=1))

    # Ping with search inactive — verification still processed
    with patch("app.routers.ping.send_ride_credit_refunded", new_callable=AsyncMock) as mock_fcm:
        resp = await app_client.post(PING_URL, json=_ping_body(), headers=headers)
    assert resp.status_code == 200
    assert resp.json()["search"] is False

    ride = await _get_ride(db_session, ride_id)
    assert ride.verification_status == "CANCELLED"
    mock_fcm.assert_called_once()
