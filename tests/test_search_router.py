from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import select

from app.models.credit_balance import CreditBalance
from app.models.paired_device import PairedDevice
from app.models.user import User

SEARCH_START_URL = "/api/v1/search/start"
SEARCH_STOP_URL = "/api/v1/search/stop"
SEARCH_STATUS_URL = "/api/v1/search/status"
DEVICE_OVERRIDE_URL = "/api/v1/search/device-override"
REGISTER_URL = "/api/v1/auth/register"
PAIRING_GENERATE_URL = "/api/v1/pairing/generate"
PAIRING_CONFIRM_URL = "/api/v1/pairing/confirm"

_TEST_PASSWORD = "securePass1"


async def _register_and_get_tokens(app_client, email="search@example.com"):
    """Helper: register a user via API and return response data with tokens."""
    response = await app_client.post(
        REGISTER_URL,
        json={"email": email, "password": _TEST_PASSWORD},
    )
    assert response.status_code == 201
    return response.json()


async def _pair_device(app_client, access_token, device_id="test-device-001"):
    """Helper: pair a device via generate + confirm flow."""
    gen_resp = await app_client.post(
        PAIRING_GENERATE_URL,
        headers=_auth_header(access_token),
    )
    assert gen_resp.status_code == 201
    code = gen_resp.json()["code"]

    confirm_resp = await app_client.post(
        PAIRING_CONFIRM_URL,
        json={"code": code, "device_id": device_id, "timezone": "America/New_York"},
    )
    assert confirm_resp.status_code == 200
    return confirm_resp.json()


def _auth_header(access_token: str) -> dict:
    return {"Authorization": f"Bearer {access_token}"}


def _device_headers(device_token: str, device_id: str) -> dict:
    return {"X-Device-Token": device_token, "X-Device-Id": device_id}


async def _verify_email_in_db(db_session, user_id: str):
    """Set email_verified=True for the given user directly in DB."""
    result = await db_session.execute(select(User).where(User.id == UUID(user_id)))
    user = result.scalar_one()
    user.email_verified = True
    await db_session.commit()


async def _set_balance_in_db(db_session, user_id: str, balance: int, fake_redis=None):
    """Set credit balance for the given user directly in DB and Redis cache."""
    result = await db_session.execute(
        select(CreditBalance).where(CreditBalance.user_id == UUID(user_id))
    )
    credit_balance = result.scalar_one()
    credit_balance.balance = balance
    await db_session.commit()
    if fake_redis is not None:
        cache_key = f"user_balance:{user_id}"
        await fake_redis.setex(cache_key, 300, str(balance))


# --- Test Strategy 1: POST /search/start without JWT → 401 ---


async def test_start_search_without_jwt_returns_401(app_client):
    """POST /search/start without Authorization header -> 401."""
    response = await app_client.post(SEARCH_START_URL)
    assert response.status_code == 401


# --- POST /search/start without paired device → 400 ---


async def test_start_search_without_paired_device_returns_400(app_client, db_session):
    """POST /search/start with no paired device -> 400 NO_PAIRED_DEVICE."""
    reg = await _register_and_get_tokens(app_client, email="nopair@example.com")
    await _verify_email_in_db(db_session, reg["user_id"])

    response = await app_client.post(
        SEARCH_START_URL,
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "NO_PAIRED_DEVICE"


# --- Test Strategy 2: POST /search/start → 200, {"ok": true} ---


async def test_start_search_returns_ok(app_client, db_session):
    """POST /search/start with paired device -> 200 {"ok": true}."""
    reg = await _register_and_get_tokens(app_client, email="start@example.com")
    await _verify_email_in_db(db_session, reg["user_id"])
    await _pair_device(app_client, reg["access_token"])

    response = await app_client.post(
        SEARCH_START_URL,
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}


# --- Test Strategy 3: POST /search/start twice → 200 (idempotent) ---


async def test_start_search_twice_is_idempotent(app_client, db_session):
    """POST /search/start called twice -> both return 200."""
    reg = await _register_and_get_tokens(app_client, email="twice@example.com")
    await _verify_email_in_db(db_session, reg["user_id"])
    await _pair_device(app_client, reg["access_token"])
    headers = _auth_header(reg["access_token"])

    resp1 = await app_client.post(SEARCH_START_URL, headers=headers)
    resp2 = await app_client.post(SEARCH_START_URL, headers=headers)

    assert resp1.status_code == 200
    assert resp2.status_code == 200


# --- Test Strategy 4: POST /search/stop → 200, {"ok": true} ---


async def test_stop_search_returns_ok(app_client):
    """POST /search/stop -> 200 {"ok": true}."""
    reg = await _register_and_get_tokens(app_client, email="stop@example.com")

    response = await app_client.post(
        SEARCH_STOP_URL,
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}


# --- POST /search/stop without JWT → 401 ---


async def test_stop_search_without_jwt_returns_401(app_client):
    """POST /search/stop without Authorization header -> 401."""
    response = await app_client.post(SEARCH_STOP_URL)
    assert response.status_code == 401


# --- Test Strategy 5: GET /search/status without paired device ---


async def test_status_without_device_returns_offline(app_client):
    """GET /search/status with no paired device -> is_online=False, last_ping_at=None."""
    reg = await _register_and_get_tokens(app_client, email="nodev@example.com")

    response = await app_client.get(
        SEARCH_STATUS_URL,
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 200
    data = response.json()
    assert data["is_active"] is False
    assert data["is_online"] is False
    assert data["last_ping_at"] is None


# --- Test Strategy 6: GET /search/status with device (last_ping 10s ago) → is_online=True ---


async def test_status_with_recent_ping_is_online(app_client, db_session):
    """GET /search/status with device pinged 10s ago -> is_online=True."""
    reg = await _register_and_get_tokens(app_client, email="online@example.com")
    await _pair_device(app_client, reg["access_token"])

    # Set last_ping_at to 10 seconds ago directly in DB
    user_id = UUID(reg["user_id"])
    result = await db_session.execute(select(PairedDevice).where(PairedDevice.user_id == user_id))
    device = result.scalar_one()
    device.last_ping_at = datetime.now(UTC) - timedelta(seconds=10)
    await db_session.commit()

    response = await app_client.get(
        SEARCH_STATUS_URL,
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 200
    data = response.json()
    assert data["is_online"] is True
    assert data["last_ping_at"] is not None


# --- Test Strategy 7: GET /search/status with device (last_ping 2 min ago) → is_online=False ---


async def test_status_with_stale_ping_is_offline(app_client, db_session):
    """GET /search/status with device pinged 2 min ago -> is_online=False."""
    reg = await _register_and_get_tokens(app_client, email="stale@example.com")
    await _pair_device(app_client, reg["access_token"])

    # Set last_ping_at to 2 minutes ago
    user_id = UUID(reg["user_id"])
    result = await db_session.execute(select(PairedDevice).where(PairedDevice.user_id == user_id))
    device = result.scalar_one()
    device.last_ping_at = datetime.now(UTC) - timedelta(seconds=120)
    await db_session.commit()

    response = await app_client.get(
        SEARCH_STATUS_URL,
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 200
    data = response.json()
    assert data["is_online"] is False
    assert data["last_ping_at"] is not None


# --- Verify start actually sets is_active to true ---


async def test_start_then_status_shows_active(app_client, db_session):
    """POST /search/start then GET /search/status -> is_active=True."""
    reg = await _register_and_get_tokens(app_client, email="active@example.com")
    await _verify_email_in_db(db_session, reg["user_id"])
    await _pair_device(app_client, reg["access_token"])
    headers = _auth_header(reg["access_token"])

    await app_client.post(SEARCH_START_URL, headers=headers)

    response = await app_client.get(SEARCH_STATUS_URL, headers=headers)

    assert response.status_code == 200
    assert response.json()["is_active"] is True


# --- Verify stop after start sets is_active to false ---


async def test_start_then_stop_then_status_shows_inactive(app_client, db_session):
    """POST /search/start then /stop then GET /search/status -> is_active=False."""
    reg = await _register_and_get_tokens(app_client, email="inactive@example.com")
    await _verify_email_in_db(db_session, reg["user_id"])
    await _pair_device(app_client, reg["access_token"])
    headers = _auth_header(reg["access_token"])

    await app_client.post(SEARCH_START_URL, headers=headers)
    await app_client.post(SEARCH_STOP_URL, headers=headers)

    response = await app_client.get(SEARCH_STATUS_URL, headers=headers)

    assert response.status_code == 200
    assert response.json()["is_active"] is False


# ===== POST /search/device-override (Task 7.5) =====


# --- 7.5 Test 1: without device headers → 422 (missing required headers) ---


async def test_device_override_without_headers_returns_422(app_client):
    """POST /search/device-override without X-Device-* headers -> 422."""
    response = await app_client.post(
        DEVICE_OVERRIDE_URL,
        json={"active": True},
    )
    assert response.status_code == 422


# --- 7.5 Test 2: invalid device token → 401 ---


async def test_device_override_with_invalid_token_returns_401(app_client):
    """POST /search/device-override with invalid device credentials -> 401."""
    response = await app_client.post(
        DEVICE_OVERRIDE_URL,
        json={"active": True},
        headers=_device_headers("bad-token", "bad-device-id"),
    )
    assert response.status_code == 401


# --- 7.5 Test 3: valid device, active=True → 200, {"ok": true} ---


async def test_device_override_active_true_returns_ok(app_client):
    """POST /search/device-override with valid device, active=True -> 200."""
    reg = await _register_and_get_tokens(app_client, email="devoverride1@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="override-dev-001")

    response = await app_client.post(
        DEVICE_OVERRIDE_URL,
        json={"active": True},
        headers=_device_headers(pairing["device_token"], "override-dev-001"),
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}


# --- 7.5 Test 4: valid device, active=False → 200, {"ok": true} ---


async def test_device_override_active_false_returns_ok(app_client):
    """POST /search/device-override with valid device, active=False -> 200."""
    reg = await _register_and_get_tokens(app_client, email="devoverride2@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="override-dev-002")

    response = await app_client.post(
        DEVICE_OVERRIDE_URL,
        json={"active": False},
        headers=_device_headers(pairing["device_token"], "override-dev-002"),
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}


# --- 7.5 Test 5: verify is_active actually updates in DB for device's user ---


async def test_device_override_updates_is_active_in_db(app_client):
    """POST /search/device-override changes is_active, verified via GET /search/status."""
    reg = await _register_and_get_tokens(app_client, email="devoverride3@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="override-dev-003")
    dev_hdrs = _device_headers(pairing["device_token"], "override-dev-003")
    jwt_hdrs = _auth_header(reg["access_token"])

    # Set active=True via device-override
    await app_client.post(DEVICE_OVERRIDE_URL, json={"active": True}, headers=dev_hdrs)

    # Verify via GET /search/status (JWT auth, Main App perspective)
    status_resp = await app_client.get(SEARCH_STATUS_URL, headers=jwt_hdrs)
    assert status_resp.json()["is_active"] is True

    # Set active=False via device-override
    await app_client.post(DEVICE_OVERRIDE_URL, json={"active": False}, headers=dev_hdrs)

    # Verify again
    status_resp = await app_client.get(SEARCH_STATUS_URL, headers=jwt_hdrs)
    assert status_resp.json()["is_active"] is False


# --- 7.5 Test 6: idempotent behavior ---


async def test_device_override_idempotent(app_client):
    """POST /search/device-override called twice with same value -> both 200."""
    reg = await _register_and_get_tokens(app_client, email="devoverride4@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="override-dev-004")
    dev_hdrs = _device_headers(pairing["device_token"], "override-dev-004")

    resp1 = await app_client.post(DEVICE_OVERRIDE_URL, json={"active": True}, headers=dev_hdrs)
    resp2 = await app_client.post(DEVICE_OVERRIDE_URL, json={"active": True}, headers=dev_hdrs)

    assert resp1.status_code == 200
    assert resp2.status_code == 200


# ===== GET /search/status force_update (Task 9.1) =====


async def test_status_force_update_false_without_device(app_client):
    """GET /search/status with no paired device -> force_update=False."""
    reg = await _register_and_get_tokens(app_client, email="fu_nodev@example.com")

    response = await app_client.get(
        SEARCH_STATUS_URL,
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 200
    assert response.json()["force_update"] is False


async def test_status_force_update_false_when_version_ok(app_client, db_session):
    """GET /search/status with current app_version -> force_update=False."""
    reg = await _register_and_get_tokens(app_client, email="fu_ok@example.com")
    await _pair_device(app_client, reg["access_token"], device_id="fu-ok-dev")

    user_id = UUID(reg["user_id"])
    result = await db_session.execute(select(PairedDevice).where(PairedDevice.user_id == user_id))
    device = result.scalar_one()
    device.app_version = "1.0.0"
    await db_session.commit()

    response = await app_client.get(
        SEARCH_STATUS_URL,
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 200
    assert response.json()["force_update"] is False


async def test_status_force_update_true_when_outdated(app_client, db_session):
    """GET /search/status with outdated app_version -> force_update=True."""
    reg = await _register_and_get_tokens(app_client, email="fu_old@example.com")
    await _pair_device(app_client, reg["access_token"], device_id="fu-old-dev")

    user_id = UUID(reg["user_id"])
    result = await db_session.execute(select(PairedDevice).where(PairedDevice.user_id == user_id))
    device = result.scalar_one()
    device.app_version = "0.9.0"
    await db_session.commit()

    response = await app_client.get(
        SEARCH_STATUS_URL,
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 200
    assert response.json()["force_update"] is True


async def test_status_force_update_false_when_no_version(app_client, db_session):
    """GET /search/status with app_version=None (never pinged) -> force_update=False."""
    reg = await _register_and_get_tokens(app_client, email="fu_none@example.com")
    await _pair_device(app_client, reg["access_token"], device_id="fu-none-dev")

    response = await app_client.get(
        SEARCH_STATUS_URL,
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 200
    assert response.json()["force_update"] is False


# ===== EMAIL_NOT_VERIFIED guard on POST /search/start (Task 22) =====


# --- Test Strategy 1: Unverified user → POST /search/start → 403 EMAIL_NOT_VERIFIED ---


async def test_start_search_unverified_email_returns_403(app_client):
    """POST /search/start with email_verified=false -> 403 EMAIL_NOT_VERIFIED."""
    reg = await _register_and_get_tokens(app_client, email="unverified@example.com")

    response = await app_client.post(
        SEARCH_START_URL,
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 403
    assert response.json()["error"]["code"] == "EMAIL_NOT_VERIFIED"


# --- Test Strategy 2-3: Verify email → POST /search/start → 200 OK (or 400 no device) ---


async def test_start_search_after_verify_email_returns_400_no_device(app_client, db_session):
    """After email verification, POST /search/start without device -> 400 NO_PAIRED_DEVICE."""
    reg = await _register_and_get_tokens(app_client, email="verify_then_start@example.com")
    headers = _auth_header(reg["access_token"])

    # Unverified → 403
    resp_before = await app_client.post(SEARCH_START_URL, headers=headers)
    assert resp_before.status_code == 403
    assert resp_before.json()["error"]["code"] == "EMAIL_NOT_VERIFIED"

    # Verify email
    await _verify_email_in_db(db_session, reg["user_id"])

    # Now passes email check but fails on no device
    resp_after = await app_client.post(SEARCH_START_URL, headers=headers)
    assert resp_after.status_code == 400
    assert resp_after.json()["error"]["code"] == "NO_PAIRED_DEVICE"


# --- Test Strategy 4: Verified user (old user) → POST /search/start → 200 OK ---


async def test_start_search_verified_user_with_device_returns_ok(app_client, db_session):
    """Verified user with paired device -> POST /search/start -> 200 {"ok": true}."""
    reg = await _register_and_get_tokens(app_client, email="verified_ok@example.com")
    await _verify_email_in_db(db_session, reg["user_id"])
    await _pair_device(app_client, reg["access_token"])

    response = await app_client.post(
        SEARCH_START_URL,
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}


# --- Test Strategy 5: Other endpoints work without email verification ---


async def test_stop_search_works_without_email_verification(app_client):
    """POST /search/stop works for unverified users (no email check)."""
    reg = await _register_and_get_tokens(app_client, email="unverified_stop@example.com")

    response = await app_client.post(
        SEARCH_STOP_URL,
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}


async def test_status_works_without_email_verification(app_client):
    """GET /search/status works for unverified users (no email check)."""
    reg = await _register_and_get_tokens(app_client, email="unverified_status@example.com")

    response = await app_client.get(
        SEARCH_STATUS_URL,
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 200


# ===== INSUFFICIENT_CREDITS guard on POST /search/start (Task 7.1) =====


async def test_start_search_with_zero_balance_returns_403(app_client, db_session, fake_redis):
    """POST /search/start with balance=0 -> 403 INSUFFICIENT_CREDITS."""
    reg = await _register_and_get_tokens(app_client, email="zerocredits@example.com")
    await _verify_email_in_db(db_session, reg["user_id"])
    await _pair_device(app_client, reg["access_token"])
    await _set_balance_in_db(db_session, reg["user_id"], 0, fake_redis)

    response = await app_client.post(
        SEARCH_START_URL,
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 403
    assert response.json()["error"]["code"] == "INSUFFICIENT_CREDITS"


async def test_start_search_with_positive_balance_passes(app_client, db_session):
    """POST /search/start with balance>0 -> passes balance check (200 OK)."""
    reg = await _register_and_get_tokens(app_client, email="hascredits@example.com")
    await _verify_email_in_db(db_session, reg["user_id"])
    await _pair_device(app_client, reg["access_token"])
    # Registration bonus gives 10 credits — no need to modify balance

    response = await app_client.post(
        SEARCH_START_URL,
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}


async def test_start_search_balance_check_precedes_email_check(app_client, db_session, fake_redis):
    """POST /search/start with balance=0 and unverified email -> INSUFFICIENT_CREDITS, not EMAIL_NOT_VERIFIED."""
    reg = await _register_and_get_tokens(app_client, email="precedence@example.com")
    # Don't verify email; set balance to 0
    await _set_balance_in_db(db_session, reg["user_id"], 0, fake_redis)

    response = await app_client.post(
        SEARCH_START_URL,
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 403
    assert response.json()["error"]["code"] == "INSUFFICIENT_CREDITS"


async def test_start_search_redis_cache_miss_falls_back_to_db(app_client, db_session, fake_redis):
    """POST /search/start reads balance from DB when Redis cache is empty."""
    reg = await _register_and_get_tokens(app_client, email="redisfallback@example.com")
    await _verify_email_in_db(db_session, reg["user_id"])
    await _pair_device(app_client, reg["access_token"])

    # Clear any cached balance from Redis to force DB fallback
    cache_key = f"user_balance:{reg['user_id']}"
    await fake_redis.delete(cache_key)

    response = await app_client.post(
        SEARCH_START_URL,
        headers=_auth_header(reg["access_token"]),
    )

    # Balance from DB is 10 (registration bonus) -> passes
    assert response.status_code == 200
    assert response.json() == {"ok": True}


# ===== credits_balance in GET /search/status (Task 7.2) =====


async def test_status_includes_credits_balance(app_client):
    """GET /search/status response includes credits_balance field (registration bonus)."""
    reg = await _register_and_get_tokens(app_client, email="cb_field@example.com")

    response = await app_client.get(
        SEARCH_STATUS_URL,
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 200
    data = response.json()
    assert "credits_balance" in data
    assert data["credits_balance"] == 10  # registration bonus


async def test_status_credits_balance_matches_actual_balance(app_client, db_session, fake_redis):
    """GET /search/status credits_balance reflects modified balance."""
    reg = await _register_and_get_tokens(app_client, email="cb_actual@example.com")
    await _set_balance_in_db(db_session, reg["user_id"], 42, fake_redis)

    response = await app_client.get(
        SEARCH_STATUS_URL,
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 200
    assert response.json()["credits_balance"] == 42


async def test_status_credits_balance_zero(app_client, db_session, fake_redis):
    """GET /search/status returns credits_balance=0 (not null) when balance is zero."""
    reg = await _register_and_get_tokens(app_client, email="cb_zero@example.com")
    await _set_balance_in_db(db_session, reg["user_id"], 0, fake_redis)

    response = await app_client.get(
        SEARCH_STATUS_URL,
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 200
    data = response.json()
    assert data["credits_balance"] == 0
    assert data["credits_balance"] is not None


async def test_status_credits_balance_redis_cache_hit(app_client, fake_redis):
    """GET /search/status reads credits_balance from Redis cache."""
    reg = await _register_and_get_tokens(app_client, email="cb_redis@example.com")

    # Set a different value in Redis cache to prove cache is used
    cache_key = f"user_balance:{reg['user_id']}"
    await fake_redis.setex(cache_key, 300, "77")

    response = await app_client.get(
        SEARCH_STATUS_URL,
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 200
    assert response.json()["credits_balance"] == 77


async def test_status_credits_balance_redis_miss_falls_back_to_db(
    app_client, db_session, fake_redis
):
    """GET /search/status reads credits_balance from DB when Redis cache is empty."""
    reg = await _register_and_get_tokens(app_client, email="cb_dbfallback@example.com")

    # Clear Redis cache to force DB fallback
    cache_key = f"user_balance:{reg['user_id']}"
    await fake_redis.delete(cache_key)

    response = await app_client.get(
        SEARCH_STATUS_URL,
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 200
    # DB has registration bonus = 10
    assert response.json()["credits_balance"] == 10
