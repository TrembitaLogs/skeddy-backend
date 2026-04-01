"""Tests for email change flow: POST /auth/change-email + POST /auth/verify-email."""

import hashlib
import json

CHANGE_EMAIL_URL = "/api/v1/auth/change-email"
VERIFY_EMAIL_URL = "/api/v1/auth/verify-email"
ME_URL = "/api/v1/auth/me"
REGISTER_URL = "/api/v1/auth/register"

_TEST_PASSWORD = "securePass1"
_VERIFY_CODE = "593817"


async def _register(app_client, email):
    resp = await app_client.post(REGISTER_URL, json={"email": email, "password": _TEST_PASSWORD})
    assert resp.status_code == 201
    return resp.json()


def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


async def _store_verify_code_in_redis(fake_redis, user_id, code=_VERIFY_CODE, new_email=None):
    """Helper: store a verification code directly in Redis."""
    code_hash = hashlib.sha256(code.encode()).hexdigest()
    data = {"code_hash": code_hash, "attempts": 0}
    if new_email:
        data["new_email"] = new_email
    await fake_redis.setex(
        f"verify_code:{user_id}",
        86400,
        json.dumps(data),
    )


# --- 1. POST /auth/change-email ---


async def test_change_email_returns_200(app_client, fake_redis):
    """POST /auth/change-email with valid password + new email → 200."""
    reg = await _register(app_client, "old@example.com")

    resp = await app_client.post(
        CHANGE_EMAIL_URL,
        json={"new_email": "new@example.com", "password": _TEST_PASSWORD},
        headers=_auth(reg["access_token"]),
    )

    assert resp.status_code == 200
    assert resp.json()["ok"] is True


async def test_change_email_stores_pending_email_in_redis(app_client, fake_redis):
    """POST /auth/change-email stores new_email in Redis alongside the code."""
    reg = await _register(app_client, "old2@example.com")

    await app_client.post(
        CHANGE_EMAIL_URL,
        json={"new_email": "new2@example.com", "password": _TEST_PASSWORD},
        headers=_auth(reg["access_token"]),
    )

    raw = await fake_redis.get(f"verify_code:{reg['user_id']}")
    assert raw is not None
    data = json.loads(raw)
    assert data["new_email"] == "new2@example.com"
    assert "code_hash" in data


async def test_change_email_wrong_password_returns_401(app_client):
    """POST /auth/change-email with wrong password → 401."""
    reg = await _register(app_client, "old3@example.com")

    resp = await app_client.post(
        CHANGE_EMAIL_URL,
        json={"new_email": "new3@example.com", "password": "wrongPassword1"},
        headers=_auth(reg["access_token"]),
    )

    assert resp.status_code == 401


async def test_change_email_same_email_returns_400(app_client):
    """POST /auth/change-email with same email → 400 EMAIL_UNCHANGED."""
    reg = await _register(app_client, "same@example.com")

    resp = await app_client.post(
        CHANGE_EMAIL_URL,
        json={"new_email": "same@example.com", "password": _TEST_PASSWORD},
        headers=_auth(reg["access_token"]),
    )

    assert resp.status_code == 400


async def test_change_email_taken_returns_409(app_client):
    """POST /auth/change-email with email already taken → 409."""
    await _register(app_client, "taken@example.com")
    reg2 = await _register(app_client, "other@example.com")

    resp = await app_client.post(
        CHANGE_EMAIL_URL,
        json={"new_email": "taken@example.com", "password": _TEST_PASSWORD},
        headers=_auth(reg2["access_token"]),
    )

    assert resp.status_code == 409


# --- 2. POST /auth/verify-email with pending email change ---


async def test_verify_email_change_updates_email(app_client, fake_redis, db_session):
    """POST /auth/verify-email with pending new_email → email updated in DB."""
    reg = await _register(app_client, "change1@example.com")
    headers = _auth(reg["access_token"])

    # Store code with pending email change
    await _store_verify_code_in_redis(fake_redis, reg["user_id"], new_email="changed1@example.com")

    resp = await app_client.post(
        VERIFY_EMAIL_URL,
        json={"code": _VERIFY_CODE},
        headers=headers,
    )

    assert resp.status_code == 200

    # Verify email was actually changed
    me = await app_client.get(ME_URL, headers=headers)
    assert me.json()["email"] == "changed1@example.com"
    assert me.json()["email_verified"] is True


async def test_verify_email_change_does_not_require_unverified(app_client, fake_redis, db_session):
    """Email change works even if current email is already verified."""
    from uuid import UUID

    from sqlalchemy import select

    from app.models.user import User

    reg = await _register(app_client, "verified1@example.com")
    headers = _auth(reg["access_token"])

    # Mark current email as verified
    result = await db_session.execute(select(User).where(User.id == UUID(reg["user_id"])))
    user = result.scalar_one()
    user.email_verified = True
    await db_session.flush()

    # Store code with pending email change
    await _store_verify_code_in_redis(
        fake_redis, reg["user_id"], new_email="newverified1@example.com"
    )

    resp = await app_client.post(
        VERIFY_EMAIL_URL,
        json={"code": _VERIFY_CODE},
        headers=headers,
    )

    assert resp.status_code == 200

    me = await app_client.get(ME_URL, headers=headers)
    assert me.json()["email"] == "newverified1@example.com"


async def test_verify_email_change_wrong_code_returns_401(app_client, fake_redis):
    """POST /auth/verify-email with wrong code during email change → 401."""
    reg = await _register(app_client, "wrongcode@example.com")

    await _store_verify_code_in_redis(
        fake_redis, reg["user_id"], new_email="new_wrongcode@example.com"
    )

    resp = await app_client.post(
        VERIFY_EMAIL_URL,
        json={"code": "000000"},
        headers=_auth(reg["access_token"]),
    )

    assert resp.status_code == 401
