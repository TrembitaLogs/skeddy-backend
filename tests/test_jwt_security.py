"""Tests for JWT security improvements (SKE-74).

1. Access token invalidated after password reset (password_changed_at check)
2. Access token invalidated after password change (password_changed_at check)
3. Access token blacklisted on account deletion
4. Refresh tokens use cryptographic randomness (secrets.token_urlsafe)
5. JWT_SECRET validation in config
"""

import hashlib
import json
import time

import pytest

from app.config import Settings
from app.services.auth_service import create_refresh_token

REGISTER_URL = "/api/v1/auth/register"
LOGIN_URL = "/api/v1/auth/login"
RESET_PASSWORD_URL = "/api/v1/auth/reset-password"
CHANGE_PASSWORD_URL = "/api/v1/auth/change-password"
DELETE_ACCOUNT_URL = "/api/v1/auth/account"
ME_URL = "/api/v1/auth/me"

_TEST_PASSWORD = "securePass1"
_NEW_PASSWORD = "newSecurePass2"
_RESET_CODE = "84729123"


async def _register_user(app_client, email):
    """Helper: register a user and return response data."""
    resp = await app_client.post(REGISTER_URL, json={"email": email, "password": _TEST_PASSWORD})
    assert resp.status_code == 201
    return resp.json()


def _auth_header(access_token: str) -> dict:
    return {"Authorization": f"Bearer {access_token}"}


async def _store_reset_code(fake_redis, email, code=_RESET_CODE):
    """Helper: store a reset code in fake Redis."""
    code_hash = hashlib.sha256(code.encode()).hexdigest()
    await fake_redis.setex(
        f"reset_code:{email}",
        900,
        json.dumps({"code_hash": code_hash, "attempts": 0}),
    )


# --- S-01: Access token invalidated after password reset ---


async def test_old_access_token_rejected_after_password_reset(app_client, fake_redis):
    """After password reset, old access tokens must be rejected (password_changed_at)."""
    email = "jwt-s01-reset@example.com"
    reg = await _register_user(app_client, email)
    old_token = reg["access_token"]

    # Verify token works before reset
    me_resp = await app_client.get(ME_URL, headers=_auth_header(old_token))
    assert me_resp.status_code == 200

    # Wait 1 second so password_changed_at > iat
    time.sleep(1)

    # Reset password
    await _store_reset_code(fake_redis, email)
    reset_resp = await app_client.post(
        RESET_PASSWORD_URL,
        json={"email": email, "code": _RESET_CODE, "new_password": _NEW_PASSWORD},
    )
    assert reset_resp.status_code == 200

    # Old token should now be rejected
    me_resp2 = await app_client.get(ME_URL, headers=_auth_header(old_token))
    assert me_resp2.status_code == 401


# --- S-01: Access token invalidated after password change ---


async def test_old_access_token_rejected_after_password_change(app_client):
    """After change-password, old access tokens must be rejected (password_changed_at)."""
    email = "jwt-s01-change@example.com"
    reg = await _register_user(app_client, email)
    old_token = reg["access_token"]

    # Verify token works before change
    me_resp = await app_client.get(ME_URL, headers=_auth_header(old_token))
    assert me_resp.status_code == 200

    # Wait 1 second so password_changed_at > iat
    time.sleep(1)

    # Change password
    change_resp = await app_client.post(
        CHANGE_PASSWORD_URL,
        json={"current_password": _TEST_PASSWORD, "new_password": _NEW_PASSWORD},
        headers=_auth_header(old_token),
    )
    assert change_resp.status_code == 200

    # Old token should now be rejected
    me_resp2 = await app_client.get(ME_URL, headers=_auth_header(old_token))
    assert me_resp2.status_code == 401


# --- S-01: Newly issued token works after password change ---


async def test_new_token_works_after_password_change(app_client):
    """After password change, a freshly obtained token must work."""
    email = "jwt-s01-newtoken@example.com"
    await _register_user(app_client, email)

    # Wait and change password
    time.sleep(1)

    # Login to get a token, then change password
    login1 = await app_client.post(LOGIN_URL, json={"email": email, "password": _TEST_PASSWORD})
    assert login1.status_code == 200
    token1 = login1.json()["access_token"]

    change_resp = await app_client.post(
        CHANGE_PASSWORD_URL,
        json={"current_password": _TEST_PASSWORD, "new_password": _NEW_PASSWORD},
        headers=_auth_header(token1),
    )
    assert change_resp.status_code == 200

    # Login with new password to get a fresh token
    login2 = await app_client.post(LOGIN_URL, json={"email": email, "password": _NEW_PASSWORD})
    assert login2.status_code == 200
    new_token = login2.json()["access_token"]

    # Fresh token should work
    me_resp = await app_client.get(ME_URL, headers=_auth_header(new_token))
    assert me_resp.status_code == 200


# --- S-02: Access token blacklisted on account deletion ---


async def test_access_token_blacklisted_after_account_deletion(app_client, fake_redis):
    """After account deletion, the access token must be blacklisted in Redis."""
    email = "jwt-s02-del@example.com"
    reg = await _register_user(app_client, email)
    token = reg["access_token"]

    # Delete account
    del_resp = await app_client.request(
        "DELETE",
        DELETE_ACCOUNT_URL,
        json={"password": _TEST_PASSWORD},
        headers=_auth_header(token),
    )
    assert del_resp.status_code == 200

    # Verify the token's JTI is in the blacklist
    from app.services.auth_service import decode_access_token

    payload = decode_access_token(token)
    assert payload is not None
    jti = payload["jti"]
    blacklisted = await fake_redis.exists(f"blacklist:{jti}")
    assert blacklisted > 0


# --- AC-01: Refresh tokens use secrets.token_urlsafe ---


def test_refresh_token_is_not_uuid_format():
    """Refresh tokens must use secrets.token_urlsafe, not UUID format."""
    token = create_refresh_token()
    # UUID format: 8-4-4-4-12 hex chars with dashes
    # token_urlsafe produces base64url chars without dashes
    assert "-" not in token or len(token) > 36
    # token_urlsafe(32) produces ~43 chars
    assert len(token) > 36


# --- Config: JWT_SECRET validation ---


def test_jwt_secret_short_raises_in_production():
    """JWT_SECRET < 32 chars must raise ValueError in production/staging."""
    with pytest.raises(ValueError, match="JWT_SECRET must be at least 32"):
        Settings(
            DATABASE_URL="postgresql+asyncpg://x:x@localhost/x",
            REDIS_URL="redis://localhost",
            JWT_SECRET="short-secret",
            ENVIRONMENT="production",
            ADMIN_SECRET_KEY="a" * 32,
            ADMIN_PASSWORD="$2b$12$test",
            CORS_ORIGINS="https://example.com",
            SENTRY_DSN="https://sentry.example.com/1",
        )


def test_jwt_secret_default_logs_warning(caplog):
    """JWT_SECRET with default placeholder value must log a warning."""
    import logging

    with caplog.at_level(logging.WARNING, logger="app.config"):
        Settings(
            DATABASE_URL="postgresql+asyncpg://x:x@localhost/x",
            REDIS_URL="redis://localhost",
            JWT_SECRET="your-super-secret-key-change-in-production",
            ENVIRONMENT="dev",
        )
    assert "default/placeholder" in caplog.text


def test_jwt_secret_valid_no_error():
    """JWT_SECRET >= 32 chars in production must not raise."""
    s = Settings(
        DATABASE_URL="postgresql+asyncpg://x:x@localhost/x",
        REDIS_URL="redis://localhost",
        JWT_SECRET="a" * 64,
        ENVIRONMENT="production",
        ADMIN_SECRET_KEY="b" * 32,
        ADMIN_PASSWORD="$2b$12$test",
        CORS_ORIGINS="https://example.com",
        SENTRY_DSN="https://sentry.example.com/1",
    )
    assert len(s.JWT_SECRET) == 64
