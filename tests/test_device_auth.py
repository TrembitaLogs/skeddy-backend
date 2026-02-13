import hashlib

import pytest
from fastapi import APIRouter, Depends, HTTPException

from app.dependencies.device_auth import verify_device
from app.main import app
from app.models.paired_device import PairedDevice
from app.models.user import User

# ---------------------------------------------------------------------------
# Test route for integration tests (HTTP-level header validation)
# ---------------------------------------------------------------------------

_test_router = APIRouter()


@_test_router.get("/test/device-auth")
async def _test_device_auth_endpoint(
    device: PairedDevice = Depends(verify_device),
):
    return {
        "device_id": device.device_id,
        "user_id": str(device.user_id),
        "timezone": device.timezone,
    }


app.include_router(_test_router)

# ---------------------------------------------------------------------------
# Constants & helpers
# ---------------------------------------------------------------------------

DEVICE_TOKEN = "test-device-token-uuid-value"
DEVICE_TOKEN_HASH = hashlib.sha256(DEVICE_TOKEN.encode()).hexdigest()
DEVICE_ID = "android-device-001"
TIMEZONE = "America/New_York"


async def _create_user(db, email="device-auth@example.com") -> User:
    """Insert a User row and return it."""
    user = User(email=email, password_hash="hashed")
    db.add(user)
    await db.flush()
    return user


async def _create_paired_device(
    db,
    user_id,
    device_id=DEVICE_ID,
    token_hash=DEVICE_TOKEN_HASH,
    timezone=TIMEZONE,
) -> PairedDevice:
    """Insert a PairedDevice row and return it."""
    device = PairedDevice(
        user_id=user_id,
        device_id=device_id,
        device_token_hash=token_hash,
        timezone=timezone,
    )
    db.add(device)
    await db.flush()
    return device


# ---------------------------------------------------------------------------
# Unit tests (verify_device called directly)
# ---------------------------------------------------------------------------


# Test 1: valid headers -> returns PairedDevice object
async def test_verify_device_valid_headers_returns_paired_device(db_session):
    user = await _create_user(db_session)
    device = await _create_paired_device(db_session, user.id)

    result = await verify_device(
        x_device_token=DEVICE_TOKEN,
        x_device_id=DEVICE_ID,
        db=db_session,
    )

    assert isinstance(result, PairedDevice)
    assert result.id == device.id
    assert result.user_id == user.id


# Test 4: invalid token -> 401 INVALID_DEVICE_TOKEN
async def test_verify_device_invalid_token_raises_401(db_session):
    user = await _create_user(db_session)
    await _create_paired_device(db_session, user.id)

    with pytest.raises(HTTPException) as exc_info:
        await verify_device(
            x_device_token="completely-wrong-token",
            x_device_id=DEVICE_ID,
            db=db_session,
        )

    assert exc_info.value.status_code == 401
    assert exc_info.value.detail == "INVALID_DEVICE_TOKEN"


# Test 5: valid token but wrong device_id -> 401 INVALID_DEVICE_TOKEN
async def test_verify_device_valid_token_wrong_device_id_raises_401(db_session):
    user = await _create_user(db_session)
    await _create_paired_device(db_session, user.id)

    with pytest.raises(HTTPException) as exc_info:
        await verify_device(
            x_device_token=DEVICE_TOKEN,
            x_device_id="wrong-device-id",
            db=db_session,
        )

    assert exc_info.value.status_code == 401
    assert exc_info.value.detail == "INVALID_DEVICE_TOKEN"


# Test 6: device.user_id, device.timezone accessible after verify
async def test_verify_device_returned_device_has_accessible_fields(db_session):
    user = await _create_user(db_session)
    await _create_paired_device(db_session, user.id, timezone="Europe/Kyiv")

    result = await verify_device(
        x_device_token=DEVICE_TOKEN,
        x_device_id=DEVICE_ID,
        db=db_session,
    )

    assert result.user_id == user.id
    assert result.timezone == "Europe/Kyiv"
    assert result.device_id == DEVICE_ID
    assert result.device_token_hash == DEVICE_TOKEN_HASH


# ---------------------------------------------------------------------------
# Integration tests (HTTP-level header validation via test endpoint)
# ---------------------------------------------------------------------------


# Test 2: missing X-Device-Token header -> 422 Validation Error
async def test_verify_device_missing_token_header_returns_422(app_client):
    response = await app_client.get(
        "/test/device-auth",
        headers={"X-Device-Id": DEVICE_ID},
    )
    assert response.status_code == 422


# Test 3: missing X-Device-Id header -> 422 Validation Error
async def test_verify_device_missing_device_id_header_returns_422(app_client):
    response = await app_client.get(
        "/test/device-auth",
        headers={"X-Device-Token": DEVICE_TOKEN},
    )
    assert response.status_code == 422
