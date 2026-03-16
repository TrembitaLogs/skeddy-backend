import hashlib
from uuid import UUID

import pytest
from fastapi import HTTPException
from sqlalchemy import select

from app.models.accept_failure import AcceptFailure
from app.models.paired_device import PairedDevice
from app.models.user import User
from app.services.auth_service import hash_password
from app.services.pairing_service import search_login

_TEST_PASSWORD = "securePass1"
_TEST_PASSWORD_HASH = hash_password(_TEST_PASSWORD)


# --- Helpers ---


async def _create_user(db, email="test@example.com") -> User:
    """Insert a User row with a proper bcrypt-hashed password and return it."""
    user = User(email=email, password_hash=_TEST_PASSWORD_HASH)
    db.add(user)
    await db.flush()
    return user


async def _create_device(
    db, user_id, device_id="dev-001", token_hash="a" * 64, tz="America/New_York"
) -> PairedDevice:
    """Insert a PairedDevice row and return it."""
    device = PairedDevice(
        user_id=user_id,
        device_id=device_id,
        device_token_hash=token_hash,
        timezone=tz,
    )
    db.add(device)
    await db.flush()
    return device


async def _create_accept_failure(db, user_id, reason="TestReason") -> AcceptFailure:
    """Insert an AcceptFailure row and return it."""
    failure = AcceptFailure(user_id=user_id, reason=reason)
    db.add(failure)
    await db.flush()
    return failure


# --- Test 1: valid credentials → device_token (UUID string) ---


async def test_search_login_valid_credentials_returns_device_token(db_session):
    user = await _create_user(db_session)

    device_token, returned_user_id = await search_login(
        email=user.email,
        password=_TEST_PASSWORD,
        device_id="android-dev-001",
        timezone_str="America/New_York",
        db=db_session,
    )

    # device_token must be a valid UUID string
    UUID(device_token)  # raises ValueError if not a valid UUID
    assert returned_user_id == user.id


# --- Test 2: invalid password → 401 INVALID_CREDENTIALS ---


async def test_search_login_invalid_password_raises_401(db_session):
    user = await _create_user(db_session)

    with pytest.raises(HTTPException) as exc_info:
        await search_login(
            email=user.email,
            password="wrongPassword123",
            device_id="dev-001",
            timezone_str="America/New_York",
            db=db_session,
        )

    assert exc_info.value.status_code == 401
    assert exc_info.value.detail == "INVALID_CREDENTIALS"


# --- Test 3: non-existent email → 401 INVALID_CREDENTIALS ---


async def test_search_login_unknown_email_raises_401(db_session):
    with pytest.raises(HTTPException) as exc_info:
        await search_login(
            email="ghost@example.com",
            password=_TEST_PASSWORD,
            device_id="dev-001",
            timezone_str="America/New_York",
            db=db_session,
        )

    assert exc_info.value.status_code == 401
    assert exc_info.value.detail == "INVALID_CREDENTIALS"


# --- Test 4: invalid timezone → 422 INVALID_TIMEZONE ---


async def test_search_login_invalid_timezone_raises_422(db_session):
    user = await _create_user(db_session)

    with pytest.raises(HTTPException) as exc_info:
        await search_login(
            email=user.email,
            password=_TEST_PASSWORD,
            device_id="dev-001",
            timezone_str="Not/A/Timezone",
            db=db_session,
        )

    assert exc_info.value.status_code == 422
    assert exc_info.value.detail == "INVALID_TIMEZONE"


# --- Test 5: device_id already paired to another user → old record deleted ---


async def test_search_login_device_paired_to_other_user_is_replaced(db_session):
    user_a = await _create_user(db_session, "usera@example.com")
    user_b = await _create_user(db_session, "userb@example.com")

    # Pair device to UserA
    old_device = await _create_device(db_session, user_a.id, "shared-dev")
    old_device_id = old_device.id

    await search_login(
        email=user_b.email,
        password=_TEST_PASSWORD,
        device_id="shared-dev",
        timezone_str="Europe/London",
        db=db_session,
    )

    # Old device (UserA) must be gone
    result = await db_session.execute(select(PairedDevice).where(PairedDevice.id == old_device_id))
    assert result.scalar_one_or_none() is None

    # New device must belong to UserB
    result = await db_session.execute(
        select(PairedDevice).where(PairedDevice.user_id == user_b.id)
    )
    new_device = result.scalar_one_or_none()
    assert new_device is not None
    assert new_device.device_id == "shared-dev"
    assert new_device.timezone == "Europe/London"


# --- Test 6: user already has device → old device deleted, accept_failures cleaned ---


async def test_search_login_user_with_existing_device_cleanup(db_session):
    user = await _create_user(db_session)

    # Pair user with old device
    old_device = await _create_device(db_session, user.id, "old-dev")
    old_device_id = old_device.id

    # Create accept failures
    await _create_accept_failure(db_session, user.id, "Reason1")
    await _create_accept_failure(db_session, user.id, "Reason2")

    await search_login(
        email=user.email,
        password=_TEST_PASSWORD,
        device_id="new-dev",
        timezone_str="Asia/Tokyo",
        db=db_session,
    )

    # Old device must be deleted
    result = await db_session.execute(select(PairedDevice).where(PairedDevice.id == old_device_id))
    assert result.scalar_one_or_none() is None

    # Accept failures must be cleaned
    result = await db_session.execute(
        select(AcceptFailure).where(AcceptFailure.user_id == user.id)
    )
    assert result.scalars().all() == []

    # New device must exist
    result = await db_session.execute(select(PairedDevice).where(PairedDevice.user_id == user.id))
    new_device = result.scalar_one_or_none()
    assert new_device is not None
    assert new_device.device_id == "new-dev"
    assert new_device.timezone == "Asia/Tokyo"


# --- Test 7: device_token hashed with SHA256 before storing in DB ---


async def test_search_login_stores_sha256_hash_of_token(db_session):
    user = await _create_user(db_session)

    device_token, _ = await search_login(
        email=user.email,
        password=_TEST_PASSWORD,
        device_id="hash-test-dev",
        timezone_str="America/Chicago",
        db=db_session,
    )

    expected_hash = hashlib.sha256(device_token.encode()).hexdigest()

    result = await db_session.execute(select(PairedDevice).where(PairedDevice.user_id == user.id))
    device = result.scalar_one()
    assert device.device_token_hash == expected_hash
    assert device.device_token_hash != device_token  # not stored as plaintext


# --- Test 8: device_model stored when provided ---


async def test_search_login_stores_device_model(db_session):
    user = await _create_user(db_session)

    await search_login(
        email=user.email,
        password=_TEST_PASSWORD,
        device_id="model-test-dev",
        timezone_str="America/New_York",
        db=db_session,
        device_model="Samsung SM-A156U",
    )

    result = await db_session.execute(select(PairedDevice).where(PairedDevice.user_id == user.id))
    device = result.scalar_one()
    assert device.device_model == "Samsung SM-A156U"


# --- Test 9: device_model is None when not provided ---


async def test_search_login_device_model_none_by_default(db_session):
    user = await _create_user(db_session)

    await search_login(
        email=user.email,
        password=_TEST_PASSWORD,
        device_id="no-model-dev",
        timezone_str="America/New_York",
        db=db_session,
    )

    result = await db_session.execute(select(PairedDevice).where(PairedDevice.user_id == user.id))
    device = result.scalar_one()
    assert device.device_model is None


# --- Test 10: same device_id re-login by same user (no conflict with other user) ---


async def test_search_login_same_device_same_user_replaces_record(db_session):
    """Re-login on same device generates new token and replaces old device record."""
    user = await _create_user(db_session)

    token1, _ = await search_login(
        email=user.email,
        password=_TEST_PASSWORD,
        device_id="my-device",
        timezone_str="America/New_York",
        db=db_session,
    )

    token2, _ = await search_login(
        email=user.email,
        password=_TEST_PASSWORD,
        device_id="my-device",
        timezone_str="US/Pacific",
        db=db_session,
    )

    # Tokens should differ
    assert token1 != token2

    # Only one device record should exist
    result = await db_session.execute(select(PairedDevice).where(PairedDevice.user_id == user.id))
    devices = result.scalars().all()
    assert len(devices) == 1
    assert devices[0].timezone == "US/Pacific"
