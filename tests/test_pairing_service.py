import hashlib
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch
from uuid import UUID, uuid4

import pytest
from fastapi import HTTPException
from redis.exceptions import RedisError
from sqlalchemy import select

from app.models.accept_failure import AcceptFailure
from app.models.paired_device import PairedDevice
from app.models.user import User
from app.services.pairing_service import (
    PAIRING_CODE_TTL,
    confirm_pairing,
    generate_pairing_code,
)

# --- Helpers ---


def _make_fake_redis():
    """Create a Redis mock with in-memory store supporting get, setex, delete."""
    store: dict[str, str] = {}

    async def mock_get(key):
        return store.get(key)

    async def mock_setex(key, ttl, value):
        store[key] = value

    async def mock_delete(*keys):
        count = 0
        for key in keys:
            if key in store:
                del store[key]
                count += 1
        return count

    redis = AsyncMock()
    redis.get = AsyncMock(side_effect=mock_get)
    redis.setex = AsyncMock(side_effect=mock_setex)
    redis.delete = AsyncMock(side_effect=mock_delete)
    redis._store = store
    return redis


def _make_broken_redis():
    """Create a Redis mock that always raises RedisError."""
    redis = AsyncMock()
    redis.get = AsyncMock(side_effect=RedisError("Connection refused"))
    redis.setex = AsyncMock(side_effect=RedisError("Connection refused"))
    redis.delete = AsyncMock(side_effect=RedisError("Connection refused"))
    return redis


# --- Test: returns 6-digit string ---


async def test_generate_pairing_code_returns_6_digit_string():
    fake_redis = _make_fake_redis()
    user_id = uuid4()

    code, _ = await generate_pairing_code(fake_redis, user_id)

    assert len(code) == 6
    assert code.isdigit()
    assert 100000 <= int(code) <= 999999


# --- Test: code stored in Redis with correct value ---


async def test_generate_pairing_code_stores_code_to_user_mapping():
    fake_redis = _make_fake_redis()
    user_id = uuid4()

    code, _ = await generate_pairing_code(fake_redis, user_id)

    stored_user_id = fake_redis._store.get(f"pairing_code:{code}")
    assert stored_user_id == str(user_id)


# --- Test: TTL is 300 seconds ---


async def test_generate_pairing_code_sets_ttl_300():
    fake_redis = _make_fake_redis()
    user_id = uuid4()

    code, _ = await generate_pairing_code(fake_redis, user_id)

    fake_redis.setex.assert_any_call(f"pairing_code:{code}", PAIRING_CODE_TTL, str(user_id))


# --- Test: each call generates different code (statistically) ---


async def test_generate_pairing_code_produces_varying_codes():
    """With 900000 possible codes, 10 calls should yield at least 2 distinct values."""
    fake_redis = _make_fake_redis()
    user_id = uuid4()

    codes = set()
    for _ in range(10):
        code, _ = await generate_pairing_code(fake_redis, user_id)
        codes.add(code)

    assert len(codes) > 1


# --- Test: expires_at is correct ---


async def test_generate_pairing_code_returns_correct_expires_at():
    fake_redis = _make_fake_redis()
    user_id = uuid4()

    before = datetime.now(UTC)
    _, expires_at = await generate_pairing_code(fake_redis, user_id)
    after = datetime.now(UTC)

    assert expires_at.tzinfo is not None
    expected_min = before + timedelta(seconds=PAIRING_CODE_TTL)
    expected_max = after + timedelta(seconds=PAIRING_CODE_TTL)
    assert expected_min <= expires_at <= expected_max


# --- Test: previous unused code invalidated ---


async def test_generate_pairing_code_invalidates_previous_code():
    fake_redis = _make_fake_redis()
    user_id = uuid4()

    # Generate first code
    code1, _ = await generate_pairing_code(fake_redis, user_id)
    assert f"pairing_code:{code1}" in fake_redis._store

    # Generate second code with a deterministic value to avoid collision
    with patch("app.services.pairing_service.random.randint", return_value=999999):
        code2, _ = await generate_pairing_code(fake_redis, user_id)

    assert code2 == "999999"
    # Old code must be gone
    assert f"pairing_code:{code1}" not in fake_redis._store
    # New code must exist
    assert fake_redis._store.get(f"pairing_code:{code2}") == str(user_id)


# --- Test: reverse key stored for future invalidation ---


async def test_generate_pairing_code_stores_reverse_key():
    fake_redis = _make_fake_redis()
    user_id = uuid4()

    code, _ = await generate_pairing_code(fake_redis, user_id)

    reverse_key = f"user_pairing:{user_id}"
    assert fake_redis._store.get(reverse_key) == code


async def test_generate_pairing_code_reverse_key_has_same_ttl():
    fake_redis = _make_fake_redis()
    user_id = uuid4()

    code, _ = await generate_pairing_code(fake_redis, user_id)

    reverse_key = f"user_pairing:{user_id}"
    fake_redis.setex.assert_any_call(reverse_key, PAIRING_CODE_TTL, code)


# --- Test: Redis unavailable raises 503 ---


async def test_generate_pairing_code_redis_unavailable_raises_503():
    broken_redis = _make_broken_redis()
    user_id = uuid4()

    with pytest.raises(HTTPException) as exc_info:
        await generate_pairing_code(broken_redis, user_id)

    assert exc_info.value.status_code == 503
    assert exc_info.value.detail == "SERVICE_UNAVAILABLE"


# --- Test: no previous code means no delete call ---


async def test_generate_pairing_code_no_delete_when_no_previous_code():
    fake_redis = _make_fake_redis()
    user_id = uuid4()

    await generate_pairing_code(fake_redis, user_id)

    # delete should not have been called (no old code existed)
    fake_redis.delete.assert_not_called()


# ===========================================================================
# confirm_pairing tests
# ===========================================================================


def _make_fake_redis_with_code(code: str, user_id) -> AsyncMock:
    """Create a fake Redis pre-loaded with a pairing code."""
    store: dict[str, str] = {f"pairing_code:{code}": str(user_id)}

    async def mock_get(key):
        return store.get(key)

    async def mock_setex(key, ttl, value):
        store[key] = value

    async def mock_delete(*keys):
        count = 0
        for key in keys:
            if key in store:
                del store[key]
                count += 1
        return count

    async def mock_exists(*keys):
        return sum(1 for key in keys if key in store)

    redis = AsyncMock()
    redis.get = AsyncMock(side_effect=mock_get)
    redis.setex = AsyncMock(side_effect=mock_setex)
    redis.delete = AsyncMock(side_effect=mock_delete)
    redis.exists = AsyncMock(side_effect=mock_exists)
    redis._store = store
    return redis


async def _create_user(db, email="test@example.com") -> User:
    """Insert a User row and return it."""
    user = User(email=email, password_hash="hashed")
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


# --- Test 1: valid code → device_token (UUID string) ---


async def test_confirm_pairing_valid_code_returns_device_token(db_session):
    user = await _create_user(db_session)
    code = "123456"
    fake_redis = _make_fake_redis_with_code(code, user.id)

    device_token, returned_user_id = await confirm_pairing(
        code, "android-dev-001", "America/New_York", fake_redis, db_session
    )

    # device_token must be a valid UUID string
    UUID(device_token)  # raises ValueError if not a valid UUID
    assert returned_user_id == user.id


# --- Test 2: invalid/expired code → 404 PAIRING_CODE_EXPIRED ---


async def test_confirm_pairing_invalid_code_raises_404():
    fake_redis = AsyncMock()
    fake_redis.get = AsyncMock(return_value=None)
    fake_redis.exists = AsyncMock(return_value=0)
    db = AsyncMock()

    with pytest.raises(HTTPException) as exc_info:
        await confirm_pairing("000000", "dev-001", "America/New_York", fake_redis, db)

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail == "PAIRING_CODE_EXPIRED"


# --- Test 3: expired code (TTL passed) → 404 PAIRING_CODE_EXPIRED ---


async def test_confirm_pairing_expired_code_raises_404():
    """An expired code is gone from Redis — identical to invalid code."""
    fake_redis = AsyncMock()
    fake_redis.get = AsyncMock(return_value=None)
    fake_redis.exists = AsyncMock(return_value=0)
    db = AsyncMock()

    with pytest.raises(HTTPException) as exc_info:
        await confirm_pairing("999999", "dev-001", "America/New_York", fake_redis, db)

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail == "PAIRING_CODE_EXPIRED"


# --- Test 3b: already-used code → 409 PAIRING_CODE_USED ---


async def test_confirm_pairing_used_code_raises_409():
    """A code that was already consumed is tracked in Redis — returns 409."""
    fake_redis = AsyncMock()
    fake_redis.get = AsyncMock(return_value=None)
    fake_redis.exists = AsyncMock(return_value=1)  # used_pairing_code:{code} exists
    db = AsyncMock()

    with pytest.raises(HTTPException) as exc_info:
        await confirm_pairing("111111", "dev-001", "America/New_York", fake_redis, db)

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == "PAIRING_CODE_USED"


# --- Test 4: invalid timezone → 422 INVALID_TIMEZONE ---


async def test_confirm_pairing_invalid_timezone_raises_422():
    fake_redis = AsyncMock()
    db = AsyncMock()

    with pytest.raises(HTTPException) as exc_info:
        await confirm_pairing("123456", "dev-001", "Not/A/Timezone", fake_redis, db)

    assert exc_info.value.status_code == 422
    assert exc_info.value.detail == "INVALID_TIMEZONE"


# --- Test 5: device_id already paired to another user → old record deleted ---


async def test_confirm_pairing_device_paired_to_other_user_is_replaced(db_session):
    user_a = await _create_user(db_session, "usera@example.com")
    user_b = await _create_user(db_session, "userb@example.com")

    # Pair device to UserA
    old_device = await _create_device(db_session, user_a.id, "shared-dev")
    old_device_id = old_device.id

    code = "555555"
    fake_redis = _make_fake_redis_with_code(code, user_b.id)

    await confirm_pairing(code, "shared-dev", "Europe/London", fake_redis, db_session)

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


async def test_confirm_pairing_user_with_existing_device_cleanup(db_session):
    user = await _create_user(db_session)

    # Pair user with old device
    old_device = await _create_device(db_session, user.id, "old-dev")
    old_device_id = old_device.id

    # Create accept failures
    await _create_accept_failure(db_session, user.id, "Reason1")
    await _create_accept_failure(db_session, user.id, "Reason2")

    code = "777777"
    fake_redis = _make_fake_redis_with_code(code, user.id)

    await confirm_pairing(code, "new-dev", "Asia/Tokyo", fake_redis, db_session)

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


async def test_confirm_pairing_stores_sha256_hash_of_token(db_session):
    user = await _create_user(db_session)
    code = "888888"
    fake_redis = _make_fake_redis_with_code(code, user.id)

    device_token, _ = await confirm_pairing(
        code, "hash-test-dev", "America/Chicago", fake_redis, db_session
    )

    expected_hash = hashlib.sha256(device_token.encode()).hexdigest()

    result = await db_session.execute(select(PairedDevice).where(PairedDevice.user_id == user.id))
    device = result.scalar_one()
    assert device.device_token_hash == expected_hash
    assert device.device_token_hash != device_token  # not stored as plaintext


# --- Test 9: device_model stored when provided ---


async def test_confirm_pairing_stores_device_model(db_session):
    user = await _create_user(db_session)
    code = "111111"
    fake_redis = _make_fake_redis_with_code(code, user.id)

    await confirm_pairing(
        code,
        "model-test-dev",
        "America/New_York",
        fake_redis,
        db_session,
        device_model="Samsung SM-A156U",
    )

    result = await db_session.execute(select(PairedDevice).where(PairedDevice.user_id == user.id))
    device = result.scalar_one()
    assert device.device_model == "Samsung SM-A156U"


# --- Test 10: device_model is None when not provided ---


async def test_confirm_pairing_device_model_none_by_default(db_session):
    user = await _create_user(db_session)
    code = "222222"
    fake_redis = _make_fake_redis_with_code(code, user.id)

    await confirm_pairing(code, "no-model-dev", "America/New_York", fake_redis, db_session)

    result = await db_session.execute(select(PairedDevice).where(PairedDevice.user_id == user.id))
    device = result.scalar_one()
    assert device.device_model is None


# --- Test 8: Redis unavailable during confirm → 503 SERVICE_UNAVAILABLE ---


async def test_confirm_pairing_redis_unavailable_raises_503():
    broken_redis = AsyncMock()
    broken_redis.get = AsyncMock(side_effect=RedisError("Connection refused"))
    db = AsyncMock()

    with pytest.raises(HTTPException) as exc_info:
        await confirm_pairing("123456", "dev-001", "America/New_York", broken_redis, db)

    assert exc_info.value.status_code == 503
    assert exc_info.value.detail == "SERVICE_UNAVAILABLE"
