"""Tests for password reset Redis helper functions.

Covers:
- store_reset_code: code storage with hash and attempts
- verify_reset_code: code verification with attempt counting
- delete_reset_code: code cleanup after successful reset
"""

import hashlib
import json

import pytest
from fastapi import HTTPException

from app.services.auth_service import (
    _RESET_CODE_TTL,
    delete_reset_code,
    store_reset_code,
    verify_reset_code,
)

# ===========================================================================
# store_reset_code tests
# ===========================================================================


async def test_store_reset_code_stores_hash_and_zero_attempts(fake_redis):
    """store_reset_code stores JSON with code_hash and attempts=0."""
    code = "84729123"
    email = "test@example.com"

    await store_reset_code(fake_redis, email, code)

    stored = await fake_redis.get(f"reset_code:{email}")
    assert stored is not None
    data = json.loads(stored)
    expected_hash = hashlib.sha256(code.encode()).hexdigest()
    assert data["code_hash"] == expected_hash
    assert data["attempts"] == 0


async def test_store_reset_code_uses_correct_ttl(fake_redis):
    """store_reset_code calls setex with 900-second TTL."""
    await store_reset_code(fake_redis, "ttl@example.com", "12345678")

    setex_calls = fake_redis.setex.call_args_list
    code_setex = [c for c in setex_calls if "reset_code:ttl@example.com" in str(c)]
    assert len(code_setex) == 1
    assert code_setex[0][0][1] == _RESET_CODE_TTL


async def test_store_reset_code_overwrites_previous(fake_redis):
    """Second store_reset_code for the same email overwrites the first."""
    email = "overwrite@example.com"

    await store_reset_code(fake_redis, email, "11111111")
    await store_reset_code(fake_redis, email, "22222222")

    stored = await fake_redis.get(f"reset_code:{email}")
    data = json.loads(stored)
    expected_hash = hashlib.sha256(b"22222222").hexdigest()
    assert data["code_hash"] == expected_hash
    assert data["attempts"] == 0


async def test_store_reset_code_different_emails_independent(fake_redis):
    """Codes for different emails are independent."""
    await store_reset_code(fake_redis, "a@example.com", "11111111")
    await store_reset_code(fake_redis, "b@example.com", "22222222")

    data_a = json.loads(await fake_redis.get("reset_code:a@example.com"))
    data_b = json.loads(await fake_redis.get("reset_code:b@example.com"))

    assert data_a["code_hash"] == hashlib.sha256(b"11111111").hexdigest()
    assert data_b["code_hash"] == hashlib.sha256(b"22222222").hexdigest()


# ===========================================================================
# verify_reset_code tests
# ===========================================================================


async def test_verify_reset_code_valid_code_returns_true(fake_redis):
    """verify_reset_code returns True for a valid code."""
    email = "valid@example.com"
    code = "84729123"
    await store_reset_code(fake_redis, email, code)

    result = await verify_reset_code(fake_redis, email, code)
    assert result is True


async def test_verify_reset_code_no_code_raises_401(fake_redis):
    """verify_reset_code raises 401 when no code exists for email."""
    with pytest.raises(HTTPException) as exc_info:
        await verify_reset_code(fake_redis, "nocode@example.com", "12345678")

    assert exc_info.value.status_code == 401
    assert exc_info.value.detail == "INVALID_RESET_CODE"


async def test_verify_reset_code_wrong_code_increments_attempts(fake_redis):
    """verify_reset_code increments attempts on wrong code."""
    email = "wrong@example.com"
    await store_reset_code(fake_redis, email, "84729123")

    with pytest.raises(HTTPException):
        await verify_reset_code(fake_redis, email, "00000000")

    stored = await fake_redis.get(f"reset_code:{email}")
    data = json.loads(stored)
    assert data["attempts"] == 1


async def test_verify_reset_code_5_wrong_attempts_deletes_code(fake_redis):
    """After 5 wrong attempts, the code is deleted from Redis."""
    email = "maxattempts@example.com"
    await store_reset_code(fake_redis, email, "84729123")

    # Manually set attempts to 5 to simulate 5 prior failures
    code_hash = hashlib.sha256(b"84729123").hexdigest()
    await fake_redis.setex(
        f"reset_code:{email}",
        900,
        json.dumps({"code_hash": code_hash, "attempts": 5}),
    )

    with pytest.raises(HTTPException) as exc_info:
        await verify_reset_code(fake_redis, email, "84729123")

    assert exc_info.value.status_code == 401
    # Code should be deleted
    assert await fake_redis.get(f"reset_code:{email}") is None


# ===========================================================================
# delete_reset_code tests
# ===========================================================================


async def test_delete_reset_code_removes_key(fake_redis):
    """delete_reset_code removes the reset_code key from Redis."""
    email = "delete@example.com"
    await store_reset_code(fake_redis, email, "12345678")

    await delete_reset_code(fake_redis, email)

    assert await fake_redis.get(f"reset_code:{email}") is None
