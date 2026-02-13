"""Tests for password reset token Redis helper functions.

Test strategy (task 11.3):
1. store_reset_token → verify_reset_token returns correct user_id
2. verify_reset_token with non-existent token → None
3. delete_reset_token → verify_reset_token returns None
4. TTL expiration (verify setex called with correct TTL)
5. Tests with mocked Redis client (all tests use fake_redis fixture)
"""

import hashlib
from uuid import uuid4

from app.services.auth_service import (
    delete_reset_token,
    store_reset_token,
    verify_reset_token,
)

# --- Test Strategy 1: store → verify returns correct user_id ---


async def test_store_then_verify_returns_correct_user_id(fake_redis):
    """store_reset_token + verify_reset_token returns the stored user_id."""
    user_id = uuid4()
    token_hash = hashlib.sha256(b"test-token").hexdigest()

    await store_reset_token(fake_redis, user_id, token_hash)
    result = await verify_reset_token(fake_redis, token_hash)

    assert result == user_id


# --- Test Strategy 2: verify non-existent token → None ---


async def test_verify_nonexistent_token_returns_none(fake_redis):
    """verify_reset_token with unknown hash returns None."""
    token_hash = hashlib.sha256(b"does-not-exist").hexdigest()

    result = await verify_reset_token(fake_redis, token_hash)

    assert result is None


# --- Test Strategy 3: delete → verify returns None ---


async def test_delete_then_verify_returns_none(fake_redis):
    """After delete_reset_token, verify_reset_token returns None."""
    user_id = uuid4()
    token_hash = hashlib.sha256(b"delete-me").hexdigest()

    await store_reset_token(fake_redis, user_id, token_hash)
    await delete_reset_token(fake_redis, token_hash, user_id)

    result = await verify_reset_token(fake_redis, token_hash)
    assert result is None


async def test_delete_also_removes_user_tracking_key(fake_redis):
    """delete_reset_token with user_id removes the user_reset tracking key."""
    user_id = uuid4()
    token_hash = hashlib.sha256(b"track-me").hexdigest()

    await store_reset_token(fake_redis, user_id, token_hash)
    await delete_reset_token(fake_redis, token_hash, user_id)

    # Verify tracking key is also gone
    tracking_value = await fake_redis.get(f"user_reset:{user_id}")
    assert tracking_value is None


async def test_delete_without_user_id_keeps_tracking_key(fake_redis):
    """delete_reset_token without user_id leaves the tracking key intact."""
    user_id = uuid4()
    token_hash = hashlib.sha256(b"partial-delete").hexdigest()

    await store_reset_token(fake_redis, user_id, token_hash)
    await delete_reset_token(fake_redis, token_hash)

    # Token itself is gone
    assert await verify_reset_token(fake_redis, token_hash) is None
    # But tracking key remains
    tracking_value = await fake_redis.get(f"user_reset:{user_id}")
    assert tracking_value == token_hash


# --- Test Strategy 4: TTL expiration (verify setex called with correct TTL) ---


async def test_store_uses_correct_ttl(fake_redis):
    """store_reset_token calls setex with 3600-second TTL for both keys."""
    user_id = uuid4()
    token_hash = hashlib.sha256(b"ttl-test").hexdigest()

    await store_reset_token(fake_redis, user_id, token_hash)

    # Check token key TTL
    token_setex = [
        c for c in fake_redis.setex.call_args_list if f"reset_token:{token_hash}" in str(c)
    ]
    assert len(token_setex) == 1
    assert token_setex[0][0][1] == 3600

    # Check tracking key TTL
    tracking_setex = [
        c for c in fake_redis.setex.call_args_list if f"user_reset:{user_id}" in str(c)
    ]
    assert len(tracking_setex) == 1
    assert tracking_setex[0][0][1] == 3600


# --- Test Strategy 5: mocked Redis (covered by fake_redis fixture above) ---
# Additional: old token invalidation on second store


async def test_store_invalidates_previous_token(fake_redis):
    """Second store_reset_token for the same user invalidates the first token."""
    user_id = uuid4()
    first_hash = hashlib.sha256(b"first-token").hexdigest()
    second_hash = hashlib.sha256(b"second-token").hexdigest()

    await store_reset_token(fake_redis, user_id, first_hash)
    assert await verify_reset_token(fake_redis, first_hash) == user_id

    await store_reset_token(fake_redis, user_id, second_hash)

    # First token invalidated
    assert await verify_reset_token(fake_redis, first_hash) is None
    # Second token active
    assert await verify_reset_token(fake_redis, second_hash) == user_id


async def test_store_different_users_independent(fake_redis):
    """Tokens for different users are independent — storing for one does not affect another."""
    user_a = uuid4()
    user_b = uuid4()
    hash_a = hashlib.sha256(b"token-a").hexdigest()
    hash_b = hashlib.sha256(b"token-b").hexdigest()

    await store_reset_token(fake_redis, user_a, hash_a)
    await store_reset_token(fake_redis, user_b, hash_b)

    assert await verify_reset_token(fake_redis, hash_a) == user_a
    assert await verify_reset_token(fake_redis, hash_b) == user_b
