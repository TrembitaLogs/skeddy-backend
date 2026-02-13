from datetime import UTC, datetime, timedelta

from sqlalchemy import select, text

from app.models.refresh_token import RefreshToken
from app.models.user import User


async def test_refresh_token_cascade_delete_on_user_removal(db_session):
    """Deleting a User automatically deletes all associated refresh tokens (CASCADE)."""
    user = User(email="cascade@example.com", password_hash="hashed")
    db_session.add(user)
    await db_session.flush()

    token = RefreshToken(
        user_id=user.id,
        token_hash="a" * 64,
        expires_at=datetime.now(UTC) + timedelta(days=30),
    )
    db_session.add(token)
    await db_session.flush()

    token_id = token.id

    await db_session.delete(user)
    await db_session.flush()

    result = await db_session.execute(select(RefreshToken).where(RefreshToken.id == token_id))
    assert result.scalar_one_or_none() is None


async def test_refresh_token_index_exists_on_token_hash(db_session):
    """Index idx_refresh_tokens_hash exists on the token_hash column."""
    result = await db_session.execute(
        text(
            "SELECT indexname FROM pg_indexes "
            "WHERE tablename = 'refresh_tokens' AND indexname = 'idx_refresh_tokens_hash'"
        )
    )
    row = result.scalar_one_or_none()
    assert row == "idx_refresh_tokens_hash"


async def test_refresh_token_index_exists_on_user_id(db_session):
    """Index idx_refresh_tokens_user exists on the user_id column."""
    result = await db_session.execute(
        text(
            "SELECT indexname FROM pg_indexes "
            "WHERE tablename = 'refresh_tokens' AND indexname = 'idx_refresh_tokens_user'"
        )
    )
    row = result.scalar_one_or_none()
    assert row == "idx_refresh_tokens_user"


async def test_refresh_token_search_by_hash_uses_index(db_session):
    """EXPLAIN shows index scan when querying by token_hash."""
    await db_session.execute(text("SET LOCAL enable_seqscan = off"))

    result = await db_session.execute(
        text("EXPLAIN SELECT * FROM refresh_tokens WHERE token_hash = 'test_hash'")
    )
    plan = "\n".join(row[0] for row in result)
    assert "idx_refresh_tokens_hash" in plan
