import uuid

import pytest
from sqlalchemy.exc import IntegrityError

from app.models.user import User


async def test_user_uuid_auto_generated(db_session):
    """UUID primary key is automatically generated when not provided."""
    user = User(email="test@example.com", password_hash="hashed_password")
    db_session.add(user)
    await db_session.flush()

    assert user.id is not None
    assert isinstance(user.id, uuid.UUID)


async def test_user_duplicate_email_raises_integrity_error(db_session):
    """Duplicate email raises IntegrityError due to UNIQUE constraint."""
    user1 = User(email="duplicate@example.com", password_hash="hash1")
    db_session.add(user1)
    await db_session.flush()

    user2 = User(email="duplicate@example.com", password_hash="hash2")
    db_session.add(user2)
    with pytest.raises(IntegrityError):
        await db_session.flush()


async def test_user_phone_number_nullable(db_session):
    """phone_number can be None (nullable)."""
    user = User(
        email="nophone@example.com",
        password_hash="hashed_password",
        phone_number=None,
    )
    db_session.add(user)
    await db_session.flush()

    assert user.phone_number is None
