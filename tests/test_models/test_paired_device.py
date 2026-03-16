import uuid

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError

from app.models.paired_device import PairedDevice
from app.models.user import User


def _make_user(email: str = "test@example.com") -> User:
    return User(email=email, password_hash="hashed")


def _make_device(user_id: uuid.UUID, **overrides) -> PairedDevice:
    defaults = {
        "user_id": user_id,
        "device_id": "android-device-001",
        "device_token_hash": "a" * 64,
        "timezone": "America/New_York",
    }
    defaults.update(overrides)
    return PairedDevice(**defaults)


async def test_unique_constraint_on_user_id(db_session):
    """Second PairedDevice for the same user_id raises IntegrityError."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    device1 = _make_device(user.id)
    db_session.add(device1)
    await db_session.flush()

    device2 = _make_device(user.id, device_id="android-device-002", device_token_hash="b" * 64)
    db_session.add(device2)
    with pytest.raises(IntegrityError):
        await db_session.flush()


async def test_cascade_delete_on_user_removal(db_session):
    """Deleting a User automatically deletes the associated PairedDevice."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    device = _make_device(user.id)
    db_session.add(device)
    await db_session.flush()

    device_id = device.id

    await db_session.delete(user)
    await db_session.flush()

    result = await db_session.execute(select(PairedDevice).where(PairedDevice.id == device_id))
    assert result.scalar_one_or_none() is None


async def test_registered_at_auto_set_on_creation(db_session):
    """registered_at is automatically set via server_default when not provided."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    device = _make_device(user.id)
    db_session.add(device)
    await db_session.flush()

    # Refresh to get server-generated value
    await db_session.refresh(device)
    assert device.registered_at is not None


async def test_unique_index_on_device_id(db_session):
    """Duplicate device_id raises IntegrityError due to UNIQUE INDEX."""
    user1 = _make_user("user1@example.com")
    user2 = _make_user("user2@example.com")
    db_session.add_all([user1, user2])
    await db_session.flush()

    device1 = _make_device(user1.id, device_id="same-device")
    db_session.add(device1)
    await db_session.flush()

    device2 = _make_device(user2.id, device_id="same-device", device_token_hash="b" * 64)
    db_session.add(device2)
    with pytest.raises(IntegrityError):
        await db_session.flush()


async def test_index_exists_on_device_token_hash(db_session):
    """Index idx_paired_devices_token_hash exists on device_token_hash column."""
    result = await db_session.execute(
        text(
            "SELECT indexname FROM pg_indexes "
            "WHERE tablename = 'paired_devices' "
            "AND indexname = 'idx_paired_devices_token_hash'"
        )
    )
    row = result.scalar_one_or_none()
    assert row == "idx_paired_devices_token_hash"
