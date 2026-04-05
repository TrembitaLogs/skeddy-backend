from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.paired_device import PairedDevice
from app.models.search_status import SearchStatus


async def get_search_status(db: AsyncSession, user_id: UUID) -> SearchStatus:
    """Return the user's search status, or a default instance if none exist."""
    result = await db.execute(select(SearchStatus).where(SearchStatus.user_id == user_id))
    status = result.scalar_one_or_none()
    if status is not None:
        return status
    # Fallback: return transient object with defaults (not persisted).
    # This covers the unlikely case where the registration-created row is missing.
    return SearchStatus(user_id=user_id, is_active=False)


async def get_search_status_with_device(
    db: AsyncSession, user_id: UUID
) -> tuple[SearchStatus, PairedDevice | None]:
    """Return search status and paired device in a single joined query.

    Consolidates the two separate DB lookups previously done by the
    /search/status endpoint into one round-trip using a LEFT JOIN.
    """
    result = await db.execute(
        select(SearchStatus, PairedDevice)
        .outerjoin(PairedDevice, SearchStatus.user_id == PairedDevice.user_id)
        .where(SearchStatus.user_id == user_id)
    )
    row = result.one_or_none()

    if row is None:
        # No search_status row — still need to check for device
        device_result = await db.execute(
            select(PairedDevice).where(PairedDevice.user_id == user_id)
        )
        return SearchStatus(user_id=user_id, is_active=False), device_result.scalar_one_or_none()

    return row[0], row[1]


async def set_search_active(db: AsyncSession, user_id: UUID, *, active: bool) -> None:
    """Set the is_active flag on the user's search status.

    Uses ORM-style update so that the ``onupdate=func.now()`` clause
    on ``updated_at`` fires automatically.  Creates a row if none exists.
    """
    result = await db.execute(select(SearchStatus).where(SearchStatus.user_id == user_id))
    status = result.scalar_one_or_none()
    if status is None:
        status = SearchStatus(user_id=user_id, is_active=active)
        db.add(status)
    else:
        status.is_active = active
    await db.commit()
