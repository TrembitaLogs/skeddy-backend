from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.search_status import SearchStatus


async def get_search_status(db: AsyncSession, user_id: UUID) -> SearchStatus:
    """Return the user's search status, or a default instance if none exist."""
    result = await db.execute(select(SearchStatus).where(SearchStatus.user_id == user_id))
    status = result.scalar_one_or_none()
    if status is not None:
        return status
    # Fallback: return transient object with defaults (not persisted).
    # This covers the unlikely case where the registration-created row is missing.
    return SearchStatus(user_id=user_id)


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
