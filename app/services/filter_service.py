from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.search_filters import SearchFilters
from app.schemas.filters import FiltersUpdateRequest


async def get_user_filters(db: AsyncSession, user_id: UUID) -> SearchFilters:
    """Return the user's search filters, or a default instance if none exist."""
    result = await db.execute(select(SearchFilters).where(SearchFilters.user_id == user_id))
    filters = result.scalar_one_or_none()
    if filters is not None:
        return filters
    # Fallback: return transient object with defaults (not persisted).
    # This covers the unlikely case where the registration-created row is missing.
    return SearchFilters(user_id=user_id)


async def update_user_filters(db: AsyncSession, user_id: UUID, data: FiltersUpdateRequest) -> None:
    """Upsert user search filters. Creates a row if missing, otherwise updates."""
    stmt = (
        insert(SearchFilters)
        .values(
            user_id=user_id,
            min_price=data.min_price,
            start_time=data.start_time,
            working_time=data.working_time,
            working_days=data.working_days,
        )
        .on_conflict_do_update(
            index_elements=["user_id"],
            set_={
                "min_price": data.min_price,
                "start_time": data.start_time,
                "working_time": data.working_time,
                "working_days": data.working_days,
            },
        )
    )
    await db.execute(stmt)
    await db.commit()
