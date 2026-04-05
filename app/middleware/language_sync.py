"""Router-level dependency that syncs user language preference from X-Language header.

Replaces the former side effect in get_current_user() with an explicit,
testable dependency that shares the request's DB session.
"""

import logging
from uuid import UUID

from fastapi import Depends, Request
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.middleware.request_id import user_id_ctx
from app.models.user import User

logger = logging.getLogger(__name__)


async def sync_language_dependency(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Yield control to the route handler, then sync X-Language header to DB."""
    yield
    # Post-response: update language if the authenticated user sent X-Language
    uid = user_id_ctx.get("")
    x_language = request.headers.get("X-Language")
    if not uid or not x_language:
        return
    lang = x_language.split("-")[0].lower()
    if not lang:
        return
    try:
        result = await db.execute(select(User.language).where(User.id == UUID(uid)))
        current = result.scalar_one_or_none()
        if current is not None and current != lang:
            await db.execute(update(User).where(User.id == UUID(uid)).values(language=lang))
            await db.commit()
    except Exception:
        logger.debug("Language sync failed for user %s", uid, exc_info=True)
