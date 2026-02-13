from fastapi import APIRouter, Depends, Request, Response
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.dependencies.auth import get_current_user
from app.middleware.rate_limiter import get_user_key, limiter
from app.models.user import User
from app.schemas.auth import OkResponse
from app.schemas.fcm import FcmRegisterRequest
from app.services.fcm_service import update_user_fcm_token

router = APIRouter(prefix="/fcm", tags=["fcm"])


@router.post("/register", response_model=OkResponse)
@limiter.limit("60/minute", key_func=get_user_key)
async def register_fcm_token(
    request: Request,
    response: Response,
    body: FcmRegisterRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Register or update FCM token for push notifications."""
    await update_user_fcm_token(db, current_user.id, body.fcm_token)
    return OkResponse()
