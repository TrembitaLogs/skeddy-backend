import asyncio
import contextlib
import logging
from contextlib import asynccontextmanager

import sentry_sdk
from fastapi import APIRouter, Depends, FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sqlalchemy import text
from starlette.middleware.sessions import SessionMiddleware

from app.admin import setup_admin
from app.config import settings
from app.database import AsyncSessionLocal
from app.middleware.content_type import ContentTypeMiddleware
from app.middleware.csrf import CSRFMiddleware
from app.middleware.error_handler import register_exception_handlers
from app.middleware.language_sync import sync_language_dependency
from app.middleware.logging import setup_logging
from app.middleware.rate_limiter import setup_rate_limiter
from app.middleware.request_id import RequestIdMiddleware
from app.middleware.security_headers import SecurityHeadersMiddleware
from app.redis import redis_client
from app.routers.auth import router as auth_router
from app.routers.credits import router as credits_router
from app.routers.fcm import router as fcm_router
from app.routers.filters import router as filters_router
from app.routers.ping import router as ping_router
from app.routers.profile import router as profile_router
from app.routers.rides import router as rides_router
from app.routers.search import router as search_router
from app.services.fcm_service import initialize_firebase
from app.tasks.balance_reconciliation import run_balance_reconciliation
from app.tasks.cluster_manager import run_cluster_manager
from app.tasks.data_cleanup import cleanup_old_data
from app.tasks.health_check import check_device_health
from app.tasks.low_balance_reminder import run_low_balance_reminder
from app.tasks.purchase_recovery import run_purchase_recovery
from app.tasks.ride_verification import run_verification_fallback
from app.tasks.token_cleanup import cleanup_expired_tokens

setup_logging(debug=settings.DEBUG)

if settings.SENTRY_DSN:
    sentry_sdk.init(
        dsn=settings.SENTRY_DSN,
        integrations=[FastApiIntegration()],
    )

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    try:
        initialize_firebase()
    except (ValueError, FileNotFoundError, KeyError):
        logger.warning("Firebase not initialized — push notifications disabled")

    health_task = asyncio.create_task(check_device_health())
    token_cleanup_task = asyncio.create_task(cleanup_expired_tokens())
    data_cleanup_task = asyncio.create_task(cleanup_old_data())
    low_balance_task = asyncio.create_task(run_low_balance_reminder())
    verification_task = asyncio.create_task(run_verification_fallback())
    purchase_recovery_task = asyncio.create_task(run_purchase_recovery())
    reconciliation_task = asyncio.create_task(run_balance_reconciliation())
    cluster_task = asyncio.create_task(run_cluster_manager())

    yield

    # Shutdown — cancel background tasks gracefully
    for task in (
        health_task,
        token_cleanup_task,
        data_cleanup_task,
        low_balance_task,
        verification_task,
        purchase_recovery_task,
        reconciliation_task,
        cluster_task,
    ):
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


app = FastAPI(
    title="Skeddy API",
    version="1.0.0",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

register_exception_handlers(app)

# Setup admin panel
setup_admin(app)

cors_origins = [o.strip() for o in settings.CORS_ORIGINS.split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=[
        "Authorization",
        "Content-Type",
        "Accept",
        "X-Device-Id",
        "X-Device-Token",
        "X-Language",
        "X-Request-ID",
    ],
)
app.add_middleware(
    CSRFMiddleware, allowed_origins=cors_origins or [f"http://{settings.HOST}:{settings.PORT}"]
)
app.add_middleware(
    SessionMiddleware,
    secret_key=settings.ADMIN_SECRET_KEY,
    https_only=settings.ENVIRONMENT != "dev",
    same_site="lax",
    max_age=3600,
)
app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(ContentTypeMiddleware)
app.add_middleware(RequestIdMiddleware)

setup_rate_limiter(app)


@app.get("/health")
async def health_check(detail_key: str = Query("", alias="detail")):
    postgres_ok = False
    redis_ok = False

    try:
        async with AsyncSessionLocal() as session:
            await session.execute(text("SELECT 1"))
        postgres_ok = True
    except Exception:
        logger.warning("Health check: PostgreSQL unavailable", exc_info=True)

    try:
        await redis_client.ping()  # type: ignore[misc]
        redis_ok = True
    except Exception:
        logger.warning("Health check: Redis unavailable", exc_info=True)

    status = "ok" if postgres_ok and redis_ok else "degraded"

    # Expose component-level details only when a valid detail key is provided
    show_details = bool(
        settings.ADMIN_SECRET_KEY and detail_key and detail_key == settings.ADMIN_SECRET_KEY
    )
    if show_details:
        return {
            "status": status,
            "postgres": "ok" if postgres_ok else "unavailable",
            "redis": "ok" if redis_ok else "unavailable",
        }
    return {"status": status}


v1_router = APIRouter(dependencies=[Depends(sync_language_dependency)])
v1_router.include_router(auth_router)
v1_router.include_router(profile_router)
v1_router.include_router(credits_router)
v1_router.include_router(fcm_router)
v1_router.include_router(filters_router)
v1_router.include_router(ping_router)
v1_router.include_router(rides_router)
v1_router.include_router(search_router)
app.include_router(v1_router, prefix="/api/v1")
