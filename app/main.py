import asyncio
import contextlib
import logging
from contextlib import asynccontextmanager

import sentry_sdk
from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sqlalchemy import text
from starlette.middleware.sessions import SessionMiddleware

from app.admin import setup_admin
from app.config import settings
from app.database import AsyncSessionLocal
from app.middleware.error_handler import register_exception_handlers
from app.middleware.logging import setup_logging
from app.middleware.rate_limiter import setup_rate_limiter
from app.middleware.request_id import RequestIdMiddleware
from app.redis import redis_client
from app.routers.admin_backup import router as admin_backup_router
from app.routers.admin_config import router as admin_config_router
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
    except Exception:
        logger.warning("Firebase not initialized — push notifications disabled")

    health_task = asyncio.create_task(check_device_health())
    token_cleanup_task = asyncio.create_task(cleanup_expired_tokens())
    data_cleanup_task = asyncio.create_task(cleanup_old_data())
    low_balance_task = asyncio.create_task(run_low_balance_reminder())
    verification_task = asyncio.create_task(run_verification_fallback())
    purchase_recovery_task = asyncio.create_task(run_purchase_recovery())
    reconciliation_task = asyncio.create_task(run_balance_reconciliation())

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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(SessionMiddleware, secret_key=settings.ADMIN_SECRET_KEY)
app.add_middleware(RequestIdMiddleware)

setup_rate_limiter(app)

# Admin APIs (use admin session auth, placed after setup_admin)
app.include_router(admin_config_router)
app.include_router(admin_backup_router)


@app.get("/health")
async def health_check():
    postgres_ok = False
    redis_ok = False

    try:
        async with AsyncSessionLocal() as session:
            await session.execute(text("SELECT 1"))
        postgres_ok = True
    except Exception:
        pass

    try:
        await redis_client.ping()
        redis_ok = True
    except Exception:
        pass

    status = "ok" if postgres_ok and redis_ok else "degraded"

    return {
        "status": status,
        "postgres": "ok" if postgres_ok else "unavailable",
        "redis": "ok" if redis_ok else "unavailable",
    }


v1_router = APIRouter()
v1_router.include_router(auth_router)
v1_router.include_router(profile_router)
v1_router.include_router(credits_router)
v1_router.include_router(fcm_router)
v1_router.include_router(filters_router)
v1_router.include_router(ping_router)
v1_router.include_router(rides_router)
v1_router.include_router(search_router)
app.include_router(v1_router, prefix="/api/v1")
