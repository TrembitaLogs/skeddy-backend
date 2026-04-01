"""Authentication backend for admin panel."""

import logging
import secrets

from sqladmin.authentication import AuthenticationBackend
from starlette.requests import Request

from app.config import settings
from app.services.auth_service import verify_password

logger = logging.getLogger(__name__)


class AdminAuth(AuthenticationBackend):
    """Authentication backend for SQLAdmin using session-based auth."""

    async def login(self, request: Request) -> bool:
        """Validate login credentials and create session."""
        form = await request.form()
        username = form.get("username")
        password = form.get("password")
        client_ip = request.client.host if request.client else "unknown"

        if not isinstance(username, str) or not isinstance(password, str):
            logger.warning("Admin login failed: invalid form data", extra={"ip": client_ip})
            return False

        # Validate credentials: ADMIN_PASSWORD stores a bcrypt hash
        if secrets.compare_digest(username, settings.ADMIN_USERNAME) and verify_password(
            password, settings.ADMIN_PASSWORD
        ):
            request.session.update({"admin_authenticated": True})
            logger.info("Admin login successful", extra={"username": username, "ip": client_ip})
            return True

        logger.warning(
            "Admin login failed: invalid credentials",
            extra={"username": username, "ip": client_ip},
        )
        return False

    async def logout(self, request: Request) -> bool:
        """Clear the admin session."""
        logger.info("Admin logout")
        request.session.clear()
        return True

    async def authenticate(self, request: Request) -> bool:
        """Check if the current request is authenticated."""
        return bool(request.session.get("admin_authenticated", False))
