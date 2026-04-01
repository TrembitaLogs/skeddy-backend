"""Authentication backend for admin panel."""

import secrets

from sqladmin.authentication import AuthenticationBackend
from starlette.requests import Request

from app.config import settings
from app.services.auth_service import verify_password


class AdminAuth(AuthenticationBackend):
    """Authentication backend for SQLAdmin using session-based auth."""

    async def login(self, request: Request) -> bool:
        """Validate login credentials and create session.

        Args:
            request: Starlette Request object

        Returns:
            True if credentials are valid, False otherwise
        """
        form = await request.form()
        username = form.get("username")
        password = form.get("password")

        if not isinstance(username, str) or not isinstance(password, str):
            return False

        # Validate credentials: ADMIN_PASSWORD stores a bcrypt hash
        if secrets.compare_digest(username, settings.ADMIN_USERNAME) and verify_password(
            password, settings.ADMIN_PASSWORD
        ):
            request.session.update({"admin_authenticated": True})
            return True

        return False

    async def logout(self, request: Request) -> bool:
        """Clear the admin session.

        Args:
            request: Starlette Request object

        Returns:
            True always
        """
        request.session.clear()
        return True

    async def authenticate(self, request: Request) -> bool:
        """Check if the current request is authenticated.

        Args:
            request: Starlette Request object

        Returns:
            True if authenticated, False otherwise
        """
        return bool(request.session.get("admin_authenticated", False))
