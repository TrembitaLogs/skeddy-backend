"""Authentication backend for admin panel."""

import ipaddress
import logging
import secrets

from sqladmin.authentication import AuthenticationBackend
from starlette.requests import Request

from app.config import settings
from app.services.auth_service import verify_password

logger = logging.getLogger(__name__)


def _parse_allowed_networks() -> list[ipaddress.IPv4Network | ipaddress.IPv6Network] | None:
    """Parse ADMIN_ALLOWED_IPS into a list of networks. Returns None if unrestricted."""
    raw = settings.ADMIN_ALLOWED_IPS.strip()
    if not raw:
        return None
    networks: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = []
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        networks.append(ipaddress.ip_network(entry, strict=False))
    return networks or None


_allowed_networks = _parse_allowed_networks()


def _is_ip_allowed(client_ip: str) -> bool:
    """Check whether client_ip is within the configured allowlist."""
    if _allowed_networks is None:
        return True
    try:
        addr = ipaddress.ip_address(client_ip)
    except ValueError:
        return False
    return any(addr in net for net in _allowed_networks)


class AdminAuth(AuthenticationBackend):
    """Authentication backend for SQLAdmin using session-based auth."""

    async def login(self, request: Request) -> bool:
        """Validate login credentials and create session."""
        form = await request.form()
        username = form.get("username")
        password = form.get("password")
        client_ip = request.client.host if request.client else "unknown"

        if not _is_ip_allowed(client_ip):
            logger.warning("Admin login blocked: IP not in allowlist", extra={"ip": client_ip})
            return False

        if not isinstance(username, str) or not isinstance(password, str):
            logger.warning("Admin login failed: invalid form data", extra={"ip": client_ip})
            return False

        # Validate credentials: ADMIN_PASSWORD stores a bcrypt hash
        if secrets.compare_digest(username, settings.ADMIN_USERNAME) and verify_password(
            password, settings.ADMIN_PASSWORD
        ):
            # Regenerate session to prevent session fixation attacks
            request.session.clear()
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
        client_ip = request.client.host if request.client else "unknown"
        if not _is_ip_allowed(client_ip):
            return False
        return bool(request.session.get("admin_authenticated", False))
