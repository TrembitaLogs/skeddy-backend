"""Admin panel setup module."""

from sqladmin import Admin

from app.admin.auth import AdminAuth
from app.admin.dashboard import DashboardView
from app.admin.views import (
    AcceptFailureAdmin,
    PairedDeviceAdmin,
    RefreshTokenAdmin,
    RideAdmin,
    SearchFiltersAdmin,
    SearchStatusAdmin,
    UserAdmin,
)
from app.config import settings
from app.database import engine


def setup_admin(app):
    """Initialize and configure the admin panel.

    Args:
        app: FastAPI application instance

    Returns:
        Admin: Configured admin instance
    """
    authentication_backend = AdminAuth(secret_key=settings.ADMIN_SECRET_KEY)
    admin = Admin(
        app=app,
        engine=engine,
        authentication_backend=authentication_backend,
        templates_dir="app/admin/templates",
        title="Skeddy Admin",
    )

    # Register Dashboard and all ModelAdmin views
    admin.add_view(DashboardView)
    admin.add_view(UserAdmin)
    admin.add_view(PairedDeviceAdmin)
    admin.add_view(SearchFiltersAdmin)
    admin.add_view(SearchStatusAdmin)
    admin.add_view(RideAdmin)
    admin.add_view(AcceptFailureAdmin)
    admin.add_view(RefreshTokenAdmin)

    return admin
