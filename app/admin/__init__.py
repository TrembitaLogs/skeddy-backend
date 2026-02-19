"""Admin panel setup module."""

from typing import ClassVar

from fastapi import FastAPI
from sqladmin import Admin, BaseView, expose
from starlette.responses import HTMLResponse, JSONResponse

from app.admin.auth import AdminAuth
from app.admin.dashboard import DashboardView
from app.admin.views import (
    AcceptFailureAdmin,
    AppConfigAdmin,
    PairedDeviceAdmin,
    RefreshTokenAdmin,
    RideAdmin,
    SearchFiltersAdmin,
    SearchStatusAdmin,
    UserAdmin,
)
from app.config import settings
from app.database import engine

SWAGGER_UI_CDN = "https://cdn.jsdelivr.net/npm/swagger-ui-dist@5"


class ApiDocsView(BaseView):
    """Swagger UI served inside admin panel with auth."""

    name = "API Docs"
    icon = "fa-solid fa-book"

    # Set by setup_admin() to reference the main FastAPI app
    _fastapi_app: ClassVar[FastAPI | None] = None

    @expose("/api-docs", methods=["GET"])
    async def api_docs(self, request):
        return HTMLResponse(
            f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>Skeddy API Docs</title>
                <link rel="stylesheet" href="{SWAGGER_UI_CDN}/swagger-ui.css">
            </head>
            <body>
                <div id="swagger-ui"></div>
                <script src="{SWAGGER_UI_CDN}/swagger-ui-bundle.js"></script>
                <script>
                SwaggerUIBundle({{
                    url: "/admin/api-openapi",
                    dom_id: "#swagger-ui",
                    presets: [
                        SwaggerUIBundle.presets.apis,
                        SwaggerUIBundle.SwaggerUIStandalonePreset
                    ],
                    layout: "BaseLayout"
                }});
                </script>
            </body>
            </html>
            """
        )

    @expose("/api-openapi", methods=["GET"])
    async def api_openapi(self, request):
        assert ApiDocsView._fastapi_app is not None
        return JSONResponse(content=ApiDocsView._fastapi_app.openapi())


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

    # Store app reference for ApiDocsView to generate OpenAPI schema
    ApiDocsView._fastapi_app = app

    # Register Dashboard and all ModelAdmin views
    admin.add_view(DashboardView)
    admin.add_view(ApiDocsView)
    admin.add_view(UserAdmin)
    admin.add_view(PairedDeviceAdmin)
    admin.add_view(SearchFiltersAdmin)
    admin.add_view(SearchStatusAdmin)
    admin.add_view(RideAdmin)
    admin.add_view(AcceptFailureAdmin)
    admin.add_view(RefreshTokenAdmin)
    admin.add_view(AppConfigAdmin)

    return admin
