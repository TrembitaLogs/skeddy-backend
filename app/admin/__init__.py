"""Admin panel setup module."""

from fastapi import Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from sqladmin import Admin, BaseView, expose
from starlette.responses import RedirectResponse as StarletteRedirect

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

SWAGGER_UI_CDN = "https://cdn.jsdelivr.net/npm/swagger-ui-dist@5"


class ApiDocsLink(BaseView):
    """Sidebar link to protected API documentation."""

    name = "API Docs"
    icon = "fa-solid fa-book"

    @expose("/api-docs-redirect", methods=["GET"])
    async def api_docs_redirect(self, request):
        return StarletteRedirect(url="/admin/docs")


def setup_admin(app):
    """Initialize and configure the admin panel.

    Args:
        app: FastAPI application instance

    Returns:
        Admin: Configured admin instance
    """

    # Register protected API docs routes BEFORE Admin() mount,
    # otherwise SQLAdmin's catch-all /admin mount shadows them.
    @app.get("/admin/openapi.json", include_in_schema=False)
    async def admin_openapi(request: Request):
        if not request.session.get("admin_authenticated"):
            return RedirectResponse(url="/admin/login", status_code=302)
        return JSONResponse(content=app.openapi())

    @app.get("/admin/docs", include_in_schema=False)
    async def admin_docs(request: Request):
        if not request.session.get("admin_authenticated"):
            return RedirectResponse(url="/admin/login", status_code=302)
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
                    url: "/admin/openapi.json",
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
    admin.add_view(ApiDocsLink)
    admin.add_view(UserAdmin)
    admin.add_view(PairedDeviceAdmin)
    admin.add_view(SearchFiltersAdmin)
    admin.add_view(SearchStatusAdmin)
    admin.add_view(RideAdmin)
    admin.add_view(AcceptFailureAdmin)
    admin.add_view(RefreshTokenAdmin)

    return admin
