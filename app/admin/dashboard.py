"""Dashboard view with statistics for admin panel."""

from datetime import datetime, timedelta

from sqladmin import BaseView, expose
from sqlalchemy import and_, cast, func, select
from sqlalchemy.types import Date

from app.database import AsyncSessionLocal
from app.models.credit_balance import CreditBalance
from app.models.paired_device import PairedDevice
from app.models.purchase_order import PurchaseOrder, PurchaseStatus
from app.models.ride import Ride
from app.models.search_status import SearchStatus
from app.models.user import User


class DashboardView(BaseView):
    """Dashboard view showing system statistics."""

    name = "Dashboard"
    icon = "fa-solid fa-chart-line"

    @expose("/dashboard", methods=["GET"])
    async def dashboard(self, request):
        """Render dashboard with statistics.

        Args:
            request: Starlette Request object

        Returns:
            TemplateResponse with dashboard statistics
        """
        async with AsyncSessionLocal() as session:
            # Total users
            users_count = await session.scalar(select(func.count(User.id)))

            # Active paired devices (pinged in last 30 minutes)
            threshold = datetime.utcnow() - timedelta(minutes=30)
            active_devices = await session.scalar(
                select(func.count(PairedDevice.id)).where(PairedDevice.last_ping_at >= threshold)
            )

            # Total paired devices
            total_devices = await session.scalar(select(func.count(PairedDevice.id)))

            # Users with active search
            active_searches = await session.scalar(
                select(func.count(SearchStatus.id)).where(
                    SearchStatus.is_active == True  # noqa: E712
                )
            )

            # Rides in last 24 hours
            day_ago = datetime.utcnow() - timedelta(hours=24)
            rides_24h = await session.scalar(
                select(func.count(Ride.id)).where(Ride.created_at >= day_ago)
            )

            # Rides in last 7 days
            week_ago = datetime.utcnow() - timedelta(days=7)
            rides_7d = await session.scalar(
                select(func.count(Ride.id)).where(Ride.created_at >= week_ago)
            )

            # Total credits in circulation
            total_credits = await session.scalar(
                select(func.coalesce(func.sum(CreditBalance.balance), 0))
            )

            # Purchases today (VERIFIED only)
            today_purchases = await session.execute(
                select(
                    func.count(PurchaseOrder.id),
                    func.coalesce(func.sum(PurchaseOrder.credits_amount), 0),
                ).where(
                    and_(
                        PurchaseOrder.status == PurchaseStatus.VERIFIED.value,
                        cast(PurchaseOrder.created_at, Date) == func.current_date(),
                    )
                )
            )
            purchases_row = today_purchases.one()
            purchases_today_count = purchases_row[0] or 0
            purchases_today_credits = purchases_row[1] or 0

        return await self.templates.TemplateResponse(
            request,
            "admin/dashboard.html",
            {
                "users_count": users_count or 0,
                "active_devices": active_devices or 0,
                "total_devices": total_devices or 0,
                "active_searches": active_searches or 0,
                "rides_24h": rides_24h or 0,
                "rides_7d": rides_7d or 0,
                "total_credits": total_credits,
                "purchases_today_count": purchases_today_count,
                "purchases_today_credits": purchases_today_credits,
            },
        )
