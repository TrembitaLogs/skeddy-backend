"""Admin view for PurchaseOrder model (read-only purchase log)."""

from typing import ClassVar

from markupsafe import Markup
from sqladmin import ModelView
from sqladmin.filters import AllUniqueStringValuesFilter, OperationColumnFilter

from app.models.purchase_order import PurchaseOrder, PurchaseStatus
from app.models.user import User

# Badge color map for purchase statuses
_STATUS_BADGE_COLORS: dict[str, str] = {
    PurchaseStatus.PENDING: "warning",
    PurchaseStatus.CONSUMED: "info",
    PurchaseStatus.VERIFIED: "success",
    PurchaseStatus.FAILED: "danger",
    PurchaseStatus.REFUNDED: "secondary",
}


def _format_status(model: object, name: str) -> Markup:
    """Render purchase status as a colored badge."""
    status_val = getattr(model, "status", "")
    color = _STATUS_BADGE_COLORS.get(status_val, "secondary")
    label = status_val.replace("_", " ").title()
    return Markup(f'<span class="badge bg-{color}">{label}</span>')


class PurchaseOrderAdmin(ModelView, model=PurchaseOrder):
    """Read-only admin view for Google Play purchase orders.

    Displays all purchase verification records with filtering by status
    and date, search by user email and google_order_id.
    No create/edit/delete — immutable purchase log.
    """

    name = "Purchase Order"
    name_plural = "Purchase Orders"
    icon = "fa-solid fa-cart-shopping"

    # Read-only: immutable purchase log
    can_create = False
    can_edit = False
    can_delete = False
    can_export = True

    # List display
    column_list: ClassVar = [
        PurchaseOrder.id,
        "user",
        PurchaseOrder.product_id,
        PurchaseOrder.credits_amount,
        PurchaseOrder.status,
        PurchaseOrder.google_order_id,
        PurchaseOrder.created_at,
    ]

    # Colored badges for status
    column_formatters: ClassVar = {
        PurchaseOrder.status: _format_status,  # type: ignore[dict-item]
    }

    # Filters: status dropdown + date operations
    column_filters: ClassVar = [
        AllUniqueStringValuesFilter(PurchaseOrder.status, title="Status"),
        OperationColumnFilter(PurchaseOrder.created_at, title="Created At"),
    ]

    # Search by user email and google_order_id
    column_searchable_list: ClassVar = [User.email, PurchaseOrder.google_order_id]

    # Sorting
    column_sortable_list: ClassVar = [
        PurchaseOrder.created_at,
        PurchaseOrder.credits_amount,
        PurchaseOrder.status,
    ]
    column_default_sort: ClassVar = [(PurchaseOrder.created_at, True)]

    # Labels
    column_labels: ClassVar = {
        "user": "User Email",
    }

    # Details view: all fields including timestamps
    column_details_list: ClassVar = [
        PurchaseOrder.id,
        "user",
        PurchaseOrder.product_id,
        PurchaseOrder.purchase_token,
        PurchaseOrder.credits_amount,
        PurchaseOrder.status,
        PurchaseOrder.google_order_id,
        PurchaseOrder.created_at,
        PurchaseOrder.verified_at,
    ]

    # Detail formatters (same colored output)
    column_formatters_detail: ClassVar = {
        PurchaseOrder.status: _format_status,  # type: ignore[dict-item]
    }

    # Export includes all data fields (raw values, no HTML formatting)
    column_export_list: ClassVar = [
        PurchaseOrder.id,
        PurchaseOrder.user_id,
        PurchaseOrder.product_id,
        PurchaseOrder.purchase_token,
        PurchaseOrder.credits_amount,
        PurchaseOrder.status,
        PurchaseOrder.google_order_id,
        PurchaseOrder.created_at,
        PurchaseOrder.verified_at,
    ]
