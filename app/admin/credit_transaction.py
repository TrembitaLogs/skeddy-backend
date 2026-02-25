"""Admin view for CreditTransaction model (read-only audit log)."""

from typing import ClassVar

from markupsafe import Markup
from sqladmin import ModelView
from sqladmin.filters import AllUniqueStringValuesFilter, OperationColumnFilter

from app.models.credit_transaction import CreditTransaction, TransactionType
from app.models.user import User

# Badge color map for transaction types
_TYPE_BADGE_COLORS: dict[str, str] = {
    TransactionType.REGISTRATION_BONUS: "info",
    TransactionType.PURCHASE: "success",
    TransactionType.RIDE_CHARGE: "warning",
    TransactionType.RIDE_REFUND: "primary",
    TransactionType.ADMIN_ADJUSTMENT: "secondary",
}


def _format_type(model: object, name: str) -> Markup:
    """Render transaction type as a colored badge."""
    type_val = getattr(model, "type", "")
    color = _TYPE_BADGE_COLORS.get(type_val, "secondary")
    label = type_val.replace("_", " ").title()
    return Markup(f'<span class="badge bg-{color}">{label}</span>')


def _format_amount(model: object, name: str) -> Markup:
    """Render amount with sign and color (green for positive, red for negative)."""
    amount = getattr(model, "amount", 0)
    if amount > 0:
        return Markup(f'<span style="color: green; font-weight: 600;">+{amount}</span>')
    elif amount < 0:
        return Markup(f'<span style="color: red; font-weight: 600;">{amount}</span>')
    return Markup(f"<span>{amount}</span>")


class CreditTransactionAdmin(ModelView, model=CreditTransaction):
    """Read-only admin view for credit transactions audit log.

    Displays all credit balance changes with filtering by type and date,
    search by user email. No create/edit/delete — immutable audit trail.
    """

    name = "Credit Transaction"
    name_plural = "Credit Transactions"
    icon = "fa-solid fa-receipt"

    # Read-only: immutable audit log
    can_create = False
    can_edit = False
    can_delete = False
    can_export = True

    # List display
    column_list: ClassVar = [
        CreditTransaction.id,
        "user",
        CreditTransaction.type,
        CreditTransaction.amount,
        CreditTransaction.balance_after,
        CreditTransaction.reference_id,
        CreditTransaction.created_at,
    ]

    # Colored badges for type, colored +/- for amount
    column_formatters: ClassVar = {
        CreditTransaction.type: _format_type,  # type: ignore[dict-item]
        CreditTransaction.amount: _format_amount,  # type: ignore[dict-item]
    }

    # Filters: type dropdown + date operations
    column_filters: ClassVar = [
        AllUniqueStringValuesFilter(CreditTransaction.type, title="Type"),
        OperationColumnFilter(CreditTransaction.created_at, title="Created At"),
    ]

    # Search by user email via relationship
    column_searchable_list: ClassVar = [User.email]

    # Sorting
    column_sortable_list: ClassVar = [
        CreditTransaction.created_at,
        CreditTransaction.amount,
        CreditTransaction.type,
    ]
    column_default_sort: ClassVar = [(CreditTransaction.created_at, True)]

    # Labels
    column_labels: ClassVar = {
        "user": "User Email",
    }

    # Details view: show all fields including description
    column_details_list: ClassVar = [
        CreditTransaction.id,
        "user",
        CreditTransaction.type,
        CreditTransaction.amount,
        CreditTransaction.balance_after,
        CreditTransaction.reference_id,
        CreditTransaction.description,
        CreditTransaction.created_at,
    ]

    # Detail formatters (same colored output)
    column_formatters_detail: ClassVar = {
        CreditTransaction.type: _format_type,  # type: ignore[dict-item]
        CreditTransaction.amount: _format_amount,  # type: ignore[dict-item]
    }

    # Export includes all data fields (raw values, no HTML formatting)
    column_export_list: ClassVar = [
        CreditTransaction.id,
        CreditTransaction.user_id,
        CreditTransaction.type,
        CreditTransaction.amount,
        CreditTransaction.balance_after,
        CreditTransaction.reference_id,
        CreditTransaction.description,
        CreditTransaction.created_at,
    ]
