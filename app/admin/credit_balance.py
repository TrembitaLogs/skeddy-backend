"""Admin view for CreditBalance model with Adjust Balance action."""

import logging
from typing import ClassVar

from sqladmin import ModelView, action, expose
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from starlette.requests import Request
from starlette.responses import RedirectResponse

from app.database import AsyncSessionLocal
from app.models.credit_balance import CreditBalance
from app.models.credit_transaction import TransactionType
from app.models.user import User
from app.redis import redis_client
from app.services.credit_service import add_credits

logger = logging.getLogger(__name__)


class CreditBalanceAdmin(ModelView, model=CreditBalance):
    """Admin view for CreditBalance model.

    Read-only list with custom 'Adjust Balance' action for manual
    credit adjustments by admin. Direct create/edit/delete disabled.
    """

    name = "Credit Balance"
    name_plural = "Credit Balances"
    icon = "fa-solid fa-coins"

    column_list: ClassVar = [
        CreditBalance.id,
        CreditBalance.user_id,
        "user",
        CreditBalance.balance,
        CreditBalance.updated_at,
    ]

    column_searchable_list: ClassVar = [User.email]

    column_sortable_list: ClassVar = [CreditBalance.balance, CreditBalance.updated_at]

    column_default_sort: ClassVar = [(CreditBalance.updated_at, True)]

    column_labels: ClassVar = {
        "user": "User Email",
    }

    can_create = True
    can_edit = False
    can_delete = False

    @action(
        name="adjust_balance",
        label="Adjust Balance",
        add_in_list=True,
        add_in_detail=True,
    )
    async def adjust_balance_action(self, request: Request):
        """Redirect to the adjust balance form for the selected credit balance."""
        pks = request.query_params.get("pks", "").split(",")
        pks = [pk for pk in pks if pk]

        if len(pks) != 1:
            referer = request.headers.get("Referer", "/admin/credit-balance/list")
            return RedirectResponse(url=referer)

        return RedirectResponse(url=f"/admin/credit-balance/adjust/{pks[0]}")

    @expose("/adjust/{pk}", methods=["GET", "POST"])
    async def adjust_balance_form(self, request: Request):
        """Render and process the Adjust Balance form.

        GET: Show form with current balance info.
        POST: Validate input, call add_credits(), redirect to list.
        """
        pk = request.path_params["pk"]
        error = None
        success = None
        form_amount = ""
        form_description = ""

        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(CreditBalance)
                .options(selectinload(CreditBalance.user))
                .where(CreditBalance.id == pk)
            )
            balance_row = result.scalar_one_or_none()

            if balance_row is None:
                return RedirectResponse(url="/admin/credit-balance/list")

            user_email = balance_row.user.email if balance_row.user else "Unknown"
            current_balance = balance_row.balance
            user_id = balance_row.user_id

            if request.method == "POST":
                form_data = await request.form()
                form_amount = str(form_data.get("amount", "")).strip()
                form_description = str(form_data.get("description", "")).strip()

                # Validate amount
                try:
                    amount = int(form_amount)
                except (ValueError, TypeError):
                    error = "Amount must be a valid integer."

                if error is None and amount == 0:
                    error = "Amount cannot be zero."

                if error is None and amount < 0 and current_balance + amount < 0:
                    error = (
                        f"Resulting balance would be {current_balance + amount}. "
                        f"Cannot go below 0. Current balance: {current_balance}."
                    )

                # Validate description
                if error is None and not form_description:
                    error = "Description is required for audit trail."

                if error is None:
                    try:
                        new_balance = await add_credits(
                            user_id=user_id,
                            amount=amount,
                            tx_type=TransactionType.ADMIN_ADJUSTMENT,
                            reference_id=None,
                            db=session,
                            redis=redis_client,
                            description=form_description,
                        )
                        success = f"Balance adjusted by {amount:+d}. New balance: {new_balance}."
                        current_balance = new_balance
                        form_amount = ""
                        form_description = ""
                    except Exception:
                        logger.exception("Failed to adjust balance for user %s", user_id)
                        error = "Failed to adjust balance. Please try again."

        return await self.templates.TemplateResponse(
            request,
            "admin/adjust_balance.html",
            context={
                "user_email": user_email,
                "user_id": str(user_id),
                "current_balance": current_balance,
                "pk": pk,
                "error": error,
                "success": success,
                "form_amount": form_amount,
                "form_description": form_description,
            },
        )
