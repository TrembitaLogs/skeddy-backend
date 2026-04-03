"""Tests for CreditTransactionAdmin view (read-only audit log)."""

import pytest
from markupsafe import Markup

from app.admin.credit_transaction import (
    CreditTransactionAdmin,
    _format_amount,
    _format_type,
)
from app.models.credit_transaction import CreditTransaction, TransactionType

# ---------------------------------------------------------------------------
# Test 1: CreditTransactionAdmin list view displays all expected columns
# ---------------------------------------------------------------------------


class TestCreditTransactionAdminConfiguration:
    """Tests for CreditTransactionAdmin ModelView configuration."""

    def test_list_view_displays_expected_columns(self):
        """CreditTransactionAdmin column_list includes all required fields."""
        column_keys = set()
        for col in CreditTransactionAdmin.column_list:
            if isinstance(col, str):
                column_keys.add(col)
            else:
                column_keys.add(col.key)

        assert "user" in column_keys
        assert "type" in column_keys
        assert "amount" in column_keys
        assert "balance_after" in column_keys
        assert "reference_id" in column_keys
        assert "created_at" in column_keys

    def test_has_required_attributes(self):
        """CreditTransactionAdmin has name, name_plural, and icon."""
        assert CreditTransactionAdmin.name == "Credit Transaction"
        assert CreditTransactionAdmin.name_plural == "Credit Transactions"
        assert CreditTransactionAdmin.icon == "fa-solid fa-receipt"

    def test_model_is_credit_transaction(self):
        """CreditTransactionAdmin references correct model."""
        assert CreditTransactionAdmin.model is CreditTransaction


# ---------------------------------------------------------------------------
# Test 2: Read-only mode — create/edit/delete disabled
# ---------------------------------------------------------------------------


class TestCreditTransactionAdminReadOnly:
    """Tests that CreditTransactionAdmin is read-only."""

    def test_can_create_is_false(self):
        """CreditTransactionAdmin does not allow creation."""
        assert CreditTransactionAdmin.can_create is False

    def test_can_edit_is_false(self):
        """CreditTransactionAdmin does not allow editing."""
        assert CreditTransactionAdmin.can_edit is False

    def test_can_delete_is_false(self):
        """CreditTransactionAdmin does not allow deletion."""
        assert CreditTransactionAdmin.can_delete is False


# ---------------------------------------------------------------------------
# Test 3: Filter by type — type filter is configured
# ---------------------------------------------------------------------------


class TestCreditTransactionAdminFilters:
    """Tests for filter configuration."""

    def test_has_column_filters(self):
        """CreditTransactionAdmin has column_filters defined."""
        assert hasattr(CreditTransactionAdmin, "column_filters")
        assert len(CreditTransactionAdmin.column_filters) >= 2

    def test_type_filter_exists(self):
        """At least one filter targets the 'type' column."""
        filter_titles = [getattr(f, "title", "") for f in CreditTransactionAdmin.column_filters]
        assert "Type" in filter_titles

    def test_created_at_filter_exists(self):
        """At least one filter targets the 'created_at' column."""
        filter_titles = [getattr(f, "title", "") for f in CreditTransactionAdmin.column_filters]
        assert "Created At" in filter_titles


# ---------------------------------------------------------------------------
# Test 4: Filter by date range — covered by filter existence test above
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Test 5: Search by user email
# ---------------------------------------------------------------------------


class TestCreditTransactionAdminSearch:
    """Tests for search configuration."""

    def test_search_by_user_email(self):
        """CreditTransactionAdmin supports search by User.email."""
        searchable = CreditTransactionAdmin.column_searchable_list
        searchable_keys = set()
        for col in searchable:
            if isinstance(col, str):
                searchable_keys.add(col)
            else:
                searchable_keys.add(col.key)
        assert "email" in searchable_keys


# ---------------------------------------------------------------------------
# Test 6: Amount formatting — positive green, negative red
# ---------------------------------------------------------------------------


class TestAmountFormatting:
    """Tests for amount column formatter."""

    def test_positive_amount_green(self):
        """Positive amount is rendered in green with + prefix."""
        tx = CreditTransaction(amount=50, type="PURCHASE", balance_after=100)
        result = _format_amount(tx, "amount")
        assert isinstance(result, Markup)
        assert "green" in str(result)
        assert "+50" in str(result)

    def test_negative_amount_red(self):
        """Negative amount is rendered in red."""
        tx = CreditTransaction(amount=-3, type="RIDE_CHARGE", balance_after=7)
        result = _format_amount(tx, "amount")
        assert isinstance(result, Markup)
        assert "red" in str(result)
        assert "-3" in str(result)

    def test_zero_amount(self):
        """Zero amount is rendered without color."""
        tx = CreditTransaction(amount=0, type="ADMIN_ADJUSTMENT", balance_after=10)
        result = _format_amount(tx, "amount")
        assert isinstance(result, Markup)
        assert "green" not in str(result)
        assert "red" not in str(result)
        assert "0" in str(result)


# ---------------------------------------------------------------------------
# Test 7: Type formatting — badge rendering
# ---------------------------------------------------------------------------


class TestTypeFormatting:
    """Tests for type column formatter (colored badges)."""

    @pytest.mark.parametrize(
        "tx_type,expected_color",
        [
            (TransactionType.REGISTRATION_BONUS, "info"),
            (TransactionType.PURCHASE, "success"),
            (TransactionType.RIDE_CHARGE, "warning"),
            (TransactionType.RIDE_REFUND, "primary"),
            (TransactionType.ADMIN_ADJUSTMENT, "secondary"),
        ],
    )
    def test_type_badge_colors(self, tx_type, expected_color):
        """Each transaction type renders with the correct badge color."""
        tx = CreditTransaction(type=tx_type, amount=0, balance_after=0)
        result = _format_type(tx, "type")
        assert isinstance(result, Markup)
        assert f"bg-{expected_color}" in str(result)
        assert "badge" in str(result)

    def test_type_badge_has_readable_label(self):
        """Type badge shows human-readable label (title case, no underscores)."""
        tx = CreditTransaction(type=TransactionType.REGISTRATION_BONUS, amount=0, balance_after=0)
        result = _format_type(tx, "type")
        assert "Registration Bonus" in str(result)
        assert "_" not in str(result)


# ---------------------------------------------------------------------------
# Test 8: Default sorting — newest first
# ---------------------------------------------------------------------------


class TestCreditTransactionAdminSorting:
    """Tests for sorting configuration."""

    def test_default_sort_by_created_at_desc(self):
        """CreditTransactionAdmin sorts by created_at descending by default."""
        default_sort = CreditTransactionAdmin.column_default_sort
        assert len(default_sort) == 1
        col, desc = default_sort[0]
        assert col.key == "created_at"
        assert desc is True

    def test_sortable_columns(self):
        """created_at, amount, type are sortable."""
        sortable_keys = set()
        for col in CreditTransactionAdmin.column_sortable_list:
            if isinstance(col, str):
                sortable_keys.add(col)
            else:
                sortable_keys.add(col.key)
        assert "created_at" in sortable_keys
        assert "amount" in sortable_keys
        assert "type" in sortable_keys


# ---------------------------------------------------------------------------
# Test 9: Export functionality is enabled
# ---------------------------------------------------------------------------


class TestCreditTransactionAdminExport:
    """Tests for export configuration."""

    def test_can_export_is_true(self):
        """CreditTransactionAdmin allows data export."""
        assert CreditTransactionAdmin.can_export is True

    def test_export_columns_include_all_data_fields(self):
        """Export list includes all essential data columns."""
        export_keys = set()
        for col in CreditTransactionAdmin.column_export_list:
            if isinstance(col, str):
                export_keys.add(col)
            else:
                export_keys.add(col.key)

        expected = {
            "id",
            "user_id",
            "type",
            "amount",
            "balance_after",
            "reference_id",
            "description",
            "created_at",
        }
        assert expected.issubset(export_keys)


# ---------------------------------------------------------------------------
# Test 10: Details view includes all fields + description
# ---------------------------------------------------------------------------


class TestCreditTransactionAdminDetails:
    """Tests for details view configuration."""

    def test_details_includes_description(self):
        """Details view includes the description field."""
        detail_keys = set()
        for col in CreditTransactionAdmin.column_details_list:
            if isinstance(col, str):
                detail_keys.add(col)
            else:
                detail_keys.add(col.key)

        assert "description" in detail_keys
        assert "user" in detail_keys
        assert "type" in detail_keys
        assert "amount" in detail_keys
        assert "balance_after" in detail_keys
        assert "reference_id" in detail_keys
        assert "created_at" in detail_keys

    def test_detail_formatters_match_list_formatters(self):
        """Detail view uses same formatters as list view."""
        assert CreditTransactionAdmin.column_formatters_detail is not None
        detail_fmt_keys = set()
        for col in CreditTransactionAdmin.column_formatters_detail:
            if isinstance(col, str):
                detail_fmt_keys.add(col)
            else:
                detail_fmt_keys.add(col.key)
        assert "type" in detail_fmt_keys
        assert "amount" in detail_fmt_keys


# ---------------------------------------------------------------------------
# Test 11: Integration — admin panel registers the view
# ---------------------------------------------------------------------------


class TestCreditTransactionAdminRegistration:
    """Tests for CreditTransactionAdmin registration in admin panel."""

    @pytest.mark.asyncio
    async def test_credit_transaction_list_view_accessible(self, app_client):
        """Credit Transaction list view URL responds (requires auth redirect)."""
        resp = await app_client.get("/admin/credit-transaction/list")
        assert resp.status_code in (200, 302, 303)


# ---------------------------------------------------------------------------
# Test 12: Integration — authenticated admin can access list view
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_admin_can_access_transaction_list(admin_client):
    """Authenticated admin can load the credit transaction list page."""
    resp = await admin_client.client.get("/admin/credit-transaction/list")
    assert resp.status_code == 200
    assert "Credit Transaction" in resp.text
