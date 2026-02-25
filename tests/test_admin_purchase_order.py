"""Tests for PurchaseOrderAdmin view (read-only purchase log)."""

import pytest
from markupsafe import Markup

from app.admin.purchase_order import PurchaseOrderAdmin, _format_status
from app.models.purchase_order import PurchaseOrder, PurchaseStatus

# ---------------------------------------------------------------------------
# Test 1: PurchaseOrderAdmin list view displays all expected columns
# ---------------------------------------------------------------------------


class TestPurchaseOrderAdminConfiguration:
    """Tests for PurchaseOrderAdmin ModelView configuration."""

    def test_list_view_displays_expected_columns(self):
        """PurchaseOrderAdmin column_list includes all required fields."""
        column_keys = set()
        for col in PurchaseOrderAdmin.column_list:
            if isinstance(col, str):
                column_keys.add(col)
            else:
                column_keys.add(col.key)

        assert "id" in column_keys
        assert "user" in column_keys
        assert "product_id" in column_keys
        assert "credits_amount" in column_keys
        assert "status" in column_keys
        assert "google_order_id" in column_keys
        assert "created_at" in column_keys

    def test_has_required_attributes(self):
        """PurchaseOrderAdmin has name, name_plural, and icon."""
        assert PurchaseOrderAdmin.name == "Purchase Order"
        assert PurchaseOrderAdmin.name_plural == "Purchase Orders"
        assert PurchaseOrderAdmin.icon == "fa-solid fa-cart-shopping"

    def test_model_is_purchase_order(self):
        """PurchaseOrderAdmin references correct model."""
        assert PurchaseOrderAdmin.model is PurchaseOrder


# ---------------------------------------------------------------------------
# Test 2: Read-only mode — create/edit/delete disabled
# ---------------------------------------------------------------------------


class TestPurchaseOrderAdminReadOnly:
    """Tests that PurchaseOrderAdmin is read-only."""

    def test_can_create_is_false(self):
        """PurchaseOrderAdmin does not allow creation."""
        assert PurchaseOrderAdmin.can_create is False

    def test_can_edit_is_false(self):
        """PurchaseOrderAdmin does not allow editing."""
        assert PurchaseOrderAdmin.can_edit is False

    def test_can_delete_is_false(self):
        """PurchaseOrderAdmin does not allow deletion."""
        assert PurchaseOrderAdmin.can_delete is False


# ---------------------------------------------------------------------------
# Test 3: Filter by status — status filter is configured
# ---------------------------------------------------------------------------


class TestPurchaseOrderAdminFilters:
    """Tests for filter configuration."""

    def test_has_column_filters(self):
        """PurchaseOrderAdmin has column_filters defined."""
        assert hasattr(PurchaseOrderAdmin, "column_filters")
        assert len(PurchaseOrderAdmin.column_filters) >= 2

    def test_status_filter_exists(self):
        """At least one filter targets the 'status' column."""
        filter_titles = [getattr(f, "title", "") for f in PurchaseOrderAdmin.column_filters]
        assert "Status" in filter_titles

    def test_created_at_filter_exists(self):
        """At least one filter targets the 'created_at' column."""
        filter_titles = [getattr(f, "title", "") for f in PurchaseOrderAdmin.column_filters]
        assert "Created At" in filter_titles


# ---------------------------------------------------------------------------
# Test 4: Filter by date range — covered by filter existence test above
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Test 5: Search by user email
# ---------------------------------------------------------------------------


class TestPurchaseOrderAdminSearch:
    """Tests for search configuration."""

    def test_search_by_user_email(self):
        """PurchaseOrderAdmin supports search by User.email."""
        searchable = PurchaseOrderAdmin.column_searchable_list
        searchable_keys = set()
        for col in searchable:
            if isinstance(col, str):
                searchable_keys.add(col)
            else:
                searchable_keys.add(col.key)
        assert "email" in searchable_keys

    def test_search_by_google_order_id(self):
        """PurchaseOrderAdmin supports search by google_order_id."""
        searchable = PurchaseOrderAdmin.column_searchable_list
        searchable_keys = set()
        for col in searchable:
            if isinstance(col, str):
                searchable_keys.add(col)
            else:
                searchable_keys.add(col.key)
        assert "google_order_id" in searchable_keys


# ---------------------------------------------------------------------------
# Test 6: Status badge formatting — correct colors per status
# ---------------------------------------------------------------------------


class TestStatusFormatting:
    """Tests for status column formatter (colored badges)."""

    @pytest.mark.parametrize(
        "status,expected_color",
        [
            (PurchaseStatus.PENDING, "warning"),
            (PurchaseStatus.CONSUMED, "info"),
            (PurchaseStatus.VERIFIED, "success"),
            (PurchaseStatus.FAILED, "danger"),
            (PurchaseStatus.REFUNDED, "secondary"),
        ],
    )
    def test_status_badge_colors(self, status, expected_color):
        """Each purchase status renders with the correct badge color."""
        order = PurchaseOrder(
            status=status.value,
            product_id="credits_10",
            credits_amount=10,
            purchase_token="test_token",
        )
        result = _format_status(order, "status")
        assert isinstance(result, Markup)
        assert f"bg-{expected_color}" in str(result)
        assert "badge" in str(result)

    def test_status_badge_has_readable_label(self):
        """Status badge shows human-readable label (title case)."""
        order = PurchaseOrder(
            status=PurchaseStatus.VERIFIED.value,
            product_id="credits_10",
            credits_amount=10,
            purchase_token="test_token",
        )
        result = _format_status(order, "status")
        assert "Verified" in str(result)


# ---------------------------------------------------------------------------
# Test 7: Default sorting — newest first
# ---------------------------------------------------------------------------


class TestPurchaseOrderAdminSorting:
    """Tests for sorting configuration."""

    def test_default_sort_by_created_at_desc(self):
        """PurchaseOrderAdmin sorts by created_at descending by default."""
        default_sort = PurchaseOrderAdmin.column_default_sort
        assert len(default_sort) == 1
        col, desc = default_sort[0]
        assert col.key == "created_at"
        assert desc is True

    def test_sortable_columns(self):
        """created_at, credits_amount, status are sortable."""
        sortable_keys = set()
        for col in PurchaseOrderAdmin.column_sortable_list:
            if isinstance(col, str):
                sortable_keys.add(col)
            else:
                sortable_keys.add(col.key)
        assert "created_at" in sortable_keys
        assert "credits_amount" in sortable_keys
        assert "status" in sortable_keys


# ---------------------------------------------------------------------------
# Test 8: Details view includes all relevant fields
# ---------------------------------------------------------------------------


class TestPurchaseOrderAdminDetails:
    """Tests for details view configuration."""

    def test_details_includes_all_fields(self):
        """Details view includes all relevant purchase order fields."""
        detail_keys = set()
        for col in PurchaseOrderAdmin.column_details_list:
            if isinstance(col, str):
                detail_keys.add(col)
            else:
                detail_keys.add(col.key)

        assert "id" in detail_keys
        assert "user" in detail_keys
        assert "product_id" in detail_keys
        assert "purchase_token" in detail_keys
        assert "credits_amount" in detail_keys
        assert "status" in detail_keys
        assert "google_order_id" in detail_keys
        assert "created_at" in detail_keys
        assert "verified_at" in detail_keys

    def test_detail_formatters_include_status(self):
        """Detail view uses status formatter."""
        assert PurchaseOrderAdmin.column_formatters_detail is not None
        detail_fmt_keys = set()
        for col in PurchaseOrderAdmin.column_formatters_detail:
            if isinstance(col, str):
                detail_fmt_keys.add(col)
            else:
                detail_fmt_keys.add(col.key)
        assert "status" in detail_fmt_keys


# ---------------------------------------------------------------------------
# Test 9: Export functionality is enabled
# ---------------------------------------------------------------------------


class TestPurchaseOrderAdminExport:
    """Tests for export configuration."""

    def test_can_export_is_true(self):
        """PurchaseOrderAdmin allows data export."""
        assert PurchaseOrderAdmin.can_export is True

    def test_export_columns_include_all_data_fields(self):
        """Export list includes all essential data columns."""
        export_keys = set()
        for col in PurchaseOrderAdmin.column_export_list:
            if isinstance(col, str):
                export_keys.add(col)
            else:
                export_keys.add(col.key)

        expected = {
            "id",
            "user_id",
            "product_id",
            "purchase_token",
            "credits_amount",
            "status",
            "google_order_id",
            "created_at",
            "verified_at",
        }
        assert expected.issubset(export_keys)


# ---------------------------------------------------------------------------
# Test 10: Integration — admin panel registers the view
# ---------------------------------------------------------------------------


class TestPurchaseOrderAdminRegistration:
    """Tests for PurchaseOrderAdmin registration in admin panel."""

    @pytest.mark.asyncio
    async def test_purchase_order_list_view_accessible(self, app_client):
        """Purchase Order list view URL responds (requires auth redirect)."""
        resp = await app_client.get("/admin/purchase-order/list")
        assert resp.status_code in (200, 302, 303)


# ---------------------------------------------------------------------------
# Test 11: Integration — authenticated admin can access list view
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_admin_can_access_purchase_order_list(admin_client):
    """Authenticated admin can load the purchase order list page."""
    resp = await admin_client.client.get("/admin/purchase-order/list")
    assert resp.status_code == 200
    assert "Purchase Order" in resp.text
