"""Integration tests for Admin ModelAdmin views."""

import pytest

from app.admin.views import (
    AcceptFailureAdmin,
    PairedDeviceAdmin,
    RefreshTokenAdmin,
    RideAdmin,
    SearchFiltersAdmin,
    SearchStatusAdmin,
    UserAdmin,
)


class TestUserAdmin:
    """Tests for UserAdmin ModelView configuration."""

    def test_sensitive_columns_not_in_column_list(self):
        """Test that password_hash and fcm_token are not in column_list."""
        # column_list should NOT contain sensitive fields
        sensitive_fields = {"password_hash", "fcm_token"}
        column_fields = {field.key for field in UserAdmin.column_list}

        assert not sensitive_fields.intersection(column_fields)

    def test_sensitive_columns_excluded_from_details(self):
        """Test that password_hash and fcm_token are excluded from details view."""
        sensitive_fields = {"password_hash", "fcm_token"}
        excluded_fields = {field.key for field in UserAdmin.column_details_exclude_list}

        assert sensitive_fields.issubset(excluded_fields)

    def test_can_create_is_true(self):
        """Test that UserAdmin has can_create=True (admin can create users)."""
        assert UserAdmin.can_create is True

    def test_form_excludes_sensitive_and_relationship_columns(self):
        """Test that form_excluded_columns prevents editing sensitive fields and relationships."""
        excluded = {field.key for field in UserAdmin.form_excluded_columns}

        # Sensitive fields
        assert "password_hash" in excluded
        assert "fcm_token" in excluded

        # Relationships (complex objects)
        assert "refresh_tokens" in excluded
        assert "paired_device" in excluded
        assert "search_filters" in excluded
        assert "search_status" in excluded
        assert "rides" in excluded
        assert "accept_failures" in excluded


class TestPairedDeviceAdmin:
    """Tests for PairedDeviceAdmin ModelView configuration."""

    def test_sensitive_columns_not_in_column_list(self):
        """Test that device_token_hash is not in column_list."""
        sensitive_fields = {"device_token_hash"}
        column_fields = {field.key for field in PairedDeviceAdmin.column_list}

        assert not sensitive_fields.intersection(column_fields)

    def test_sensitive_columns_excluded_from_details(self):
        """Test that device_token_hash is excluded from details view."""
        sensitive_fields = {"device_token_hash"}
        excluded_fields = {field.key for field in PairedDeviceAdmin.column_details_exclude_list}

        assert sensitive_fields.issubset(excluded_fields)

    def test_can_create_is_true(self):
        """Test that PairedDeviceAdmin has can_create=True."""
        assert PairedDeviceAdmin.can_create is True

    def test_form_excludes_sensitive_columns(self):
        """Test that form_excluded_columns prevents editing device_token_hash."""
        excluded = {field.key for field in PairedDeviceAdmin.form_excluded_columns}

        assert "device_token_hash" in excluded


class TestRefreshTokenAdmin:
    """Tests for RefreshTokenAdmin ModelView configuration."""

    def test_sensitive_columns_not_in_column_list(self):
        """Test that token_hash is not in column_list."""
        sensitive_fields = {"token_hash"}
        column_fields = {field.key for field in RefreshTokenAdmin.column_list}

        assert not sensitive_fields.intersection(column_fields)

    def test_sensitive_columns_excluded_from_details(self):
        """Test that token_hash is excluded from details view."""
        sensitive_fields = {"token_hash"}
        excluded_fields = {field.key for field in RefreshTokenAdmin.column_details_exclude_list}

        assert sensitive_fields.issubset(excluded_fields)

    def test_can_create_is_false(self):
        """Test that RefreshTokenAdmin has can_create=False."""
        assert RefreshTokenAdmin.can_create is False

    def test_can_edit_is_false(self):
        """Test that RefreshTokenAdmin has can_edit=False."""
        assert RefreshTokenAdmin.can_edit is False

    def test_form_excludes_sensitive_columns(self):
        """Test that form_excluded_columns prevents editing token_hash."""
        excluded = {field.key for field in RefreshTokenAdmin.form_excluded_columns}

        assert "token_hash" in excluded


class TestRideAdmin:
    """Tests for RideAdmin ModelView configuration."""

    def test_can_create_is_true(self):
        """Test that RideAdmin allows creation via admin."""
        assert RideAdmin.can_create is True


class TestAcceptFailureAdmin:
    """Tests for AcceptFailureAdmin ModelView configuration."""

    def test_can_create_is_false(self):
        """Test that AcceptFailureAdmin has can_create=False."""
        assert AcceptFailureAdmin.can_create is False


class TestModelAdminViewsRegistration:
    """Tests for ModelAdmin views registration."""

    @pytest.mark.parametrize(
        "view_class",
        [
            UserAdmin,
            PairedDeviceAdmin,
            SearchFiltersAdmin,
            SearchStatusAdmin,
            RideAdmin,
            AcceptFailureAdmin,
            RefreshTokenAdmin,
        ],
    )
    def test_view_has_required_attributes(self, view_class):
        """Test that all ModelAdmin views have required configuration attributes."""
        assert hasattr(view_class, "name")
        assert hasattr(view_class, "name_plural")
        assert hasattr(view_class, "icon")
        assert hasattr(view_class, "column_list")
        assert hasattr(view_class, "can_create")
        assert hasattr(view_class, "can_delete")

    def test_all_views_are_unique(self):
        """Test that all views have unique names."""
        views = [
            UserAdmin,
            PairedDeviceAdmin,
            SearchFiltersAdmin,
            SearchStatusAdmin,
            RideAdmin,
            AcceptFailureAdmin,
            RefreshTokenAdmin,
        ]

        names = [view.name for view in views]
        assert len(names) == len(set(names)), "View names must be unique"
