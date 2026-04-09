"""Tests for PushTemplateAdmin view configuration and cache invalidation."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.admin.push_template import PushTemplateAdmin
from app.models.push_template import PushTemplate


class TestPushTemplateAdminConfiguration:
    """Tests for PushTemplateAdmin class attributes and permissions."""

    def test_has_correct_name(self):
        assert PushTemplateAdmin.name == "Push Template"
        assert PushTemplateAdmin.name_plural == "Push Templates"

    def test_has_correct_icon(self):
        assert PushTemplateAdmin.icon == "fa-solid fa-bell"

    def test_permissions_are_read_and_edit_only(self):
        assert PushTemplateAdmin.can_create is False
        assert PushTemplateAdmin.can_edit is True
        assert PushTemplateAdmin.can_delete is False
        assert PushTemplateAdmin.can_export is False

    def test_column_list_includes_expected_fields(self):
        col_attrs = [c.key for c in PushTemplateAdmin.column_list]
        assert "notification_type" in col_attrs
        assert "title_en" in col_attrs
        assert "title_es" in col_attrs
        assert "updated_at" in col_attrs

    def test_column_labels_defined(self):
        labels = PushTemplateAdmin.column_labels
        assert "notification_type" in labels
        assert "title_en" in labels
        assert "body_en" in labels

    def test_sortable_columns_defined(self):
        sortable_attrs = [c.key for c in PushTemplateAdmin.column_sortable_list]
        assert "notification_type" in sortable_attrs
        assert "updated_at" in sortable_attrs


class TestPushTemplateAdminColumnFormatters:
    """Tests for column formatter lambda functions."""

    def test_updated_at_formatter_with_datetime(self):
        from datetime import datetime

        formatter = PushTemplateAdmin.column_formatters[PushTemplate.updated_at]
        mock_model = MagicMock()
        mock_model.updated_at = datetime(2025, 6, 20, 14, 0, 0)
        result = formatter(mock_model, "updated_at")
        assert result == "2025-06-20 14:00:00"

    def test_updated_at_formatter_with_none(self):
        formatter = PushTemplateAdmin.column_formatters[PushTemplate.updated_at]
        mock_model = MagicMock()
        mock_model.updated_at = None
        result = formatter(mock_model, "updated_at")
        assert result == ""


class TestPushTemplateAfterModelChange:
    """Tests for after_model_change cache invalidation hook."""

    @pytest.mark.asyncio
    async def test_invalidates_cache_on_edit(self):
        mock_redis = AsyncMock()
        mock_model = MagicMock(spec=PushTemplate)
        mock_model.notification_type = "ride_match"
        mock_request = MagicMock()

        view = object.__new__(PushTemplateAdmin)

        with (
            patch("app.redis.redis_client", mock_redis),
            patch(
                "app.services.config_service.invalidate_push_templates",
                new_callable=AsyncMock,
            ) as mock_invalidate,
        ):
            await view.after_model_change({}, mock_model, False, mock_request)
            mock_invalidate.assert_called_once_with(mock_redis)

    @pytest.mark.asyncio
    async def test_handles_exception_gracefully(self):
        mock_redis = AsyncMock()
        mock_model = MagicMock(spec=PushTemplate)
        mock_model.notification_type = "ride_match"
        mock_request = MagicMock()

        view = object.__new__(PushTemplateAdmin)

        with (
            patch("app.redis.redis_client", mock_redis),
            patch(
                "app.services.config_service.invalidate_push_templates",
                new_callable=AsyncMock,
                side_effect=RuntimeError("Unexpected error"),
            ),
        ):
            # Should not raise — logs warning instead
            await view.after_model_change({}, mock_model, False, mock_request)

    @pytest.mark.asyncio
    async def test_logs_audit_entry(self):
        mock_redis = AsyncMock()
        mock_model = MagicMock(spec=PushTemplate)
        mock_model.notification_type = "low_balance"
        mock_request = MagicMock()

        view = object.__new__(PushTemplateAdmin)

        with (
            patch("app.redis.redis_client", mock_redis),
            patch(
                "app.services.config_service.invalidate_push_templates",
                new_callable=AsyncMock,
            ),
            patch("app.admin.push_template.audit_logger") as mock_audit,
        ):
            await view.after_model_change({}, mock_model, False, mock_request)
            mock_audit.info.assert_called_once()
            call_args = mock_audit.info.call_args
            assert "low_balance" in call_args[0][1]
