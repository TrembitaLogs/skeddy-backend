"""Tests for EmailTemplateAdmin view configuration and cache invalidation."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from redis.exceptions import RedisError

from app.admin.email_template import EmailTemplateAdmin
from app.models.email_template import EmailTemplate


class TestEmailTemplateAdminConfiguration:
    """Tests for EmailTemplateAdmin class attributes and permissions."""

    def test_has_correct_name(self):
        assert EmailTemplateAdmin.name == "Email Template"
        assert EmailTemplateAdmin.name_plural == "Email Templates"

    def test_has_correct_icon(self):
        assert EmailTemplateAdmin.icon == "fa-solid fa-envelope"

    def test_permissions_are_read_and_edit_only(self):
        assert EmailTemplateAdmin.can_create is False
        assert EmailTemplateAdmin.can_edit is True
        assert EmailTemplateAdmin.can_delete is False
        assert EmailTemplateAdmin.can_export is False

    def test_column_list_includes_expected_fields(self):
        col_attrs = [c.key for c in EmailTemplateAdmin.column_list]
        assert "email_type" in col_attrs
        assert "subject_en" in col_attrs
        assert "subject_es" in col_attrs
        assert "updated_at" in col_attrs

    def test_column_labels_defined(self):
        labels = EmailTemplateAdmin.column_labels
        assert "email_type" in labels
        assert "subject_en" in labels
        assert "body_en" in labels

    def test_sortable_columns_defined(self):
        sortable_attrs = [c.key for c in EmailTemplateAdmin.column_sortable_list]
        assert "email_type" in sortable_attrs
        assert "updated_at" in sortable_attrs


class TestEmailTemplateAdminColumnFormatters:
    """Tests for column formatter lambda functions."""

    def test_updated_at_formatter_with_datetime(self):
        from datetime import datetime

        formatter = EmailTemplateAdmin.column_formatters[EmailTemplate.updated_at]
        mock_model = MagicMock()
        mock_model.updated_at = datetime(2025, 3, 15, 10, 30, 45)
        # The lambda takes (model, attribute_name)
        result = formatter(mock_model, "updated_at")
        assert result == "2025-03-15 10:30:45"

    def test_updated_at_formatter_with_none(self):
        formatter = EmailTemplateAdmin.column_formatters[EmailTemplate.updated_at]
        mock_model = MagicMock()
        mock_model.updated_at = None
        result = formatter(mock_model, "updated_at")
        assert result == ""


class TestEmailTemplateAfterModelChange:
    """Tests for after_model_change cache invalidation hook."""

    @pytest.mark.asyncio
    async def test_invalidates_cache_on_edit(self):
        mock_redis = AsyncMock()
        mock_model = MagicMock(spec=EmailTemplate)
        mock_model.email_type = "verification"
        mock_request = MagicMock()

        view = object.__new__(EmailTemplateAdmin)

        with (
            patch("app.admin.email_template.redis_client", mock_redis),
            patch(
                "app.admin.email_template.invalidate_email_templates",
                new_callable=AsyncMock,
            ) as mock_invalidate,
        ):
            await view.after_model_change({}, mock_model, False, mock_request)
            mock_invalidate.assert_called_once_with(mock_redis)

    @pytest.mark.asyncio
    async def test_handles_redis_error_gracefully(self):
        mock_redis = AsyncMock()
        mock_model = MagicMock(spec=EmailTemplate)
        mock_model.email_type = "verification"
        mock_request = MagicMock()

        view = object.__new__(EmailTemplateAdmin)

        with (
            patch("app.admin.email_template.redis_client", mock_redis),
            patch(
                "app.admin.email_template.invalidate_email_templates",
                new_callable=AsyncMock,
                side_effect=RedisError("Connection refused"),
            ),
        ):
            # Should not raise — logs warning instead
            await view.after_model_change({}, mock_model, False, mock_request)

    @pytest.mark.asyncio
    async def test_handles_os_error_gracefully(self):
        mock_redis = AsyncMock()
        mock_model = MagicMock(spec=EmailTemplate)
        mock_model.email_type = "verification"
        mock_request = MagicMock()

        view = object.__new__(EmailTemplateAdmin)

        with (
            patch("app.admin.email_template.redis_client", mock_redis),
            patch(
                "app.admin.email_template.invalidate_email_templates",
                new_callable=AsyncMock,
                side_effect=OSError("Network unreachable"),
            ),
        ):
            # Should not raise — logs warning instead
            await view.after_model_change({}, mock_model, False, mock_request)

    @pytest.mark.asyncio
    async def test_logs_audit_entry(self):
        mock_redis = AsyncMock()
        mock_model = MagicMock(spec=EmailTemplate)
        mock_model.email_type = "password_reset"
        mock_request = MagicMock()

        view = object.__new__(EmailTemplateAdmin)

        with (
            patch("app.admin.email_template.redis_client", mock_redis),
            patch(
                "app.admin.email_template.invalidate_email_templates",
                new_callable=AsyncMock,
            ),
            patch("app.admin.email_template.audit_logger") as mock_audit,
        ):
            await view.after_model_change({}, mock_model, False, mock_request)
            mock_audit.info.assert_called_once()
            call_args = mock_audit.info.call_args
            assert "password_reset" in call_args[0][1]
