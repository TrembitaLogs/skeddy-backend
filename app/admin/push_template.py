"""Admin view for PushTemplate model."""

import logging
from typing import Any, ClassVar

from sqladmin import ModelView
from starlette.requests import Request

from app.models.push_template import PushTemplate

logger = logging.getLogger(__name__)
audit_logger = logging.getLogger("audit.admin")


class PushTemplateAdmin(ModelView, model=PushTemplate):
    """Admin view for managing push notification templates.

    Each row is a notification type with localized title/body.
    Body supports {placeholder} syntax for dynamic values.
    """

    name = "Push Template"
    name_plural = "Push Templates"
    icon = "fa-solid fa-bell"

    column_list: ClassVar = [
        PushTemplate.notification_type,
        PushTemplate.title_en,
        PushTemplate.title_es,
        PushTemplate.updated_at,
    ]

    column_sortable_list: ClassVar = [PushTemplate.notification_type, PushTemplate.updated_at]
    column_default_sort: ClassVar = [(PushTemplate.notification_type, False)]

    column_labels: ClassVar = {
        "notification_type": "Type",
        "title_en": "Title (EN)",
        "body_en": "Body (EN)",
        "title_es": "Title (ES)",
        "body_es": "Body (ES)",
    }

    can_create = False
    can_edit = True
    can_delete = False
    can_export = False

    async def after_model_change(
        self, data: dict[str, Any], model: PushTemplate, is_created: bool, request: Request
    ) -> None:
        """Invalidate push templates cache after edit."""
        audit_logger.info(
            "Admin push template updated: %s",
            model.notification_type,
            extra={"action": "updated", "notification_type": model.notification_type},
        )
        from app.redis import redis_client
        from app.services.config_service import invalidate_push_templates

        try:
            await invalidate_push_templates(redis_client)
        except Exception:
            logger.warning("Cache invalidation failed for push templates", exc_info=True)
