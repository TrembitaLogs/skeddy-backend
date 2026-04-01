"""Admin view for EmailTemplate model."""

import logging
from typing import Any, ClassVar

from redis.exceptions import RedisError
from sqladmin import ModelView
from starlette.requests import Request

from app.models.email_template import EmailTemplate

logger = logging.getLogger(__name__)


class EmailTemplateAdmin(ModelView, model=EmailTemplate):
    """Admin view for managing email templates.

    Each row is an email type with localized subject/body.
    Body supports {placeholder} syntax for dynamic values (e.g. {code}).
    """

    name = "Email Template"
    name_plural = "Email Templates"
    icon = "fa-solid fa-envelope"

    column_list: ClassVar = [
        EmailTemplate.email_type,
        EmailTemplate.subject_en,
        EmailTemplate.subject_es,
        EmailTemplate.updated_at,
    ]

    column_sortable_list: ClassVar = [EmailTemplate.email_type, EmailTemplate.updated_at]
    column_default_sort: ClassVar = [(EmailTemplate.email_type, False)]

    column_labels: ClassVar = {
        "email_type": "Type",
        "subject_en": "Subject (EN)",
        "body_en": "Body (EN)",
        "subject_es": "Subject (ES)",
        "body_es": "Body (ES)",
    }

    can_create = False
    can_edit = True
    can_delete = False
    can_export = False

    async def after_model_change(
        self, data: dict[str, Any], model: EmailTemplate, is_created: bool, request: Request
    ) -> None:
        """Invalidate email templates cache after edit."""
        from app.redis import redis_client
        from app.services.config_service import invalidate_email_templates

        try:
            await invalidate_email_templates(redis_client)
        except (RedisError, OSError):
            logger.warning("Cache invalidation failed for email templates", exc_info=True)
