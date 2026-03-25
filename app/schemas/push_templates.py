"""Pydantic schemas for push notification template validation.

Used to parse and validate JSON values stored in AppConfig
for the ``push_notification_templates`` key.
"""

from __future__ import annotations

from pydantic import BaseModel, Field, RootModel, model_validator

SUPPORTED_LANGUAGES = {"en", "es"}

# Notification types that must have templates defined
REQUIRED_NOTIFICATION_TYPES = {
    "RIDE_ACCEPTED",
    "SEARCH_OFFLINE",
    "CREDITS_DEPLETED",
    "CREDITS_LOW",
    "RIDE_CREDIT_REFUNDED",
    "BALANCE_ADJUSTED",
    "SEARCH_UPDATE_REQUIRED",
}


class PushTemplate(BaseModel):
    """A single localized push notification template with title and body.

    Body supports placeholders in {field_name} format that are
    substituted with actual values from the FCM data payload.
    """

    title: str = Field(min_length=1)
    body: str = Field(min_length=1)


class PushNotificationTemplatesConfig(RootModel[dict[str, dict[str, PushTemplate]]]):
    """Validated mapping of notification_type -> language -> template.

    Structure: {"RIDE_ACCEPTED": {"en": {"title": "...", "body": "..."}, "es": {...}}}

    Guarantees:
    - All required notification types are present
    - Each type has templates for all supported languages (en, es)
    """

    @model_validator(mode="after")
    def validate_templates(self) -> PushNotificationTemplatesConfig:
        templates = self.root

        missing_types = REQUIRED_NOTIFICATION_TYPES - set(templates.keys())
        if missing_types:
            raise ValueError(f"Missing notification types: {sorted(missing_types)}")

        for ntype, langs in templates.items():
            missing_langs = SUPPORTED_LANGUAGES - set(langs.keys())
            if missing_langs:
                raise ValueError(f"Type '{ntype}' missing languages: {sorted(missing_langs)}")

        return self

    def get_template(self, notification_type: str, language: str) -> PushTemplate | None:
        """Look up a template by notification type and language.

        Falls back to English if the requested language is not found.
        """
        type_templates = self.root.get(notification_type)
        if type_templates is None:
            return None
        # Try exact language, then base language (e.g. "en-US" -> "en"), then English
        template = type_templates.get(language)
        if template is None:
            base_lang = language.split("-")[0]
            template = type_templates.get(base_lang)
        if template is None:
            template = type_templates.get("en")
        return template
