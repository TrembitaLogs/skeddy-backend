"""Tests for push notification template Pydantic schema validation."""

import pytest

from app.schemas.push_templates import (
    REQUIRED_NOTIFICATION_TYPES,
    PushNotificationTemplatesConfig,
    PushTemplate,
)


class TestPushTemplate:
    """Tests for individual PushTemplate model."""

    def test_valid_template(self):
        t = PushTemplate(title="Hello", body="World")
        assert t.title == "Hello"
        assert t.body == "World"

    def test_empty_title_rejected(self):
        with pytest.raises(ValueError):
            PushTemplate(title="", body="World")

    def test_empty_body_rejected(self):
        with pytest.raises(ValueError):
            PushTemplate(title="Hello", body="")


class TestPushNotificationTemplatesConfig:
    """Tests for the full templates config validation."""

    @pytest.fixture
    def valid_templates(self):
        return {
            ntype: {
                "en": {"title": f"{ntype} EN", "body": f"{ntype} body EN"},
                "es": {"title": f"{ntype} ES", "body": f"{ntype} body ES"},
            }
            for ntype in REQUIRED_NOTIFICATION_TYPES
        }

    def test_valid_config(self, valid_templates):
        config = PushNotificationTemplatesConfig.model_validate(valid_templates)
        assert len(config.root) == len(REQUIRED_NOTIFICATION_TYPES)

    def test_missing_notification_type_rejected(self, valid_templates):
        del valid_templates["RIDE_ACCEPTED"]
        with pytest.raises(ValueError, match="Missing notification types"):
            PushNotificationTemplatesConfig.model_validate(valid_templates)

    def test_missing_language_rejected(self, valid_templates):
        del valid_templates["CREDITS_DEPLETED"]["es"]
        with pytest.raises(ValueError, match="missing languages"):
            PushNotificationTemplatesConfig.model_validate(valid_templates)

    def test_extra_notification_type_allowed(self, valid_templates):
        valid_templates["CUSTOM_TYPE"] = {
            "en": {"title": "Custom", "body": "Custom body"},
            "es": {"title": "Custom ES", "body": "Custom body ES"},
        }
        config = PushNotificationTemplatesConfig.model_validate(valid_templates)
        assert "CUSTOM_TYPE" in config.root

    def test_get_template_exact_language(self, valid_templates):
        config = PushNotificationTemplatesConfig.model_validate(valid_templates)
        template = config.get_template("RIDE_ACCEPTED", "en")
        assert template is not None
        assert template.title == "RIDE_ACCEPTED EN"

    def test_get_template_base_language_fallback(self, valid_templates):
        config = PushNotificationTemplatesConfig.model_validate(valid_templates)
        template = config.get_template("RIDE_ACCEPTED", "en-US")
        assert template is not None
        assert template.title == "RIDE_ACCEPTED EN"

    def test_get_template_english_fallback(self, valid_templates):
        config = PushNotificationTemplatesConfig.model_validate(valid_templates)
        template = config.get_template("RIDE_ACCEPTED", "fr")
        assert template is not None
        assert template.title == "RIDE_ACCEPTED EN"

    def test_get_template_unknown_type_returns_none(self, valid_templates):
        config = PushNotificationTemplatesConfig.model_validate(valid_templates)
        assert config.get_template("NONEXISTENT", "en") is None
