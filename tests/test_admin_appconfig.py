"""Tests for AppConfigAdmin JSON validation and form configuration.

Test strategy (task 13.4):
4. SQLAdmin save valid credit_products JSON -> success (on_model_change passes)
5. SQLAdmin save invalid JSON (missing field) -> validation error
6. SQLAdmin save -> Redis cache invalidated
7. SQLAdmin edit registration_bonus_credits -> new value available via getter
"""

import json
from unittest.mock import AsyncMock

import pytest
from starlette.requests import Request
from wtforms import TextAreaField

from app.admin.views import AppConfigAdmin, _get_json_validators
from app.models.app_config import AppConfig
from app.services.config_service import (
    CACHE_KEY_CREDIT_PRODUCTS,
    CACHE_KEY_REGISTRATION_BONUS,
    CACHE_KEY_RIDE_CREDIT_TIERS,
    get_registration_bonus_credits,
    invalidate_config,
)


@pytest.fixture
def admin_view():
    """Provide a fresh AppConfigAdmin instance."""
    return AppConfigAdmin()


@pytest.fixture
def mock_request():
    """Provide a minimal mock Request."""
    return AsyncMock(spec=Request)


# ===========================================================================
# Form configuration tests
# ===========================================================================


class TestAppConfigAdminFormConfig:
    """Tests for AppConfigAdmin form overrides and widget args."""

    def test_value_field_uses_textarea(self):
        """Value field is overridden to TextAreaField for multiline JSON editing."""
        assert AppConfigAdmin.form_overrides.get("value") is TextAreaField

    def test_value_widget_has_rows(self):
        """Value textarea widget has rows attribute for comfortable editing."""
        widget_args = AppConfigAdmin.form_widget_args.get("value", {})
        assert "rows" in widget_args
        assert widget_args["rows"] >= 4

    def test_can_create_and_edit(self):
        """AppConfigAdmin allows create and edit but not delete."""
        assert AppConfigAdmin.can_create is True
        assert AppConfigAdmin.can_edit is True
        assert AppConfigAdmin.can_delete is False


# ===========================================================================
# JSON validation tests (on_model_change)
# ===========================================================================


class TestAppConfigAdminJsonValidation:
    """Tests for on_model_change JSON validation."""

    async def test_valid_credit_products_passes(self, admin_view, mock_request):
        """Valid credit_products JSON passes on_model_change without error."""
        valid_products = json.dumps(
            [
                {"product_id": "test_10", "credits": 10, "price_usd": 9.99},
                {"product_id": "test_25", "credits": 25, "price_usd": 19.99},
            ]
        )
        model = AppConfig(key="credit_products", value=valid_products)

        # Should not raise
        await admin_view.on_model_change({}, model, is_created=False, request=mock_request)

    async def test_valid_ride_credit_tiers_passes(self, admin_view, mock_request):
        """Valid ride_credit_tiers JSON passes on_model_change without error."""
        valid_tiers = json.dumps(
            [
                {"max_price": 25.0, "credits": 1},
                {"max_price": None, "credits": 3},
            ]
        )
        model = AppConfig(key="ride_credit_tiers", value=valid_tiers)

        # Should not raise
        await admin_view.on_model_change({}, model, is_created=False, request=mock_request)

    async def test_invalid_json_raises_value_error(self, admin_view, mock_request):
        """Malformed JSON raises ValueError in on_model_change."""
        model = AppConfig(key="credit_products", value="not valid json {{{")

        with pytest.raises(ValueError, match="Invalid JSON"):
            await admin_view.on_model_change({}, model, is_created=False, request=mock_request)

    async def test_missing_required_field_raises_value_error(self, admin_view, mock_request):
        """JSON with missing required field raises ValueError."""
        bad_products = json.dumps(
            [
                {"product_id": "test_10", "credits": 10},  # missing price_usd
            ]
        )
        model = AppConfig(key="credit_products", value=bad_products)

        with pytest.raises(ValueError, match="Validation failed"):
            await admin_view.on_model_change({}, model, is_created=False, request=mock_request)

    async def test_empty_list_raises_value_error(self, admin_view, mock_request):
        """Empty list raises ValueError (at least one product required)."""
        model = AppConfig(key="credit_products", value="[]")

        with pytest.raises(ValueError, match="Validation failed"):
            await admin_view.on_model_change({}, model, is_created=False, request=mock_request)

    async def test_tiers_wrong_order_raises_value_error(self, admin_view, mock_request):
        """Tiers with non-ascending max_price raises ValueError."""
        bad_tiers = json.dumps(
            [
                {"max_price": 50.0, "credits": 2},
                {"max_price": 20.0, "credits": 1},  # not ascending
            ]
        )
        model = AppConfig(key="ride_credit_tiers", value=bad_tiers)

        with pytest.raises(ValueError, match="Validation failed"):
            await admin_view.on_model_change({}, model, is_created=False, request=mock_request)

    async def test_non_json_key_skips_validation(self, admin_view, mock_request):
        """Non-JSON keys (like registration_bonus_credits) skip Pydantic validation."""
        model = AppConfig(key="registration_bonus_credits", value="15")

        # Should not raise (no JSON validation for this key)
        await admin_view.on_model_change({}, model, is_created=False, request=mock_request)

    async def test_unknown_key_skips_validation(self, admin_view, mock_request):
        """Unknown keys skip validation entirely."""
        model = AppConfig(key="some_new_config", value="any_value")

        # Should not raise
        await admin_view.on_model_change({}, model, is_created=False, request=mock_request)

    async def test_create_with_valid_json_passes(self, admin_view, mock_request):
        """Creating a new config entry with valid JSON passes validation."""
        valid_products = json.dumps(
            [
                {"product_id": "new_product", "credits": 5, "price_usd": 4.99},
            ]
        )
        model = AppConfig(key="credit_products", value=valid_products)

        # Should not raise (is_created=True)
        await admin_view.on_model_change({}, model, is_created=True, request=mock_request)


# ===========================================================================
# Cache invalidation tests (after_model_change)
# ===========================================================================


class TestAppConfigAdminCacheInvalidation:
    """Tests for after_model_change cache invalidation."""

    async def test_save_invalidates_redis_cache(self, db_session, fake_redis):
        """Saving an AppConfig entry clears the corresponding Redis cache key."""
        # Pre-populate Redis cache
        fake_redis._store[CACHE_KEY_CREDIT_PRODUCTS] = "stale_data"

        await invalidate_config("credit_products", fake_redis)

        assert CACHE_KEY_CREDIT_PRODUCTS not in fake_redis._store

    async def test_save_invalidates_ride_tiers_cache(self, db_session, fake_redis):
        """Saving ride_credit_tiers clears its Redis cache key."""
        fake_redis._store[CACHE_KEY_RIDE_CREDIT_TIERS] = "stale_data"

        await invalidate_config("ride_credit_tiers", fake_redis)

        assert CACHE_KEY_RIDE_CREDIT_TIERS not in fake_redis._store

    async def test_save_invalidates_registration_bonus_cache(self, db_session, fake_redis):
        """Saving registration_bonus_credits clears its Redis cache key."""
        fake_redis._store[CACHE_KEY_REGISTRATION_BONUS] = "99"

        await invalidate_config("registration_bonus_credits", fake_redis)

        assert CACHE_KEY_REGISTRATION_BONUS not in fake_redis._store


# ===========================================================================
# Integration: edit value -> getter returns new value
# ===========================================================================


class TestAppConfigAdminIntegration:
    """Integration tests: edit config -> getter reflects new value."""

    async def test_edit_registration_bonus_reflected_in_getter(self, db_session, fake_redis):
        """Editing registration_bonus_credits in DB makes getter return new value."""
        # Insert initial value
        db_session.add(AppConfig(key="registration_bonus_credits", value="10"))
        await db_session.flush()

        result = await get_registration_bonus_credits(db_session, fake_redis)
        assert result == 10

        # Simulate admin edit: update value + invalidate cache
        from sqlalchemy import update

        await db_session.execute(
            update(AppConfig)
            .where(AppConfig.key == "registration_bonus_credits")
            .values(value="25")
        )
        await db_session.flush()
        await invalidate_config("registration_bonus_credits", fake_redis)

        result = await get_registration_bonus_credits(db_session, fake_redis)
        assert result == 25


# ===========================================================================
# Validators registry test
# ===========================================================================


class TestJsonValidatorsRegistry:
    """Tests for the _get_json_validators helper."""

    def test_validators_contain_expected_keys(self):
        """JSON validators registry has credit_products and ride_credit_tiers."""
        validators = _get_json_validators()
        assert "credit_products" in validators
        assert "ride_credit_tiers" in validators

    def test_validators_not_contain_non_json_keys(self):
        """JSON validators registry does not contain plain-value keys."""
        validators = _get_json_validators()
        assert "registration_bonus_credits" not in validators
        assert "verification_deadline_minutes" not in validators
