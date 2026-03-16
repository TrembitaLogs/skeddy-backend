"""ModelAdmin views for SQLAdmin panel."""

import json
import logging
from typing import Any, ClassVar

from sqladmin import ModelView
from starlette.requests import Request
from wtforms import Form, PasswordField, TextAreaField

from app.models.accept_failure import AcceptFailure
from app.models.app_config import AppConfig
from app.models.paired_device import PairedDevice
from app.models.refresh_token import RefreshToken
from app.models.ride import Ride
from app.models.search_filters import SearchFilters
from app.models.search_status import SearchStatus
from app.models.user import User

logger = logging.getLogger(__name__)

# AppConfig keys that store JSON and require Pydantic validation on save.
_JSON_VALIDATORS: dict[str, type] = {}


def _get_json_validators() -> dict[str, type]:
    """Lazy-load Pydantic validators to avoid circular imports."""
    if not _JSON_VALIDATORS:
        from app.schemas.billing_config import (
            CreditProductsConfig,
            RideCreditTiersConfig,
        )

        _JSON_VALIDATORS.update(
            {
                "credit_products": CreditProductsConfig,
                "ride_credit_tiers": RideCreditTiersConfig,
            }
        )
    return _JSON_VALIDATORS


class AppConfigAdmin(ModelView, model=AppConfig):
    """Admin view for AppConfig model with JSON validation."""

    name = "App Config"
    name_plural = "App Configs"
    icon = "fa-solid fa-gear"

    column_list: ClassVar = [AppConfig.key, AppConfig.value, AppConfig.updated_at]
    column_sortable_list: ClassVar = [AppConfig.key, AppConfig.updated_at]

    # Use TextAreaField for 'value' so multiline JSON is easy to edit
    form_overrides: ClassVar = {"value": TextAreaField}
    form_widget_args: ClassVar = {
        "value": {"rows": 6},
    }

    can_create = True
    can_edit = True
    can_delete = False

    async def on_model_change(
        self, data: dict[str, Any], model: AppConfig, is_created: bool, request: Request
    ) -> None:
        """Validate JSON config values via Pydantic before saving."""
        validators = _get_json_validators()
        schema_cls = validators.get(model.key)
        if schema_cls is None:
            return

        raw_value = model.value
        try:
            parsed = json.loads(raw_value)
        except (json.JSONDecodeError, TypeError) as exc:
            raise ValueError(f"Invalid JSON for '{model.key}': {exc}") from exc

        try:
            schema_cls.model_validate(parsed)  # type: ignore[attr-defined]
        except Exception as exc:
            raise ValueError(f"Validation failed for '{model.key}': {exc}") from exc

    async def after_model_change(
        self, data: dict[str, Any], model: AppConfig, is_created: bool, request: Request
    ) -> None:
        """Invalidate Redis and in-memory cache after config create/update."""
        from app.redis import redis_client
        from app.services.config_service import invalidate_config

        try:
            await invalidate_config(model.key, redis_client)
        except Exception:
            logger.warning("Cache invalidation failed for key %s", model.key, exc_info=True)


class UserAdmin(ModelView, model=User):
    """Admin view for User model."""

    name = "User"
    name_plural = "Users"
    icon = "fa-solid fa-user"

    # Columns to display in list view (sensitive fields excluded)
    column_list: ClassVar = [User.id, User.email, User.phone_number, User.created_at]

    # Columns searchable in list view
    column_searchable_list: ClassVar = [User.email, User.phone_number]

    # Columns sortable in list view
    column_sortable_list: ClassVar = [User.id, User.email, User.created_at]

    # Default sort order (newest first)
    column_default_sort: ClassVar = [(User.created_at, True)]

    # Hide sensitive fields from details view
    column_details_exclude_list: ClassVar = [User.password_hash, User.fcm_token]

    # Exclude sensitive/complex fields from edit form
    form_excluded_columns: ClassVar = [
        User.password_hash,
        User.fcm_token,
        User.refresh_tokens,
        User.paired_device,
        User.search_filters,
        User.search_status,
        User.rides,
        User.accept_failures,
        User.credit_balance,
        User.credit_transactions,
        User.purchase_orders,
    ]

    # Allow creating users via admin panel
    can_create = True
    can_delete = True
    can_edit = True

    async def scaffold_form(self, rules: list[str] | None = None) -> type[Form]:
        """Add a Password field to the generated form."""
        form = await super().scaffold_form(rules)
        form.password = PasswordField("Password")
        return form

    async def on_model_change(
        self, data: dict[str, Any], model: User, is_created: bool, request: Request
    ) -> None:
        """Hash password on create; allow optional password update on edit."""
        from app.services.auth_service import hash_password

        password = data.get("password")
        if is_created:
            if not password:
                raise ValueError("Password is required when creating a user.")
            model.password_hash = hash_password(password)
        elif password:
            model.password_hash = hash_password(password)


class PairedDeviceAdmin(ModelView, model=PairedDevice):
    """Admin view for PairedDevice model."""

    name = "Paired Device"
    name_plural = "Paired Devices"
    icon = "fa-solid fa-mobile"

    column_list: ClassVar = [
        PairedDevice.id,
        PairedDevice.user_id,
        PairedDevice.device_id,
        PairedDevice.registered_at,
        PairedDevice.last_ping_at,
        PairedDevice.timezone,
    ]

    column_searchable_list: ClassVar = [PairedDevice.device_id]

    column_sortable_list: ClassVar = [PairedDevice.registered_at, PairedDevice.last_ping_at]

    # Hide sensitive token hash from details view
    column_details_exclude_list: ClassVar = [PairedDevice.device_token_hash]

    form_excluded_columns: ClassVar = [PairedDevice.device_token_hash]

    # Allow creating paired devices via admin panel
    can_create = True
    can_edit = True
    can_delete = True


class SearchFiltersAdmin(ModelView, model=SearchFilters):
    """Admin view for SearchFilters model."""

    name = "Search Filter"
    name_plural = "Search Filters"
    icon = "fa-solid fa-filter"

    column_list: ClassVar = [
        SearchFilters.id,
        SearchFilters.user_id,
        SearchFilters.min_price,
        SearchFilters.start_time,
        SearchFilters.working_time,
        SearchFilters.working_days,
    ]

    column_sortable_list: ClassVar = [SearchFilters.min_price, SearchFilters.working_time]


class SearchStatusAdmin(ModelView, model=SearchStatus):
    """Admin view for SearchStatus model."""

    name = "Search Status"
    name_plural = "Search Statuses"
    icon = "fa-solid fa-toggle-on"

    column_list: ClassVar = [
        SearchStatus.id,
        SearchStatus.user_id,
        SearchStatus.is_active,
        SearchStatus.updated_at,
    ]

    column_sortable_list: ClassVar = [SearchStatus.is_active, SearchStatus.updated_at]


class RideAdmin(ModelView, model=Ride):
    """Admin view for Ride model."""

    name = "Ride"
    name_plural = "Rides"
    icon = "fa-solid fa-car"

    column_list: ClassVar = [
        Ride.id,
        Ride.user_id,
        Ride.event_type,
        Ride.idempotency_key,
        Ride.created_at,
    ]

    column_searchable_list: ClassVar = [Ride.idempotency_key, Ride.event_type]

    column_sortable_list: ClassVar = [Ride.created_at, Ride.event_type]

    column_default_sort: ClassVar = [(Ride.created_at, True)]

    can_create = True
    can_edit = True
    can_delete = True


class AcceptFailureAdmin(ModelView, model=AcceptFailure):
    """Admin view for AcceptFailure model."""

    name = "Accept Failure"
    name_plural = "Accept Failures"
    icon = "fa-solid fa-exclamation-triangle"

    column_list: ClassVar = [
        AcceptFailure.id,
        AcceptFailure.user_id,
        AcceptFailure.reason,
        AcceptFailure.ride_price,
        AcceptFailure.reported_at,
    ]

    column_searchable_list: ClassVar = [AcceptFailure.reason]

    column_sortable_list: ClassVar = [AcceptFailure.reported_at, AcceptFailure.ride_price]

    column_default_sort: ClassVar = [(AcceptFailure.reported_at, True)]

    # Failures are created via API
    can_create = False
    can_edit = True
    can_delete = True


class RefreshTokenAdmin(ModelView, model=RefreshToken):
    """Admin view for RefreshToken model."""

    name = "Refresh Token"
    name_plural = "Refresh Tokens"
    icon = "fa-solid fa-key"

    column_list: ClassVar = [
        RefreshToken.id,
        RefreshToken.user_id,
        RefreshToken.created_at,
        RefreshToken.expires_at,
    ]

    column_sortable_list: ClassVar = [RefreshToken.created_at, RefreshToken.expires_at]

    # Hide token hash from details view
    column_details_exclude_list: ClassVar = [RefreshToken.token_hash]

    form_excluded_columns: ClassVar = [RefreshToken.token_hash]

    # Tokens are created via API
    can_create = False
    can_edit = False
    can_delete = True
