import uuid

import pytest
from pydantic import ValidationError

from app.schemas.auth import (
    AuthResponse,
    ChangePasswordRequest,
    DeleteAccountRequest,
    LoginRequest,
    OkResponse,
    RefreshRequest,
    RegisterRequest,
    ResetPasswordRequest,
)
from app.schemas.profile import UpdateProfileRequest


class TestRegisterRequest:
    """Tests for RegisterRequest schema."""

    def test_valid_data(self):
        schema = RegisterRequest(email="user@example.com", password="secureP8")
        assert schema.email == "user@example.com"
        assert schema.password == "secureP8"

    def test_short_password_raises_validation_error(self):
        with pytest.raises(ValidationError) as exc_info:
            RegisterRequest(email="user@example.com", password="short")
        assert "password" in str(exc_info.value)

    def test_password_exactly_8_chars_with_uppercase(self):
        schema = RegisterRequest(email="user@example.com", password="Abcdefgh")
        assert schema.password == "Abcdefgh"

    def test_password_without_uppercase_raises_validation_error(self):
        with pytest.raises(ValidationError) as exc_info:
            RegisterRequest(email="user@example.com", password="alllowercase1")
        assert "PASSWORD_REQUIRES_UPPERCASE" in str(exc_info.value)

    def test_password_all_digits_raises_validation_error(self):
        with pytest.raises(ValidationError) as exc_info:
            RegisterRequest(email="user@example.com", password="12345678")
        assert "PASSWORD_REQUIRES_UPPERCASE" in str(exc_info.value)

    def test_invalid_email_raises_validation_error(self):
        with pytest.raises(ValidationError) as exc_info:
            RegisterRequest(email="not-an-email", password="securePassword123")
        assert "email" in str(exc_info.value)

    def test_empty_email_raises_validation_error(self):
        with pytest.raises(ValidationError):
            RegisterRequest(email="", password="securePassword123")

    def test_missing_fields_raises_validation_error(self):
        with pytest.raises(ValidationError):
            RegisterRequest()


class TestLoginRequest:
    """Tests for LoginRequest schema."""

    def test_valid_data(self):
        schema = LoginRequest(email="user@example.com", password="any")
        assert schema.email == "user@example.com"
        assert schema.password == "any"

    def test_invalid_email_raises_validation_error(self):
        with pytest.raises(ValidationError):
            LoginRequest(email="bad-email", password="password123")

    def test_short_password_allowed(self):
        schema = LoginRequest(email="user@example.com", password="x")
        assert schema.password == "x"


class TestAuthResponse:
    """Tests for AuthResponse schema."""

    def test_valid_data(self):
        uid = uuid.uuid4()
        schema = AuthResponse(
            user_id=uid,
            access_token="jwt.token.here",
            refresh_token="refresh_token_value",
        )
        assert schema.user_id == uid
        assert schema.access_token == "jwt.token.here"
        assert schema.refresh_token == "refresh_token_value"

    def test_serialization_user_id_as_string(self):
        uid = uuid.uuid4()
        schema = AuthResponse(
            user_id=uid,
            access_token="access",
            refresh_token="refresh",
        )
        data = schema.model_dump(mode="json")
        assert isinstance(data["user_id"], str)
        assert data["user_id"] == str(uid)

    def test_accepts_string_uuid(self):
        uid = uuid.uuid4()
        schema = AuthResponse(
            user_id=str(uid),
            access_token="access",
            refresh_token="refresh",
        )
        assert schema.user_id == uid

    def test_invalid_uuid_raises_validation_error(self):
        with pytest.raises(ValidationError):
            AuthResponse(
                user_id="not-a-uuid",
                access_token="access",
                refresh_token="refresh",
            )


class TestChangePasswordRequest:
    """Tests for ChangePasswordRequest schema."""

    def test_valid_data(self):
        schema = ChangePasswordRequest(current_password="oldPassword1", new_password="newPasswd1")
        assert schema.current_password == "oldPassword1"
        assert schema.new_password == "newPasswd1"

    def test_short_new_password_raises_validation_error(self):
        with pytest.raises(ValidationError) as exc_info:
            ChangePasswordRequest(current_password="oldPassword1", new_password="short")
        assert "new_password" in str(exc_info.value)

    def test_new_password_exactly_8_chars_with_uppercase(self):
        schema = ChangePasswordRequest(current_password="oldPassword1", new_password="Abcdefgh")
        assert schema.new_password == "Abcdefgh"

    def test_new_password_without_uppercase_raises_validation_error(self):
        with pytest.raises(ValidationError) as exc_info:
            ChangePasswordRequest(current_password="oldPassword1", new_password="alllowercase1")
        assert "PASSWORD_REQUIRES_UPPERCASE" in str(exc_info.value)

    def test_missing_fields_raises_validation_error(self):
        with pytest.raises(ValidationError):
            ChangePasswordRequest()

    def test_current_password_no_min_length(self):
        schema = ChangePasswordRequest(current_password="x", new_password="newPasswd1")
        assert schema.current_password == "x"


class TestRefreshRequest:
    """Tests for RefreshRequest schema."""

    def test_valid_data(self):
        schema = RefreshRequest(refresh_token="some_token_value")
        assert schema.refresh_token == "some_token_value"

    def test_missing_token_raises_validation_error(self):
        with pytest.raises(ValidationError):
            RefreshRequest()


class TestUpdateProfileRequest:
    """Tests for UpdateProfileRequest schema."""

    def test_phone_none_is_valid(self):
        schema = UpdateProfileRequest(phone_number=None)
        assert schema.phone_number is None

    def test_valid_ukrainian_number(self):
        schema = UpdateProfileRequest(phone_number="+380501234567")
        assert schema.phone_number == "+380501234567"

    def test_valid_short_number(self):
        schema = UpdateProfileRequest(phone_number="+1234567")
        assert schema.phone_number == "+1234567"

    def test_valid_max_length_number(self):
        schema = UpdateProfileRequest(phone_number="+123456789012345")
        assert schema.phone_number == "+123456789012345"

    def test_missing_plus_raises_validation_error(self):
        with pytest.raises(ValidationError) as exc_info:
            UpdateProfileRequest(phone_number="380501234567")
        assert "INVALID_PHONE_FORMAT" in str(exc_info.value)

    def test_too_short_raises_validation_error(self):
        with pytest.raises(ValidationError) as exc_info:
            UpdateProfileRequest(phone_number="+123456")
        assert "INVALID_PHONE_FORMAT" in str(exc_info.value)

    def test_too_long_raises_validation_error(self):
        with pytest.raises(ValidationError) as exc_info:
            UpdateProfileRequest(phone_number="+1234567890123456")
        assert "INVALID_PHONE_FORMAT" in str(exc_info.value)

    def test_letters_in_number_raises_validation_error(self):
        with pytest.raises(ValidationError) as exc_info:
            UpdateProfileRequest(phone_number="+12345abc90")
        assert "INVALID_PHONE_FORMAT" in str(exc_info.value)

    def test_empty_string_raises_validation_error(self):
        with pytest.raises(ValidationError) as exc_info:
            UpdateProfileRequest(phone_number="")
        assert "INVALID_PHONE_FORMAT" in str(exc_info.value)

    def test_model_fields_set_tracks_provided_fields(self):
        schema = UpdateProfileRequest(phone_number="+12025551234")
        assert "phone_number" in schema.model_fields_set


class TestDeleteAccountRequest:
    """Tests for DeleteAccountRequest schema."""

    def test_valid_data(self):
        schema = DeleteAccountRequest(password="currentPassword123")
        assert schema.password == "currentPassword123"

    def test_missing_password_raises_validation_error(self):
        with pytest.raises(ValidationError):
            DeleteAccountRequest()

    def test_short_password_allowed(self):
        schema = DeleteAccountRequest(password="x")
        assert schema.password == "x"


class TestResetPasswordRequest:
    """Tests for ResetPasswordRequest password policy."""

    def test_valid_new_password_with_uppercase(self):
        schema = ResetPasswordRequest(
            email="user@example.com", code="12345678", new_password="newPasswd"
        )
        assert schema.new_password == "newPasswd"

    def test_new_password_without_uppercase_raises_validation_error(self):
        with pytest.raises(ValidationError) as exc_info:
            ResetPasswordRequest(
                email="user@example.com", code="12345678", new_password="alllowercase1"
            )
        assert "PASSWORD_REQUIRES_UPPERCASE" in str(exc_info.value)

    def test_short_new_password_raises_validation_error(self):
        with pytest.raises(ValidationError):
            ResetPasswordRequest(email="user@example.com", code="12345678", new_password="Short")


class TestOkResponse:
    """Tests for OkResponse schema."""

    def test_default_ok_true(self):
        schema = OkResponse()
        assert schema.ok is True

    def test_serialization(self):
        data = OkResponse().model_dump(mode="json")
        assert data == {"ok": True}
