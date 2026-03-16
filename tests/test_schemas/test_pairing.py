import uuid

import pytest
from pydantic import ValidationError

from app.schemas.pairing import (
    PairingStatusResponse,
    SearchLoginRequest,
    SearchLoginResponse,
)


class TestSearchLoginRequestValid:
    """SearchLoginRequest with valid inputs."""

    def test_valid_request(self):
        schema = SearchLoginRequest(
            email="test@example.com",
            password="securePass1",
            device_id="android-id-1",
            timezone="America/New_York",
        )
        assert schema.email == "test@example.com"
        assert schema.password == "securePass1"
        assert schema.device_id == "android-id-1"
        assert schema.timezone == "America/New_York"
        assert schema.device_model is None

    def test_valid_with_device_model(self):
        schema = SearchLoginRequest(
            email="test@example.com",
            password="securePass1",
            device_id="android-id-1",
            timezone="America/New_York",
            device_model="Samsung SM-A156U",
        )
        assert schema.device_model == "Samsung SM-A156U"

    def test_device_model_optional(self):
        schema = SearchLoginRequest(
            email="test@example.com",
            password="pass",
            device_id="dev1",
            timezone="UTC",
        )
        assert schema.device_model is None


class TestSearchLoginRequestInvalid:
    """SearchLoginRequest rejects invalid inputs."""

    def test_invalid_email_raises_validation_error(self):
        with pytest.raises(ValidationError) as exc_info:
            SearchLoginRequest(
                email="not-an-email",
                password="pass",
                device_id="dev1",
                timezone="UTC",
            )
        assert "email" in str(exc_info.value)

    def test_empty_device_id_raises_validation_error(self):
        with pytest.raises(ValidationError) as exc_info:
            SearchLoginRequest(
                email="test@example.com",
                password="pass",
                device_id="",
                timezone="UTC",
            )
        assert "device_id" in str(exc_info.value)

    def test_empty_timezone_raises_validation_error(self):
        with pytest.raises(ValidationError) as exc_info:
            SearchLoginRequest(
                email="test@example.com",
                password="pass",
                device_id="dev1",
                timezone="",
            )
        assert "timezone" in str(exc_info.value)

    def test_missing_fields_raises_validation_error(self):
        with pytest.raises(ValidationError):
            SearchLoginRequest()


class TestSearchLoginResponse:
    """SearchLoginResponse with device_token and user_id."""

    def test_valid_uuid(self):
        uid = uuid.uuid4()
        schema = SearchLoginResponse(device_token="some-long-token", user_id=uid)
        assert schema.device_token == "some-long-token"
        assert schema.user_id == uid

    def test_accepts_string_uuid(self):
        uid = uuid.uuid4()
        schema = SearchLoginResponse(device_token="token", user_id=str(uid))
        assert schema.user_id == uid

    def test_invalid_uuid_raises_validation_error(self):
        with pytest.raises(ValidationError):
            SearchLoginResponse(device_token="token", user_id="not-a-uuid")

    def test_serialization_user_id_as_string(self):
        uid = uuid.uuid4()
        schema = SearchLoginResponse(device_token="token", user_id=uid)
        data = schema.model_dump(mode="json")
        assert isinstance(data["user_id"], str)
        assert data["user_id"] == str(uid)


class TestPairingStatusResponse:
    """PairingStatusResponse serialization."""

    def test_paired_with_device_model(self):
        schema = PairingStatusResponse(
            paired=True, device_id="dev-001", device_model="Samsung Galaxy A14"
        )
        data = schema.model_dump(mode="json")
        assert data["paired"] is True
        assert data["device_id"] == "dev-001"
        assert data["device_model"] == "Samsung Galaxy A14"

    def test_paired_without_device_model(self):
        schema = PairingStatusResponse(paired=True, device_id="dev-001")
        assert schema.device_model is None

    def test_not_paired(self):
        schema = PairingStatusResponse(paired=False)
        data = schema.model_dump(mode="json")
        assert data["paired"] is False
        assert data["device_id"] is None
        assert data["device_model"] is None
