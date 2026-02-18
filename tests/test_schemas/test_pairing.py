import uuid
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from app.schemas.pairing import (
    ConfirmPairingRequest,
    ConfirmPairingResponse,
    GeneratePairingResponse,
    PairingStatusResponse,
)


class TestConfirmPairingRequestValid:
    """ConfirmPairingRequest with valid 6-digit code."""

    def test_valid_code(self):
        schema = ConfirmPairingRequest(
            code="123456", device_id="android-id-1", timezone="America/New_York"
        )
        assert schema.code == "123456"
        assert schema.device_id == "android-id-1"
        assert schema.timezone == "America/New_York"
        assert schema.device_model is None

    def test_valid_with_device_model(self):
        schema = ConfirmPairingRequest(
            code="123456",
            device_id="android-id-1",
            timezone="America/New_York",
            device_model="Samsung SM-A156U",
        )
        assert schema.device_model == "Samsung SM-A156U"

    def test_device_model_optional(self):
        schema = ConfirmPairingRequest(code="123456", device_id="dev1", timezone="UTC")
        assert schema.device_model is None

    def test_code_boundary_low(self):
        schema = ConfirmPairingRequest(code="100000", device_id="dev1", timezone="UTC")
        assert schema.code == "100000"

    def test_code_boundary_high(self):
        schema = ConfirmPairingRequest(code="999999", device_id="dev1", timezone="UTC")
        assert schema.code == "999999"

    def test_code_all_zeros(self):
        schema = ConfirmPairingRequest(code="000000", device_id="dev1", timezone="UTC")
        assert schema.code == "000000"


class TestConfirmPairingRequestInvalidCode:
    """ConfirmPairingRequest rejects invalid codes."""

    def test_five_digits_raises_validation_error(self):
        with pytest.raises(ValidationError) as exc_info:
            ConfirmPairingRequest(code="12345", device_id="dev1", timezone="UTC")
        assert "code" in str(exc_info.value)

    def test_seven_digits_raises_validation_error(self):
        with pytest.raises(ValidationError) as exc_info:
            ConfirmPairingRequest(code="1234567", device_id="dev1", timezone="UTC")
        assert "code" in str(exc_info.value)

    def test_letters_in_code_raises_validation_error(self):
        with pytest.raises(ValidationError) as exc_info:
            ConfirmPairingRequest(code="12345a", device_id="dev1", timezone="UTC")
        assert "code" in str(exc_info.value)

    def test_empty_code_raises_validation_error(self):
        with pytest.raises(ValidationError):
            ConfirmPairingRequest(code="", device_id="dev1", timezone="UTC")

    def test_spaces_in_code_raises_validation_error(self):
        with pytest.raises(ValidationError):
            ConfirmPairingRequest(code="123 56", device_id="dev1", timezone="UTC")


class TestConfirmPairingRequestInvalidFields:
    """ConfirmPairingRequest rejects empty device_id and timezone."""

    def test_empty_device_id_raises_validation_error(self):
        with pytest.raises(ValidationError) as exc_info:
            ConfirmPairingRequest(code="123456", device_id="", timezone="UTC")
        assert "device_id" in str(exc_info.value)

    def test_empty_timezone_raises_validation_error(self):
        with pytest.raises(ValidationError) as exc_info:
            ConfirmPairingRequest(code="123456", device_id="dev1", timezone="")
        assert "timezone" in str(exc_info.value)

    def test_missing_fields_raises_validation_error(self):
        with pytest.raises(ValidationError):
            ConfirmPairingRequest()


class TestGeneratePairingResponse:
    """GeneratePairingResponse serialization."""

    def test_serializes_to_json(self):
        ts = datetime(2026, 2, 9, 15, 5, 0, tzinfo=UTC)
        schema = GeneratePairingResponse(code="482917", expires_at=ts)
        data = schema.model_dump(mode="json")
        assert data["code"] == "482917"
        assert isinstance(data["expires_at"], str)

    def test_fields_preserved(self):
        ts = datetime(2026, 2, 9, 15, 5, 0, tzinfo=UTC)
        schema = GeneratePairingResponse(code="100000", expires_at=ts)
        assert schema.code == "100000"
        assert schema.expires_at == ts


class TestConfirmPairingResponse:
    """ConfirmPairingResponse with device_token and user_id."""

    def test_valid_uuid(self):
        uid = uuid.uuid4()
        schema = ConfirmPairingResponse(device_token="some-long-token", user_id=uid)
        assert schema.device_token == "some-long-token"
        assert schema.user_id == uid

    def test_accepts_string_uuid(self):
        uid = uuid.uuid4()
        schema = ConfirmPairingResponse(device_token="token", user_id=str(uid))
        assert schema.user_id == uid

    def test_invalid_uuid_raises_validation_error(self):
        with pytest.raises(ValidationError):
            ConfirmPairingResponse(device_token="token", user_id="not-a-uuid")

    def test_serialization_user_id_as_string(self):
        uid = uuid.uuid4()
        schema = ConfirmPairingResponse(device_token="token", user_id=uid)
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
