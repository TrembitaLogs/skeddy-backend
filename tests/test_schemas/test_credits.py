import pytest
from pydantic import ValidationError

from app.schemas.credits import (
    CreditProductSchema,
    PurchaseRequest,
    PurchaseResponse,
)


class TestPurchaseRequestValid:
    """Valid PurchaseRequest parses correctly."""

    def test_valid_request(self):
        req = PurchaseRequest(
            product_id="credits_50",
            purchase_token="some-long-google-play-token-string",
        )
        assert req.product_id == "credits_50"
        assert req.purchase_token == "some-long-google-play-token-string"

    def test_product_id_with_underscores(self):
        req = PurchaseRequest(
            product_id="credits_100",
            purchase_token="token123",
        )
        assert req.product_id == "credits_100"

    def test_long_purchase_token(self):
        long_token = "a" * 2000
        req = PurchaseRequest(
            product_id="credits_10",
            purchase_token=long_token,
        )
        assert req.purchase_token == long_token

    def test_serialization_round_trip(self):
        req = PurchaseRequest(
            product_id="credits_25",
            purchase_token="google-play-token-xyz",
        )
        data = req.model_dump(mode="json")
        restored = PurchaseRequest.model_validate(data)
        assert restored.product_id == req.product_id
        assert restored.purchase_token == req.purchase_token


class TestPurchaseRequestInvalidProductId:
    """Invalid product_id raises ValidationError."""

    def test_empty_string_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            PurchaseRequest(
                product_id="",
                purchase_token="valid-token",
            )
        assert "product_id" in str(exc_info.value)

    def test_whitespace_only_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            PurchaseRequest(
                product_id="   ",
                purchase_token="valid-token",
            )
        assert "product_id" in str(exc_info.value)


class TestPurchaseRequestInvalidToken:
    """Invalid purchase_token raises ValidationError."""

    def test_empty_string_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            PurchaseRequest(
                product_id="credits_50",
                purchase_token="",
            )
        assert "purchase_token" in str(exc_info.value)

    def test_whitespace_only_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            PurchaseRequest(
                product_id="credits_50",
                purchase_token="   ",
            )
        assert "purchase_token" in str(exc_info.value)


class TestPurchaseResponseSerialization:
    """PurchaseResponse serializes correctly."""

    def test_serializes_fields(self):
        resp = PurchaseResponse(credits_added=50, new_balance=92)
        data = resp.model_dump(mode="json")
        assert data["credits_added"] == 50
        assert data["new_balance"] == 92

    def test_zero_balance(self):
        resp = PurchaseResponse(credits_added=10, new_balance=0)
        assert resp.new_balance == 0

    def test_large_values(self):
        resp = PurchaseResponse(credits_added=1000, new_balance=9999)
        data = resp.model_dump(mode="json")
        assert data["credits_added"] == 1000
        assert data["new_balance"] == 9999


class TestCreditProductSchema:
    """CreditProductSchema validation."""

    def test_valid_product(self):
        product = CreditProductSchema(
            product_id="credits_50",
            credits=50,
            price_usd=40.00,
        )
        assert product.product_id == "credits_50"
        assert product.credits == 50
        assert product.price_usd == 40.00

    def test_zero_credits_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            CreditProductSchema(
                product_id="credits_0",
                credits=0,
                price_usd=10.00,
            )
        assert "credits" in str(exc_info.value)

    def test_negative_credits_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            CreditProductSchema(
                product_id="credits_neg",
                credits=-5,
                price_usd=10.00,
            )
        assert "credits" in str(exc_info.value)

    def test_zero_price_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            CreditProductSchema(
                product_id="credits_10",
                credits=10,
                price_usd=0,
            )
        assert "price_usd" in str(exc_info.value)

    def test_negative_price_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            CreditProductSchema(
                product_id="credits_10",
                credits=10,
                price_usd=-5.00,
            )
        assert "price_usd" in str(exc_info.value)

    def test_serialization_round_trip(self):
        product = CreditProductSchema(
            product_id="credits_100",
            credits=100,
            price_usd=80.00,
        )
        data = product.model_dump(mode="json")
        restored = CreditProductSchema.model_validate(data)
        assert restored.product_id == product.product_id
        assert restored.credits == product.credits
        assert restored.price_usd == product.price_usd
