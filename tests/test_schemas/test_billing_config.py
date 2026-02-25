"""Tests for billing AppConfig Pydantic schemas."""

import pytest
from pydantic import ValidationError

from app.schemas.billing_config import (
    CreditProduct,
    CreditProductsConfig,
    RideCreditTier,
    RideCreditTiersConfig,
)

# ---------------------------------------------------------------------------
# CreditProduct
# ---------------------------------------------------------------------------


class TestCreditProductValid:
    """CreditProduct with valid data parses correctly."""

    def test_basic(self):
        p = CreditProduct(product_id="credits_50", credits=50, price_usd=40.00)
        assert p.product_id == "credits_50"
        assert p.credits == 50
        assert p.price_usd == 40.00

    def test_minimal_values(self):
        p = CreditProduct(product_id="x", credits=1, price_usd=0.01)
        assert p.credits == 1
        assert p.price_usd == 0.01

    def test_serialization_round_trip(self):
        p = CreditProduct(product_id="credits_100", credits=100, price_usd=80.0)
        data = p.model_dump(mode="json")
        restored = CreditProduct.model_validate(data)
        assert restored == p


class TestCreditProductInvalid:
    """CreditProduct rejects invalid data."""

    def test_negative_credits(self):
        with pytest.raises(ValidationError, match="credits"):
            CreditProduct(product_id="x", credits=-5, price_usd=10.0)

    def test_zero_credits(self):
        with pytest.raises(ValidationError, match="credits"):
            CreditProduct(product_id="x", credits=0, price_usd=10.0)

    def test_negative_price(self):
        with pytest.raises(ValidationError, match="price_usd"):
            CreditProduct(product_id="x", credits=10, price_usd=-1.0)

    def test_zero_price(self):
        with pytest.raises(ValidationError, match="price_usd"):
            CreditProduct(product_id="x", credits=10, price_usd=0)

    def test_empty_product_id(self):
        with pytest.raises(ValidationError, match="product_id"):
            CreditProduct(product_id="", credits=10, price_usd=10.0)


# ---------------------------------------------------------------------------
# CreditProductsConfig
# ---------------------------------------------------------------------------


class TestCreditProductsConfigValid:
    """CreditProductsConfig with valid data."""

    def test_single_product(self):
        cfg = CreditProductsConfig.model_validate(
            [{"product_id": "credits_10", "credits": 10, "price_usd": 10.0}]
        )
        assert len(cfg.root) == 1

    def test_multiple_products(self):
        cfg = CreditProductsConfig.model_validate(
            [
                {"product_id": "credits_10", "credits": 10, "price_usd": 10.0},
                {"product_id": "credits_25", "credits": 25, "price_usd": 22.0},
                {"product_id": "credits_50", "credits": 50, "price_usd": 40.0},
                {"product_id": "credits_100", "credits": 100, "price_usd": 80.0},
            ]
        )
        assert len(cfg.root) == 4

    def test_from_json_string(self):
        """Parse from JSON string (mimics AppConfig.value deserialization)."""
        import json

        raw = json.dumps(
            [
                {"product_id": "credits_10", "credits": 10, "price_usd": 10.0},
            ]
        )
        cfg = CreditProductsConfig.model_validate_json(raw)
        assert cfg.root[0].product_id == "credits_10"

    def test_serialization_round_trip(self):
        data = [
            {"product_id": "credits_10", "credits": 10, "price_usd": 10.0},
            {"product_id": "credits_50", "credits": 50, "price_usd": 40.0},
        ]
        cfg = CreditProductsConfig.model_validate(data)
        dumped = cfg.model_dump(mode="json")
        restored = CreditProductsConfig.model_validate(dumped)
        assert len(restored.root) == 2
        assert restored.root[0].product_id == "credits_10"


class TestCreditProductsConfigInvalid:
    """CreditProductsConfig rejects invalid data."""

    def test_empty_list(self):
        with pytest.raises(ValidationError, match="at least one product"):
            CreditProductsConfig.model_validate([])

    def test_duplicate_product_ids(self):
        with pytest.raises(ValidationError, match="Duplicate product_id"):
            CreditProductsConfig.model_validate(
                [
                    {"product_id": "credits_10", "credits": 10, "price_usd": 10.0},
                    {"product_id": "credits_10", "credits": 20, "price_usd": 20.0},
                ]
            )

    def test_invalid_product_in_list(self):
        with pytest.raises(ValidationError):
            CreditProductsConfig.model_validate(
                [
                    {"product_id": "credits_10", "credits": -1, "price_usd": 10.0},
                ]
            )


class TestCreditProductsConfigGetProductById:
    """CreditProductsConfig.get_product_by_id() helper."""

    @pytest.fixture()
    def catalog(self) -> CreditProductsConfig:
        return CreditProductsConfig.model_validate(
            [
                {"product_id": "credits_10", "credits": 10, "price_usd": 10.0},
                {"product_id": "credits_50", "credits": 50, "price_usd": 40.0},
                {"product_id": "credits_100", "credits": 100, "price_usd": 80.0},
            ]
        )

    def test_found(self, catalog: CreditProductsConfig):
        p = catalog.get_product_by_id("credits_50")
        assert p is not None
        assert p.credits == 50
        assert p.price_usd == 40.0

    def test_not_found(self, catalog: CreditProductsConfig):
        assert catalog.get_product_by_id("nonexistent") is None

    def test_first_product(self, catalog: CreditProductsConfig):
        p = catalog.get_product_by_id("credits_10")
        assert p is not None
        assert p.credits == 10

    def test_last_product(self, catalog: CreditProductsConfig):
        p = catalog.get_product_by_id("credits_100")
        assert p is not None
        assert p.credits == 100


# ---------------------------------------------------------------------------
# RideCreditTier
# ---------------------------------------------------------------------------


class TestRideCreditTierValid:
    """RideCreditTier with valid data."""

    def test_with_max_price(self):
        t = RideCreditTier(max_price=20.0, credits=1)
        assert t.max_price == 20.0
        assert t.credits == 1

    def test_catch_all_null(self):
        t = RideCreditTier(max_price=None, credits=3)
        assert t.max_price is None
        assert t.credits == 3

    def test_catch_all_default(self):
        """max_price defaults to None when omitted."""
        t = RideCreditTier(credits=3)
        assert t.max_price is None


class TestRideCreditTierInvalid:
    """RideCreditTier rejects invalid data."""

    def test_zero_credits(self):
        with pytest.raises(ValidationError, match="credits"):
            RideCreditTier(max_price=20.0, credits=0)

    def test_negative_credits(self):
        with pytest.raises(ValidationError, match="credits"):
            RideCreditTier(max_price=20.0, credits=-1)


# ---------------------------------------------------------------------------
# RideCreditTiersConfig
# ---------------------------------------------------------------------------


class TestRideCreditTiersConfigValid:
    """RideCreditTiersConfig with valid data."""

    def test_standard_tiers_from_prd(self):
        cfg = RideCreditTiersConfig.model_validate(
            [
                {"max_price": 20.0, "credits": 1},
                {"max_price": 50.0, "credits": 2},
                {"max_price": None, "credits": 3},
            ]
        )
        assert len(cfg.root) == 3

    def test_single_catch_all(self):
        cfg = RideCreditTiersConfig.model_validate(
            [
                {"max_price": None, "credits": 5},
            ]
        )
        assert len(cfg.root) == 1

    def test_all_non_null(self):
        """Config with no catch-all tier is valid."""
        cfg = RideCreditTiersConfig.model_validate(
            [
                {"max_price": 10.0, "credits": 1},
                {"max_price": 30.0, "credits": 2},
            ]
        )
        assert len(cfg.root) == 2

    def test_from_json_string(self):
        import json

        raw = json.dumps(
            [
                {"max_price": 20.0, "credits": 1},
                {"max_price": None, "credits": 3},
            ]
        )
        cfg = RideCreditTiersConfig.model_validate_json(raw)
        assert len(cfg.root) == 2

    def test_serialization_round_trip(self):
        data = [
            {"max_price": 20.0, "credits": 1},
            {"max_price": 50.0, "credits": 2},
            {"max_price": None, "credits": 3},
        ]
        cfg = RideCreditTiersConfig.model_validate(data)
        dumped = cfg.model_dump(mode="json")
        restored = RideCreditTiersConfig.model_validate(dumped)
        assert len(restored.root) == 3
        assert restored.root[0].max_price == 20.0
        assert restored.root[2].max_price is None


class TestRideCreditTiersConfigInvalid:
    """RideCreditTiersConfig rejects invalid data."""

    def test_empty_list(self):
        with pytest.raises(ValidationError, match="at least one tier"):
            RideCreditTiersConfig.model_validate([])

    def test_wrong_order(self):
        with pytest.raises(ValidationError, match="ascending order"):
            RideCreditTiersConfig.model_validate(
                [
                    {"max_price": 50.0, "credits": 2},
                    {"max_price": 20.0, "credits": 1},
                ]
            )

    def test_equal_prices(self):
        with pytest.raises(ValidationError, match="ascending order"):
            RideCreditTiersConfig.model_validate(
                [
                    {"max_price": 20.0, "credits": 1},
                    {"max_price": 20.0, "credits": 2},
                ]
            )

    def test_null_not_last(self):
        with pytest.raises(ValidationError, match="must be the last tier"):
            RideCreditTiersConfig.model_validate(
                [
                    {"max_price": None, "credits": 3},
                    {"max_price": 50.0, "credits": 2},
                ]
            )

    def test_multiple_null_tiers(self):
        with pytest.raises(ValidationError, match="Only one catch-all"):
            RideCreditTiersConfig.model_validate(
                [
                    {"max_price": None, "credits": 1},
                    {"max_price": None, "credits": 2},
                ]
            )

    def test_invalid_tier_in_list(self):
        with pytest.raises(ValidationError):
            RideCreditTiersConfig.model_validate(
                [
                    {"max_price": 20.0, "credits": 0},
                ]
            )


class TestGetCreditsForPrice:
    """RideCreditTiersConfig.get_credits_for_price() tier matching."""

    @pytest.fixture()
    def tiers(self) -> RideCreditTiersConfig:
        """Standard PRD tiers: <=20 -> 1, <=50 -> 2, else -> 3."""
        return RideCreditTiersConfig.model_validate(
            [
                {"max_price": 20.0, "credits": 1},
                {"max_price": 50.0, "credits": 2},
                {"max_price": None, "credits": 3},
            ]
        )

    def test_below_first_tier(self, tiers: RideCreditTiersConfig):
        assert tiers.get_credits_for_price(10.0) == 1

    def test_exact_first_boundary(self, tiers: RideCreditTiersConfig):
        assert tiers.get_credits_for_price(20.0) == 1

    def test_between_tiers(self, tiers: RideCreditTiersConfig):
        assert tiers.get_credits_for_price(35.0) == 2

    def test_exact_second_boundary(self, tiers: RideCreditTiersConfig):
        assert tiers.get_credits_for_price(50.0) == 2

    def test_above_last_boundary(self, tiers: RideCreditTiersConfig):
        assert tiers.get_credits_for_price(100.0) == 3

    def test_very_high_price(self, tiers: RideCreditTiersConfig):
        assert tiers.get_credits_for_price(999999.0) == 3

    def test_very_low_price(self, tiers: RideCreditTiersConfig):
        assert tiers.get_credits_for_price(0.01) == 1

    def test_just_above_first_boundary(self, tiers: RideCreditTiersConfig):
        assert tiers.get_credits_for_price(20.01) == 2

    def test_just_above_second_boundary(self, tiers: RideCreditTiersConfig):
        assert tiers.get_credits_for_price(50.01) == 3


class TestGetCreditsForPriceNoNullTier:
    """Tier matching when no catch-all tier exists."""

    @pytest.fixture()
    def tiers(self) -> RideCreditTiersConfig:
        return RideCreditTiersConfig.model_validate(
            [
                {"max_price": 20.0, "credits": 1},
                {"max_price": 50.0, "credits": 2},
            ]
        )

    def test_within_range(self, tiers: RideCreditTiersConfig):
        assert tiers.get_credits_for_price(30.0) == 2

    def test_above_all_tiers_fallback(self, tiers: RideCreditTiersConfig):
        """Without a null tier, price above all tiers falls back to last."""
        assert tiers.get_credits_for_price(100.0) == 2
