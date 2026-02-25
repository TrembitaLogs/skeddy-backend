"""Pydantic schemas for billing AppConfig validation.

Used to parse and validate JSON values stored in AppConfig
for ``credit_products`` and ``ride_credit_tiers`` keys.
"""

from __future__ import annotations

from pydantic import BaseModel, Field, RootModel, model_validator


class CreditProduct(BaseModel):
    """A single purchasable credit product from the catalog."""

    product_id: str = Field(min_length=1)
    credits: int = Field(gt=0)
    price_usd: float = Field(gt=0)


class CreditProductsConfig(RootModel[list[CreditProduct]]):
    """Validated list of credit products.

    Guarantees:
    - At least one product
    - All ``product_id`` values are unique
    """

    @model_validator(mode="after")
    def validate_products(self) -> CreditProductsConfig:
        products = self.root
        if not products:
            raise ValueError("credit_products must contain at least one product")
        ids = [p.product_id for p in products]
        if len(ids) != len(set(ids)):
            seen: set[str] = set()
            duplicates: list[str] = []
            for pid in ids:
                if pid in seen:
                    duplicates.append(pid)
                seen.add(pid)
            raise ValueError(f"Duplicate product_id values: {duplicates}")
        return self

    def get_product_by_id(self, product_id: str) -> CreditProduct | None:
        """Look up a product by its ``product_id``."""
        for product in self.root:
            if product.product_id == product_id:
                return product
        return None


class RideCreditTier(BaseModel):
    """A single ride-price-to-credits tier.

    ``max_price=None`` acts as a catch-all for any price above
    the previous tiers.
    """

    max_price: float | None = None
    credits: int = Field(gt=0)


class RideCreditTiersConfig(RootModel[list[RideCreditTier]]):
    """Validated list of ride credit tiers.

    Guarantees:
    - At least one tier
    - Non-null ``max_price`` values are in ascending order
    - Only the last tier may have ``max_price=None`` (catch-all)
    - At most one ``null`` tier
    """

    @model_validator(mode="after")
    def validate_tiers(self) -> RideCreditTiersConfig:
        tiers = self.root
        if not tiers:
            raise ValueError("ride_credit_tiers must contain at least one tier")

        null_indices = [i for i, t in enumerate(tiers) if t.max_price is None]
        if len(null_indices) > 1:
            raise ValueError("Only one catch-all tier (max_price=null) is allowed")
        if null_indices and null_indices[0] != len(tiers) - 1:
            raise ValueError("Catch-all tier (max_price=null) must be the last tier")

        # Check ascending order for non-null tiers
        non_null_prices = [t.max_price for t in tiers if t.max_price is not None]
        for i in range(1, len(non_null_prices)):
            if non_null_prices[i] <= non_null_prices[i - 1]:
                raise ValueError(
                    "Tier max_price values must be in strictly ascending order, "
                    f"but got {non_null_prices[i - 1]} followed by {non_null_prices[i]}"
                )
        return self

    def get_credits_for_price(self, price: float) -> int:
        """Determine the credit cost for a given ride price.

        Tiers are evaluated top-down.  The first tier whose
        ``max_price`` is ``>= price`` (or ``None``) wins.
        """
        for tier in self.root:
            if tier.max_price is None or price <= tier.max_price:
                return tier.credits
        # Fallback — should not happen when a null catch-all exists
        return self.root[-1].credits
