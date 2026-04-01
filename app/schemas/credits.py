import re

from pydantic import BaseModel, Field, field_validator


class PurchaseRequest(BaseModel):
    """Request schema for POST /credits/purchase."""

    product_id: str = Field(min_length=1)
    purchase_token: str = Field(min_length=1)

    @field_validator("product_id")
    @classmethod
    def validate_product_id(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("product_id must not be blank")
        return v

    @field_validator("purchase_token")
    @classmethod
    def validate_purchase_token(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("purchase_token must not be blank")
        return v


class PurchaseResponse(BaseModel):
    """Response schema for POST /credits/purchase."""

    credits_added: int
    new_balance: int


class RestoreCreditsRequest(BaseModel):
    """Request schema for POST /credits/restore."""

    phone_number: str
    license_number: str = Field(min_length=1)

    @field_validator("phone_number")
    @classmethod
    def validate_phone_e164(cls, v: str) -> str:
        if not re.match(r"^\+[0-9]{7,15}$", v):
            raise ValueError("INVALID_PHONE_FORMAT")
        return v

    @field_validator("license_number")
    @classmethod
    def validate_license_not_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("INVALID_LICENSE_FORMAT")
        return v


class RestoreCreditsResponse(BaseModel):
    """Response schema for POST /credits/restore."""

    ok: bool = True
    restored_credits: int


class CreditProductSchema(BaseModel):
    """Schema for a single credit product from AppConfig catalog."""

    product_id: str
    credits: int = Field(gt=0)
    price_usd: float = Field(gt=0)
