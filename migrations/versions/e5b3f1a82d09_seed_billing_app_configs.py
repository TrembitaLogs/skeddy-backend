"""seed billing app config values

Revision ID: e5b3f1a82d09
Revises: d7f2a8b31c04
Create Date: 2026-02-25 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e5b3f1a82d09'
down_revision: Union[str, Sequence[str], None] = 'd7f2a8b31c04'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Billing AppConfig keys and their default values
BILLING_CONFIGS = {
    "credit_products": (
        '[{"product_id":"credits_10","credits":10,"price_usd":10.0},'
        '{"product_id":"credits_25","credits":25,"price_usd":22.0},'
        '{"product_id":"credits_50","credits":50,"price_usd":40.0},'
        '{"product_id":"credits_100","credits":100,"price_usd":80.0}]'
    ),
    "ride_credit_tiers": (
        '[{"max_price":20.0,"credits":1},'
        '{"max_price":50.0,"credits":2},'
        '{"max_price":null,"credits":3}]'
    ),
    "registration_bonus_credits": "10",
    "verification_deadline_minutes": "30",
    "verification_check_interval_minutes": "60",
}


def upgrade() -> None:
    """Seed billing-related AppConfig values (idempotent)."""
    for key, value in BILLING_CONFIGS.items():
        # Use parameterized query to avoid SQLAlchemy interpreting
        # colons in JSON values (e.g. ":10") as bind parameters.
        op.execute(
            sa.text(
                "INSERT INTO app_configs (key, value) "
                "VALUES (:key, :value) "
                "ON CONFLICT (key) DO NOTHING"
            ).bindparams(key=key, value=value)
        )


def downgrade() -> None:
    """Remove billing-related AppConfig values."""
    keys = list(BILLING_CONFIGS.keys())
    op.execute(
        sa.text("DELETE FROM app_configs WHERE key = ANY(:keys)").bindparams(
            sa.bindparam("keys", value=keys, type_=sa.ARRAY(sa.String))
        )
    )
