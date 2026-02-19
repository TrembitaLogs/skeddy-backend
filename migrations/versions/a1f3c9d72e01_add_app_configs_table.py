"""add app_configs table

Revision ID: a1f3c9d72e01
Revises: b6dc720bf058
Create Date: 2026-02-19 22:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a1f3c9d72e01'
down_revision: Union[str, Sequence[str], None] = 'b6dc720bf058'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create app_configs table and seed default values."""
    op.create_table(
        "app_configs",
        sa.Column("key", sa.String(100), primary_key=True),
        sa.Column("value", sa.String(500), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )
    op.execute(
        "INSERT INTO app_configs (key, value) VALUES ('min_search_app_version', '1.0.0')"
    )


def downgrade() -> None:
    """Drop app_configs table."""
    op.drop_table("app_configs")
