"""seed clustering app config values

Revision ID: a3b4c5d6e7f8
Revises: f6a7b8c9d0e1
Create Date: 2026-04-09 16:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "a3b4c5d6e7f8"
down_revision: Union[str, Sequence[str], None] = "f6a7b8c9d0e1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Clustering AppConfig keys and their default values
CLUSTERING_CONFIGS = {
    "clustering_enabled": "false",
    "clustering_penalty_minutes": "60",
    "clustering_threshold_miles": "16",
    "clustering_rebuild_interval_minutes": "5",
}


def upgrade() -> None:
    """Seed clustering-related AppConfig values (idempotent)."""
    for key, value in CLUSTERING_CONFIGS.items():
        op.execute(
            sa.text(
                "INSERT INTO app_configs (key, value) "
                "VALUES (:key, :value) "
                "ON CONFLICT (key) DO NOTHING"
            ).bindparams(key=key, value=value)
        )


def downgrade() -> None:
    """Remove clustering-related AppConfig values."""
    keys = list(CLUSTERING_CONFIGS.keys())
    op.execute(
        sa.text("DELETE FROM app_configs WHERE key = ANY(:keys)").bindparams(
            sa.bindparam("keys", value=keys, type_=sa.ARRAY(sa.String))
        )
    )
