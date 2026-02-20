"""widen app_config value column and seed interval configs

Revision ID: c4a8e2f19b03
Revises: 3f535dcf865b
Create Date: 2026-02-20 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c4a8e2f19b03'
down_revision: Union[str, Sequence[str], None] = '3f535dcf865b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Widen app_configs.value to 1000 chars and seed interval schedule configs."""
    op.alter_column(
        "app_configs",
        "value",
        type_=sa.String(1000),
        existing_type=sa.String(500),
        existing_nullable=False,
    )
    op.execute(
        "INSERT INTO app_configs (key, value) VALUES "
        "('requests_per_day', '1920')"
    )
    op.execute(
        "INSERT INTO app_configs (key, value) VALUES "
        "('requests_per_hour', "
        "'[5.23,5.19,4.97,4.28,3.07,1.0,1.0,1.0,1.0,1.0,"
        "3.69,5.10,6.24,4.96,5.06,5.18,4.59,4.57,5.91,5.58,"
        "5.98,5.29,5.15,4.96]')"
    )


def downgrade() -> None:
    """Remove interval configs and revert column width."""
    op.execute("DELETE FROM app_configs WHERE key IN ('requests_per_day', 'requests_per_hour')")
    op.alter_column(
        "app_configs",
        "value",
        type_=sa.String(500),
        existing_type=sa.String(1000),
        existing_nullable=False,
    )
