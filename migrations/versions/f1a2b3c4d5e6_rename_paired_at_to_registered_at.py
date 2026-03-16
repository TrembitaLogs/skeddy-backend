"""rename paired_at to registered_at in paired_devices

Revision ID: f1a2b3c4d5e6
Revises: e5b3f1a82d09
Create Date: 2026-03-16 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = 'f1a2b3c4d5e6'
down_revision: Union[str, Sequence[str], None] = 'e5b3f1a82d09'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Rename paired_at column to registered_at."""
    op.alter_column(
        "paired_devices",
        "paired_at",
        new_column_name="registered_at",
    )


def downgrade() -> None:
    """Revert registered_at back to paired_at."""
    op.alter_column(
        "paired_devices",
        "registered_at",
        new_column_name="paired_at",
    )
