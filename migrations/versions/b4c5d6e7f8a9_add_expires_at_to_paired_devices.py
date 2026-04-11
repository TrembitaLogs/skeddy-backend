"""add expires_at to paired_devices

Revision ID: b4c5d6e7f8a9
Revises: a3b4c5d6e7f8
Create Date: 2026-04-11 12:00:00.000000

"""
from datetime import UTC, datetime, timedelta
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "b4c5d6e7f8a9"
down_revision: Union[str, Sequence[str], None] = "a3b4c5d6e7f8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "paired_devices",
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "idx_paired_devices_expires_at", "paired_devices", ["expires_at"]
    )

    # Backfill existing rows: set expires_at = now + 90 days
    paired_devices = sa.table(
        "paired_devices",
        sa.column("expires_at", sa.DateTime(timezone=True)),
    )
    op.execute(
        paired_devices.update().values(
            expires_at=datetime.now(UTC) + timedelta(days=90)
        )
    )


def downgrade() -> None:
    op.drop_index("idx_paired_devices_expires_at", table_name="paired_devices")
    op.drop_column("paired_devices", "expires_at")
