"""add expires_at to paired_devices

Revision ID: e9f74122e224
Revises: c8d9e0f1a2b3
Create Date: 2026-04-11 12:00:00.000000

"""
from datetime import UTC, datetime, timedelta
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "e9f74122e224"
down_revision: Union[str, Sequence[str], None] = "c8d9e0f1a2b3"
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
