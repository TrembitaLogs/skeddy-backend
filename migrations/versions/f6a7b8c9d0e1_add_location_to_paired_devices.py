"""add location to paired_devices

Revision ID: f6a7b8c9d0e1
Revises: e5f6a7b8c9d0
Create Date: 2026-04-02 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f6a7b8c9d0e1'
down_revision: Union[str, None] = 'e5f6a7b8c9d0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("paired_devices", sa.Column("latitude", sa.Float, nullable=True))
    op.add_column("paired_devices", sa.Column("longitude", sa.Float, nullable=True))
    op.add_column("paired_devices", sa.Column("location_updated_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("paired_devices", "location_updated_at")
    op.drop_column("paired_devices", "longitude")
    op.drop_column("paired_devices", "latitude")
