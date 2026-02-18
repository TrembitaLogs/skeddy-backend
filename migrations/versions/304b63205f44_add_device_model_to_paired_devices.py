"""add device_model to paired_devices

Revision ID: 304b63205f44
Revises: 2e926db57b8b
Create Date: 2026-02-17 11:50:00.374280

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '304b63205f44'
down_revision: Union[str, Sequence[str], None] = '2e926db57b8b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("paired_devices", sa.Column("device_model", sa.String(255), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("paired_devices", "device_model")
