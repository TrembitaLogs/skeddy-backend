"""add app_version to paired_devices

Revision ID: b6dc720bf058
Revises: 304b63205f44
Create Date: 2026-02-19 20:52:13.155997

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b6dc720bf058'
down_revision: Union[str, Sequence[str], None] = '304b63205f44'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("paired_devices", sa.Column("app_version", sa.String(20), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("paired_devices", "app_version")
