"""add license_number to users

Revision ID: c3d4e5f6a7b8
Revises: b2c3d4e5f6a7
Create Date: 2026-03-29 13:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c3d4e5f6a7b8'
down_revision: str = 'b2c3d4e5f6a7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('users', sa.Column('license_number', sa.String(50), nullable=True))
    op.create_index('ix_users_license_number', 'users', ['license_number'], unique=True)


def downgrade() -> None:
    op.drop_index('ix_users_license_number', table_name='users')
    op.drop_column('users', 'license_number')
