"""replace license_number with legacy_user_id on users

Revision ID: d4e5f6a7b8c9
Revises: c3d4e5f6a7b8
Create Date: 2026-04-01 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd4e5f6a7b8c9'
down_revision: str = 'c3d4e5f6a7b8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_index('ix_users_license_number', table_name='users')
    op.drop_column('users', 'license_number')
    op.add_column('users', sa.Column('legacy_user_id', sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column('users', 'legacy_user_id')
    op.add_column('users', sa.Column('license_number', sa.String(50), nullable=True))
    op.create_index('ix_users_license_number', 'users', ['license_number'], unique=True)
