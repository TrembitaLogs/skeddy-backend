"""add legacy_credits table for old balance migration

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-03-29 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID


# revision identifiers, used by Alembic.
revision: str = 'b2c3d4e5f6a7'
down_revision: str = 'a1b2c3d4e5f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'legacy_credits',
        sa.Column('id', UUID(as_uuid=True), primary_key=True),
        sa.Column('old_user_id', sa.Integer(), nullable=False),
        sa.Column('phone_number', sa.String(20), nullable=False),
        sa.Column('license_number', sa.String(50), nullable=False),
        sa.Column('name', sa.String(255), nullable=True),
        sa.Column('email', sa.String(255), nullable=True),
        sa.Column('balance', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('claimed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('imported_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index(
        'idx_legacy_credits_phone_license',
        'legacy_credits',
        ['phone_number', 'license_number'],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index('idx_legacy_credits_phone_license', table_name='legacy_credits')
    op.drop_table('legacy_credits')
