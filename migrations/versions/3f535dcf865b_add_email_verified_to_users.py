"""add email_verified to users

Revision ID: 3f535dcf865b
Revises: a1f3c9d72e01
Create Date: 2026-02-20 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '3f535dcf865b'
down_revision: Union[str, Sequence[str], None] = 'a1f3c9d72e01'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add email_verified column. Existing users get true, new users default to false."""
    op.add_column(
        'users',
        sa.Column('email_verified', sa.Boolean(), nullable=False, server_default='true')
    )
    # Remove server_default so new rows use the Python-side default (False)
    op.alter_column('users', 'email_verified', server_default=None)


def downgrade() -> None:
    """Remove email_verified column."""
    op.drop_column('users', 'email_verified')
