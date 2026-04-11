"""merge heads

Revision ID: c8d9e0f1a2b3
Revises: a7b8c9d0e1f2, b4c5d6e7f8a9
Create Date: 2026-04-11 13:00:00.000000

"""

from typing import Sequence, Union

# revision identifiers, used by Alembic.
revision: str = "c8d9e0f1a2b3"
down_revision: Union[str, Sequence[str]] = ("a7b8c9d0e1f2", "b4c5d6e7f8a9")
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
