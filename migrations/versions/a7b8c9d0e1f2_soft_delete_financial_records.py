"""soft-delete financial records and change cascade to SET NULL

Revision ID: a7b8c9d0e1f2
Revises: a3b4c5d6e7f8
Create Date: 2026-04-11 06:00:00.000000

"""

from typing import Sequence, Union

from alembic import op

import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "a7b8c9d0e1f2"
down_revision: Union[str, Sequence[str], None] = "a3b4c5d6e7f8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add deleted_at columns and change FK cascade from CASCADE to SET NULL."""
    # Add deleted_at to credit_transactions
    op.add_column(
        "credit_transactions",
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
    )

    # Add deleted_at to purchase_orders
    op.add_column(
        "purchase_orders",
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
    )

    # Change credit_transactions.user_id FK: CASCADE → SET NULL, nullable
    op.alter_column("credit_transactions", "user_id", existing_type=sa.UUID(), nullable=True)
    op.drop_constraint(
        "credit_transactions_user_id_fkey", "credit_transactions", type_="foreignkey"
    )
    op.create_foreign_key(
        "credit_transactions_user_id_fkey",
        "credit_transactions",
        "users",
        ["user_id"],
        ["id"],
        ondelete="SET NULL",
    )

    # Change purchase_orders.user_id FK: CASCADE → SET NULL, nullable
    op.alter_column("purchase_orders", "user_id", existing_type=sa.UUID(), nullable=True)
    op.drop_constraint("purchase_orders_user_id_fkey", "purchase_orders", type_="foreignkey")
    op.create_foreign_key(
        "purchase_orders_user_id_fkey",
        "purchase_orders",
        "users",
        ["user_id"],
        ["id"],
        ondelete="SET NULL",
    )


def downgrade() -> None:
    """Revert to CASCADE and remove deleted_at columns."""
    # Revert purchase_orders FK
    op.drop_constraint("purchase_orders_user_id_fkey", "purchase_orders", type_="foreignkey")
    op.create_foreign_key(
        "purchase_orders_user_id_fkey",
        "purchase_orders",
        "users",
        ["user_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.alter_column("purchase_orders", "user_id", existing_type=sa.UUID(), nullable=False)

    # Revert credit_transactions FK
    op.drop_constraint(
        "credit_transactions_user_id_fkey", "credit_transactions", type_="foreignkey"
    )
    op.create_foreign_key(
        "credit_transactions_user_id_fkey",
        "credit_transactions",
        "users",
        ["user_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.alter_column("credit_transactions", "user_id", existing_type=sa.UUID(), nullable=False)

    # Remove deleted_at columns
    op.drop_column("purchase_orders", "deleted_at")
    op.drop_column("credit_transactions", "deleted_at")
