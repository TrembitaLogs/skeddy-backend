"""add billing models and ride verification columns

Revision ID: d7f2a8b31c04
Revises: c4a8e2f19b03
Create Date: 2026-02-24 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd7f2a8b31c04'
down_revision: Union[str, Sequence[str], None] = 'c4a8e2f19b03'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create billing tables and add verification columns to rides."""

    # --- credit_balances ---
    op.create_table(
        'credit_balances',
        sa.Column('id', sa.UUID(), nullable=False),
        sa.Column('user_id', sa.UUID(), nullable=False),
        sa.Column('balance', sa.Integer(), nullable=False),
        sa.Column(
            'updated_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('user_id'),
        sa.CheckConstraint(
            'balance >= 0', name='ck_credit_balances_balance_non_negative'
        ),
    )
    op.create_index(
        'idx_credit_balances_low',
        'credit_balances',
        ['balance'],
        postgresql_where=sa.text('balance > 0'),
    )

    # --- credit_transactions ---
    op.create_table(
        'credit_transactions',
        sa.Column('id', sa.UUID(), nullable=False),
        sa.Column('user_id', sa.UUID(), nullable=False),
        sa.Column('type', sa.String(length=30), nullable=False),
        sa.Column('amount', sa.Integer(), nullable=False),
        sa.Column('balance_after', sa.Integer(), nullable=False),
        sa.Column('reference_id', sa.UUID(), nullable=True),
        sa.Column('description', sa.String(length=500), nullable=True),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(
        'idx_credit_transactions_user_created',
        'credit_transactions',
        ['user_id', sa.literal_column('created_at DESC')],
    )
    op.create_index(
        'idx_credit_transactions_reference',
        'credit_transactions',
        ['reference_id'],
    )

    # --- purchase_orders ---
    op.create_table(
        'purchase_orders',
        sa.Column('id', sa.UUID(), nullable=False),
        sa.Column('user_id', sa.UUID(), nullable=False),
        sa.Column('google_order_id', sa.String(length=255), nullable=True),
        sa.Column('product_id', sa.String(length=100), nullable=False),
        sa.Column('purchase_token', sa.Text(), nullable=False),
        sa.Column('credits_amount', sa.Integer(), nullable=False),
        sa.Column('status', sa.String(length=20), nullable=False),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.Column('verified_at', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('google_order_id'),
        sa.UniqueConstraint('purchase_token'),
        sa.CheckConstraint(
            'credits_amount > 0',
            name='ck_purchase_orders_credits_amount_positive',
        ),
    )
    op.create_index(
        'idx_purchase_orders_user', 'purchase_orders', ['user_id']
    )
    op.create_index(
        'idx_purchase_orders_consumed',
        'purchase_orders',
        ['status'],
        postgresql_where=sa.text("status = 'CONSUMED'"),
    )

    # --- rides: add billing and verification columns ---
    op.add_column(
        'rides',
        sa.Column('ride_hash', sa.String(length=64), nullable=False),
    )
    op.add_column(
        'rides',
        sa.Column(
            'verification_status',
            sa.String(length=20),
            server_default='PENDING',
            nullable=True,
        ),
    )
    op.add_column(
        'rides',
        sa.Column(
            'verification_deadline', sa.DateTime(timezone=True), nullable=True
        ),
    )
    op.add_column(
        'rides',
        sa.Column('verified_at', sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        'rides',
        sa.Column('disappeared_at', sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        'rides',
        sa.Column('last_reported_present', sa.Boolean(), nullable=True),
    )
    op.add_column(
        'rides',
        sa.Column(
            'last_verification_requested_at',
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.add_column(
        'rides',
        sa.Column(
            'credits_charged',
            sa.Integer(),
            nullable=False,
            server_default='0',
        ),
    )
    op.add_column(
        'rides',
        sa.Column(
            'credits_refunded',
            sa.Integer(),
            nullable=False,
            server_default='0',
        ),
    )
    op.create_check_constraint(
        'ck_rides_credits_charged_non_negative',
        'rides',
        'credits_charged >= 0',
    )
    op.create_check_constraint(
        'ck_rides_credits_refunded_non_negative',
        'rides',
        'credits_refunded >= 0',
    )
    op.create_index(
        'idx_rides_verification',
        'rides',
        ['verification_status', 'verification_deadline'],
        postgresql_where=sa.text("verification_status = 'PENDING'"),
    )
    op.create_index('idx_rides_ride_hash', 'rides', ['ride_hash'])


def downgrade() -> None:
    """Remove billing tables and ride verification columns."""

    # --- rides: drop indexes, constraints, and columns ---
    op.drop_index('idx_rides_ride_hash', table_name='rides')
    op.drop_index('idx_rides_verification', table_name='rides')
    op.drop_constraint(
        'ck_rides_credits_refunded_non_negative', 'rides', type_='check'
    )
    op.drop_constraint(
        'ck_rides_credits_charged_non_negative', 'rides', type_='check'
    )
    op.drop_column('rides', 'credits_refunded')
    op.drop_column('rides', 'credits_charged')
    op.drop_column('rides', 'last_verification_requested_at')
    op.drop_column('rides', 'last_reported_present')
    op.drop_column('rides', 'disappeared_at')
    op.drop_column('rides', 'verified_at')
    op.drop_column('rides', 'verification_deadline')
    op.drop_column('rides', 'verification_status')
    op.drop_column('rides', 'ride_hash')

    # --- purchase_orders ---
    op.drop_index('idx_purchase_orders_consumed', table_name='purchase_orders')
    op.drop_index('idx_purchase_orders_user', table_name='purchase_orders')
    op.drop_table('purchase_orders')

    # --- credit_transactions ---
    op.drop_index(
        'idx_credit_transactions_reference', table_name='credit_transactions'
    )
    op.drop_index(
        'idx_credit_transactions_user_created',
        table_name='credit_transactions',
    )
    op.drop_table('credit_transactions')

    # --- credit_balances ---
    op.drop_index('idx_credit_balances_low', table_name='credit_balances')
    op.drop_table('credit_balances')
