"""add user language, push_templates table, widen app_config value

Revision ID: a1b2c3d4e5f6
Revises: f1a2b3c4d5e6
Create Date: 2026-03-25 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, Sequence[str], None] = 'f1a2b3c4d5e6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SEED_TEMPLATES = [
    ("RIDE_ACCEPTED", "New Ride", "Ride from {pickup_location} to {dropoff_location}, ${price}", "Nuevo viaje", "Viaje de {pickup_location} a {dropoff_location}, ${price}"),
    ("SEARCH_OFFLINE", "Device Offline", "Your search device has been offline since {last_ping_at}", "Dispositivo fuera de línea", "Su dispositivo de búsqueda está fuera de línea desde {last_ping_at}"),
    ("CREDITS_DEPLETED", "Credits Depleted", "Your credit balance is empty. Top up to continue.", "Créditos agotados", "Su saldo de créditos está vacío. Recargue para continuar."),
    ("CREDITS_LOW", "Low Credits", "Your balance is {balance} credits. Minimum for a ride is {threshold}.", "Créditos bajos", "Su saldo es de {balance} créditos. Mínimo para un viaje es {threshold}."),
    ("RIDE_CREDIT_REFUNDED", "Credit Refunded", "{credits_refunded} credit(s) refunded. New balance: {new_balance}", "Crédito reembolsado", "{credits_refunded} crédito(s) reembolsado(s). Nuevo saldo: {new_balance}"),
    ("BALANCE_ADJUSTED", "Balance Updated", "Your balance was adjusted by {amount}. New balance: {new_balance}", "Saldo actualizado", "Su saldo fue ajustado en {amount}. Nuevo saldo: {new_balance}"),
    ("SEARCH_UPDATE_REQUIRED", "Update Required", "Your search device needs an update to version {min_version}.", "Actualización requerida", "Su dispositivo de búsqueda necesita una actualización a la versión {min_version}."),
]


def upgrade() -> None:
    # 1. Add language column to users
    op.add_column('users', sa.Column('language', sa.String(10), nullable=False, server_default='en'))

    # 2. Widen app_config value column from String(1000) to Text
    op.alter_column('app_configs', 'value',
                     existing_type=sa.String(1000),
                     type_=sa.Text(),
                     existing_nullable=False)

    # 3. Create push_templates table
    op.create_table(
        'push_templates',
        sa.Column('notification_type', sa.String(50), primary_key=True),
        sa.Column('title_en', sa.String(200), nullable=False),
        sa.Column('body_en', sa.String(500), nullable=False),
        sa.Column('title_es', sa.String(200), nullable=False),
        sa.Column('body_es', sa.String(500), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # 4. Seed default templates
    for ntype, title_en, body_en, title_es, body_es in SEED_TEMPLATES:
        op.execute(
            sa.text(
                "INSERT INTO push_templates (notification_type, title_en, body_en, title_es, body_es) "
                "VALUES (:ntype, :title_en, :body_en, :title_es, :body_es) "
                "ON CONFLICT (notification_type) DO NOTHING"
            ).bindparams(
                ntype=ntype, title_en=title_en, body_en=body_en,
                title_es=title_es, body_es=body_es,
            )
        )


def downgrade() -> None:
    op.drop_table('push_templates')
    op.alter_column('app_configs', 'value',
                     existing_type=sa.Text(),
                     type_=sa.String(1000),
                     existing_nullable=False)
    op.drop_column('users', 'language')
