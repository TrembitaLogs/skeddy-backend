"""add email_templates table

Revision ID: e5f6a7b8c9d0
Revises: d4e5f6a7b8c9
Create Date: 2026-04-01 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e5f6a7b8c9d0'
down_revision: str = 'd4e5f6a7b8c9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SEED_TEMPLATES = [
    (
        "VERIFICATION",
        "Verify your Skeddy account",
        "Welcome to Skeddy!\n\nYour verification code is:\n\n{code}\n\nEnter this code in the Skeddy app to verify your email address.\nThis code expires in 24 hours.\n\nIf you didn't create a Skeddy account, you can safely ignore this email.\n\n\u2014 Skeddy Team",
        "Verifica tu cuenta de Skeddy",
        "\u00a1Bienvenido a Skeddy!\n\nTu c\u00f3digo de verificaci\u00f3n es:\n\n{code}\n\nIngresa este c\u00f3digo en la aplicaci\u00f3n Skeddy para verificar tu direcci\u00f3n de correo.\nEste c\u00f3digo expira en 24 horas.\n\nSi no creaste una cuenta en Skeddy, puedes ignorar este correo.\n\n\u2014 Equipo Skeddy",
    ),
    (
        "EMAIL_CHANGE",
        "Confirm your new Skeddy email",
        "You requested to change your Skeddy account email to this address.\n\nYour confirmation code is:\n\n{code}\n\nEnter this code in the Skeddy app to confirm the change.\nThis code expires in 24 hours.\n\nIf you didn't request this change, you can safely ignore this email.\n\n\u2014 Skeddy Team",
        "Confirma tu nuevo correo de Skeddy",
        "Solicitaste cambiar el correo de tu cuenta de Skeddy a esta direcci\u00f3n.\n\nTu c\u00f3digo de confirmaci\u00f3n es:\n\n{code}\n\nIngresa este c\u00f3digo en la aplicaci\u00f3n Skeddy para confirmar el cambio.\nEste c\u00f3digo expira en 24 horas.\n\nSi no solicitaste este cambio, puedes ignorar este correo.\n\n\u2014 Equipo Skeddy",
    ),
    (
        "PASSWORD_RESET",
        "Your Skeddy password reset code",
        "Your password reset code is:\n\n{code}\n\nEnter this code in the Skeddy app to reset your password.\nThis code expires in 15 minutes.\n\nIf you didn't request a password reset, you can safely ignore this email.\n\n\u2014 Skeddy Team",
        "Tu c\u00f3digo de restablecimiento de contrase\u00f1a de Skeddy",
        "Tu c\u00f3digo de restablecimiento de contrase\u00f1a es:\n\n{code}\n\nIngresa este c\u00f3digo en la aplicaci\u00f3n Skeddy para restablecer tu contrase\u00f1a.\nEste c\u00f3digo expira en 15 minutos.\n\nSi no solicitaste un restablecimiento de contrase\u00f1a, puedes ignorar este correo.\n\n\u2014 Equipo Skeddy",
    ),
]


def upgrade() -> None:
    op.create_table(
        'email_templates',
        sa.Column('email_type', sa.String(50), primary_key=True),
        sa.Column('subject_en', sa.String(200), nullable=False),
        sa.Column('body_en', sa.Text(), nullable=False),
        sa.Column('subject_es', sa.String(200), nullable=False),
        sa.Column('body_es', sa.Text(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    for email_type, subject_en, body_en, subject_es, body_es in SEED_TEMPLATES:
        op.execute(
            sa.text(
                "INSERT INTO email_templates (email_type, subject_en, body_en, subject_es, body_es) "
                "VALUES (:email_type, :subject_en, :body_en, :subject_es, :body_es) "
                "ON CONFLICT (email_type) DO NOTHING"
            ).bindparams(
                email_type=email_type, subject_en=subject_en, body_en=body_en,
                subject_es=subject_es, body_es=body_es,
            )
        )


def downgrade() -> None:
    op.drop_table('email_templates')
