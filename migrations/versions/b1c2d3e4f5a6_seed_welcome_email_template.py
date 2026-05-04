"""seed welcome email template

Revision ID: b1c2d3e4f5a6
Revises: e9f74122e224
Create Date: 2026-05-04 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "b1c2d3e4f5a6"
down_revision: str = "e9f74122e224"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


WELCOME_SUBJECT_EN = "Welcome to Skeddy"
WELCOME_BODY_EN = (
    "Welcome to Skeddy!\n\n"
    "Your account is verified and {bonus_amount} bonus credits have been added to your balance.\n\n"
    "Download the Skeddy Search app for Android here:\n"
    "{search_app_url}\n\n"
    "— Skeddy Team"
)
WELCOME_SUBJECT_ES = "Bienvenido a Skeddy"
WELCOME_BODY_ES = (
    "¡Bienvenido a Skeddy!\n\n"
    "Tu cuenta está verificada y se han añadido {bonus_amount} créditos de bonificación a tu saldo.\n\n"
    "Descarga la aplicación Skeddy Search para Android aquí:\n"
    "{search_app_url}\n\n"
    "— Equipo de Skeddy"
)


def upgrade() -> None:
    op.execute(
        sa.text(
            "INSERT INTO email_templates "
            "(email_type, subject_en, body_en, subject_es, body_es) "
            "VALUES (:email_type, :subject_en, :body_en, :subject_es, :body_es) "
            "ON CONFLICT (email_type) DO NOTHING"
        ).bindparams(
            email_type="WELCOME",
            subject_en=WELCOME_SUBJECT_EN,
            body_en=WELCOME_BODY_EN,
            subject_es=WELCOME_SUBJECT_ES,
            body_es=WELCOME_BODY_ES,
        )
    )


def downgrade() -> None:
    op.execute(
        sa.text("DELETE FROM email_templates WHERE email_type = 'WELCOME'")
    )
