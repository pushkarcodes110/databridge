"""Add webhook settings

Revision ID: a7e2c9f4a6b1
Revises: 5a18d7cb7c2a
Create Date: 2026-04-16 16:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "a7e2c9f4a6b1"
down_revision: Union[str, Sequence[str], None] = "5a18d7cb7c2a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("settings", sa.Column("webhook_enabled", sa.Integer(), nullable=True, server_default="0"))
    op.add_column("settings", sa.Column("webhook_url", sa.String(), nullable=True))
    op.add_column("settings", sa.Column("webhook_batch_size", sa.Integer(), nullable=True, server_default="500"))


def downgrade() -> None:
    op.drop_column("settings", "webhook_batch_size")
    op.drop_column("settings", "webhook_url")
    op.drop_column("settings", "webhook_enabled")
