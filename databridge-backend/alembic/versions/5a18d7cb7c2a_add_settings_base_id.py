"""Add settings base id

Revision ID: 5a18d7cb7c2a
Revises: 0b7abe2958e6
Create Date: 2026-04-16 16:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "5a18d7cb7c2a"
down_revision: Union[str, Sequence[str], None] = "0b7abe2958e6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("settings", sa.Column("base_id", sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column("settings", "base_id")
