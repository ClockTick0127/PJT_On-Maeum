"""Create household outing state table.

Revision ID: 20260919_06
Revises: 20260919_05
Create Date: 2026-09-19
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "20260919_06"
down_revision: str | None = "20260919_05"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "household_outing_state",
        sa.Column("household_id", sa.String(length=50), nullable=False),
        sa.Column("is_outing", sa.Boolean(), nullable=False),
        sa.Column("outing_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_returned_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_event_id", sa.Uuid(), nullable=False),
        sa.Column("last_event_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "(is_outing AND outing_started_at IS NOT NULL) OR "
            "(NOT is_outing AND outing_started_at IS NULL)",
            name=op.f(
                "ck_household_outing_state_outing_started_at_matches_state"
            ),
        ),
        sa.PrimaryKeyConstraint(
            "household_id",
            name=op.f("pk_household_outing_state"),
        ),
    )


def downgrade() -> None:
    op.drop_table("household_outing_state")
