"""Add persisted analysis event emission history.

Revision ID: 20260918_03
Revises: 20260916_02
Create Date: 2026-09-18
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "20260918_03"
down_revision: str | None = "20260916_02"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "analysis_event_emission",
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("household_id", sa.String(length=50), nullable=False),
        sa.Column("event_type", sa.String(length=50), nullable=False),
        sa.Column("appliance_type", sa.String(length=50), nullable=True),
        sa.Column("emitted_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint(
            "event_id",
            name=op.f("pk_analysis_event_emission"),
        ),
    )
    op.create_index(
        "ix_analysis_event_emission_cooldown",
        "analysis_event_emission",
        ["household_id", "event_type", "appliance_type", "emitted_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_analysis_event_emission_cooldown",
        table_name="analysis_event_emission",
    )
    op.drop_table("analysis_event_emission")
