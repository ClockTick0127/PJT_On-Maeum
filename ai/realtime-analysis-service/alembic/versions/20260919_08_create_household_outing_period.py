"""Create household outing periods for routine overlap checks.

Revision ID: 20260919_08
Revises: 20260919_07
Create Date: 2026-09-19
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "20260919_08"
down_revision: str | None = "20260919_07"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "household_outing_period",
        sa.Column("started_event_id", sa.Uuid(), nullable=False),
        sa.Column("household_id", sa.String(length=50), nullable=False),
        sa.Column("ended_event_id", sa.Uuid(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "ended_at IS NULL OR ended_at >= started_at",
            name=op.f("ck_household_outing_period_valid_time_range"),
        ),
        sa.PrimaryKeyConstraint(
            "started_event_id",
            name=op.f("pk_household_outing_period"),
        ),
        sa.UniqueConstraint(
            "ended_event_id",
            name=op.f("uq_household_outing_period_ended_event"),
        ),
    )
    op.create_index(
        "ix_household_outing_period_overlap",
        "household_outing_period",
        ["household_id", "started_at", "ended_at"],
        unique=False,
    )
    op.create_index(
        "uq_household_outing_period_open_household",
        "household_outing_period",
        ["household_id"],
        unique=True,
        postgresql_where=sa.text("ended_at IS NULL"),
    )
    op.execute(
        """
        INSERT INTO household_outing_period (
            started_event_id,
            household_id,
            ended_event_id,
            started_at,
            ended_at,
            updated_at
        )
        SELECT
            last_event_id,
            household_id,
            NULL,
            outing_started_at,
            NULL,
            updated_at
        FROM household_outing_state
        WHERE is_outing = true
          AND outing_started_at IS NOT NULL
        """
    )


def downgrade() -> None:
    op.drop_index(
        "uq_household_outing_period_open_household",
        table_name="household_outing_period",
    )
    op.drop_index(
        "ix_household_outing_period_overlap",
        table_name="household_outing_period",
    )
    op.drop_table("household_outing_period")
