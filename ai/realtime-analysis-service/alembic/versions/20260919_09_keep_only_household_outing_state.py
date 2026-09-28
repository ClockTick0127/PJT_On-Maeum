"""Keep only household outing state and its latest interval.

Revision ID: 20260919_09
Revises: 20260919_08
Create Date: 2026-09-19
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "20260919_09"
down_revision: str | None = "20260919_08"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Preserve the latest interval in the single state row before removing the
    # short-lived interval table from revision 08.
    op.execute(
        """
        UPDATE household_outing_state AS state
        SET
            outing_started_at = latest.started_at,
            last_returned_at = CASE
                WHEN latest.ended_at IS NOT NULL THEN latest.ended_at
                ELSE state.last_returned_at
            END
        FROM (
            SELECT DISTINCT ON (household_id)
                household_id,
                started_at,
                ended_at
            FROM household_outing_period
            ORDER BY household_id, started_at DESC
        ) AS latest
        WHERE state.household_id = latest.household_id
        """
    )
    op.drop_table("household_outing_period")
    op.drop_constraint(
        op.f("ck_household_outing_state_outing_started_at_matches_state"),
        "household_outing_state",
        type_="check",
    )
    op.create_check_constraint(
        op.f("ck_household_outing_state_outing_started_at_available_when_outing"),
        "household_outing_state",
        "NOT is_outing OR outing_started_at IS NOT NULL",
    )


def downgrade() -> None:
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
            CASE WHEN is_outing THEN NULL ELSE last_event_id END,
            outing_started_at,
            CASE WHEN is_outing THEN NULL ELSE last_returned_at END,
            updated_at
        FROM household_outing_state
        WHERE outing_started_at IS NOT NULL
          AND (
              is_outing
              OR last_returned_at IS NULL
              OR last_returned_at >= outing_started_at
          )
        """
    )
    op.drop_constraint(
        op.f(
            "ck_household_outing_state_outing_started_at_available_when_outing"
        ),
        "household_outing_state",
        type_="check",
    )
    op.execute(
        """
        UPDATE household_outing_state
        SET outing_started_at = NULL
        WHERE NOT is_outing
        """
    )
    op.create_check_constraint(
        op.f("ck_household_outing_state_outing_started_at_matches_state"),
        "household_outing_state",
        "(is_outing AND outing_started_at IS NOT NULL) OR "
        "(NOT is_outing AND outing_started_at IS NULL)",
    )
