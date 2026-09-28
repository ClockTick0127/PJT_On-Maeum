"""Exclude the expected sleep window from prolonged inactivity.

Revision ID: 20260919_05
Revises: 20260918_04
Create Date: 2026-09-19
"""

from collections.abc import Sequence

from alembic import op


revision: str = "20260919_05"
down_revision: str | None = "20260918_04"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE analysis_policy
        SET algorithm_type = 'AWAKE_INACTIVITY_ELAPSED',
            parameters = jsonb_set(
                jsonb_set(
                    parameters,
                    '{inactivity_hours}',
                    '6'::jsonb,
                    true
                ),
                '{sleep_window}',
                '{"start":"23:00","end":"07:00"}'::jsonb,
                true
            )
        WHERE event_type = 'PROLONGED_INACTIVITY'
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE analysis_policy
        SET algorithm_type = 'LAST_ACTIVITY_ELAPSED',
            parameters = jsonb_set(
                parameters - 'sleep_window',
                '{inactivity_hours}',
                '12'::jsonb,
                true
            )
        WHERE event_type = 'PROLONGED_INACTIVITY'
        """
    )
