"""Lower the prolonged appliance use limits to two minutes.

Revision ID: 20260921_16
Revises: 20260921_15
Create Date: 2026-09-21
"""

from collections.abc import Sequence

from alembic import op


revision: str = "20260921_16"
down_revision: str | None = "20260921_15"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE analysis_policy
        SET parameters = jsonb_set(
                parameters,
                '{limits_minutes}',
                '{"INDUCTION": 2, "IRON": 2}'::jsonb,
                true
            )
        WHERE event_type = 'PROLONGED_APPLIANCE_USE'
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE analysis_policy
        SET parameters = jsonb_set(
                parameters,
                '{limits_minutes}',
                '{"INDUCTION": 120, "IRON": 60}'::jsonb,
                true
            )
        WHERE event_type = 'PROLONGED_APPLIANCE_USE'
        """
    )
