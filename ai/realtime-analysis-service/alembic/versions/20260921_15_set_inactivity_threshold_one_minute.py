"""Lower the prolonged inactivity threshold to one minute.

Revision ID: 20260921_15
Revises: 20260920_14
Create Date: 2026-09-21
"""

from collections.abc import Sequence

from alembic import op


revision: str = "20260921_15"
down_revision: str | None = "20260920_14"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE analysis_policy
        SET parameters = jsonb_set(
                parameters,
                '{inactivity_hours}',
                '0.0167'::jsonb,
                true
            )
        WHERE event_type = 'PROLONGED_INACTIVITY'
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE analysis_policy
        SET parameters = jsonb_set(
                parameters,
                '{inactivity_hours}',
                '6'::jsonb,
                true
            )
        WHERE event_type = 'PROLONGED_INACTIVITY'
        """
    )
