"""Keep only the latest household outing state.

Revision ID: 20260919_07
Revises: 20260919_06
Create Date: 2026-09-19
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "20260919_07"
down_revision: str | None = "20260919_06"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Remove history created by the earlier draft if it was already applied."""

    inspector = sa.inspect(op.get_bind())
    if "household_outing_event" not in inspector.get_table_names():
        return

    foreign_key_names = {
        item["name"]
        for item in inspector.get_foreign_keys("household_outing_state")
        if item["name"] is not None
    }
    foreign_key_name = op.f(
        "fk_household_outing_state_last_event_id_household_outing_event"
    )
    if foreign_key_name in foreign_key_names:
        op.drop_constraint(
            foreign_key_name,
            "household_outing_state",
            type_="foreignkey",
        )

    op.drop_table("household_outing_event")


def downgrade() -> None:
    # Revision 06 now defines only household_outing_state. Revision 07 exists
    # solely to clean up databases that applied the earlier local draft.
    pass
