"""Remove monitoring-owned risk fields from analysis policy.

Revision ID: 20260916_02
Revises: 20260909_01
Create Date: 2026-09-16
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "20260916_02"
down_revision: str | None = "20260909_01"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_constraint(
        op.f("ck_analysis_policy_score"),
        "analysis_policy",
        type_="check",
    )
    op.drop_column("analysis_policy", "severity")
    op.drop_column("analysis_policy", "score")


def downgrade() -> None:
    op.add_column(
        "analysis_policy",
        sa.Column("score", sa.SmallInteger(), nullable=False, server_default="0"),
    )
    op.add_column(
        "analysis_policy",
        sa.Column("severity", sa.String(length=20), nullable=False, server_default="INFO"),
    )
    op.create_check_constraint(
        op.f("ck_analysis_policy_score"),
        "analysis_policy",
        "score BETWEEN 0 AND 100",
    )
    op.alter_column("analysis_policy", "score", server_default=None)
    op.alter_column("analysis_policy", "severity", server_default=None)
