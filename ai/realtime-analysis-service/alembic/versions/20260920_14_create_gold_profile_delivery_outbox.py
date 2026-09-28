"""Create durable Gold profile Kafka outbox.

Revision ID: 20260920_14
Revises: 20260920_13
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "20260920_14"
down_revision: str | None = "20260920_13"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "gold_profile_delivery_outbox",
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("household_id", sa.String(50), nullable=False),
        sa.Column("profile_version", sa.String(100), nullable=False),
        sa.Column("profile_revision", sa.BigInteger(), nullable=False),
        sa.Column("delivery_mode", sa.String(20), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("profile_revision >= 1", name="ck_gold_profile_delivery_outbox_profile_revision_positive"),
        sa.CheckConstraint("delivery_mode IN ('ACTIVE', 'SHADOW')", name="ck_gold_profile_delivery_outbox_delivery_mode"),
        sa.CheckConstraint("status IN ('PENDING', 'PUBLISHED')", name="ck_gold_profile_delivery_outbox_status"),
        sa.PrimaryKeyConstraint("event_id"),
        sa.UniqueConstraint("household_id", "profile_version", "delivery_mode", name="uq_gold_profile_delivery_identity"),
    )
    op.create_index(
        "ix_gold_profile_delivery_pending", "gold_profile_delivery_outbox",
        ["status", "next_attempt_at"], unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_gold_profile_delivery_pending", table_name="gold_profile_delivery_outbox")
    op.drop_table("gold_profile_delivery_outbox")
