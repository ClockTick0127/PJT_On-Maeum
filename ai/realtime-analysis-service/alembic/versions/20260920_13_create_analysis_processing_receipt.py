"""Create per-input analysis processing receipts and their lake outbox.

Revision ID: 20260920_13
Revises: 20260920_12
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "20260920_13"
down_revision: str | None = "20260920_12"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "analysis_processing_receipt",
        sa.Column("receipt_id", sa.Uuid(), nullable=False),
        sa.Column("message_id", sa.Uuid(), nullable=False),
        sa.Column("household_id", sa.String(50), nullable=False),
        sa.Column("device_id", sa.String(50), nullable=False),
        sa.Column("source_topic", sa.String(255), nullable=True),
        sa.Column("source_partition", sa.Integer(), nullable=True),
        sa.Column("source_offset", sa.BigInteger(), nullable=True),
        sa.Column("measured_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("analysis_run_id", sa.String(100), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("model_version", sa.String(50), nullable=False),
        sa.Column("pipeline_version", sa.String(50), nullable=False),
        sa.Column("state_epoch", sa.Uuid(), nullable=False),
        sa.Column("outcome", sa.String(40), nullable=False),
        sa.Column("appliance_types", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("session_change_refs", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("error_type", sa.String(200), nullable=True),
        sa.CheckConstraint(
            "outcome IN ('SUCCEEDED', 'SKIPPED_WARMUP', 'SKIPPED_QUALITY_GATE', "
            "'FAILED_INFERENCE', 'FAILED_PERSISTENCE')",
            name=op.f("ck_analysis_processing_receipt_outcome"),
        ),
        sa.CheckConstraint(
            "(source_partition IS NULL) = (source_offset IS NULL)",
            name=op.f("ck_analysis_processing_receipt_source_position_complete"),
        ),
        sa.PrimaryKeyConstraint("receipt_id", name=op.f("pk_analysis_processing_receipt")),
        sa.UniqueConstraint(
            "message_id", "analysis_run_id", "attempt",
            name="uq_analysis_receipt_message_run_attempt"
        ),
    )
    op.create_index(
        "ix_analysis_receipt_household_measured",
        "analysis_processing_receipt",
        ["household_id", "measured_at"],
    )
    op.create_table(
        "analysis_receipt_lake_batch",
        sa.Column("batch_id", sa.Uuid(), nullable=False),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("ingest_date", sa.Date(), nullable=False),
        sa.Column("event_count", sa.Integer(), nullable=False),
        sa.Column("manifest_path", sa.String(500), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("status IN ('ASSIGNED', 'FAILED', 'COMPLETED')", name=op.f("ck_analysis_receipt_lake_batch_status")),
        sa.CheckConstraint("event_count >= 0", name=op.f("ck_analysis_receipt_lake_batch_event_count_nonnegative")),
        sa.PrimaryKeyConstraint("batch_id", name=op.f("pk_analysis_receipt_lake_batch")),
    )
    op.create_table(
        "analysis_receipt_lake_outbox",
        sa.Column("event_id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("receipt_id", sa.Uuid(), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("changed_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("delivery_status", sa.String(20), server_default="PENDING", nullable=False),
        sa.Column("batch_id", sa.Uuid(), nullable=True),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "delivery_status IN ('PENDING', 'ASSIGNED', 'DELIVERED')",
            name=op.f("ck_analysis_receipt_lake_outbox_delivery_status"),
        ),
        sa.CheckConstraint(
            "(delivery_status = 'PENDING') = (batch_id IS NULL)",
            name=op.f("ck_analysis_receipt_lake_outbox_batch_assigned_matches_status"),
        ),
        sa.ForeignKeyConstraint(
            ["batch_id"], ["analysis_receipt_lake_batch.batch_id"],
            name=op.f("fk_analysis_receipt_lake_outbox_batch_id_analysis_receipt_lake_batch"),
        ),
        sa.ForeignKeyConstraint(
            ["receipt_id"], ["analysis_processing_receipt.receipt_id"],
            name=op.f("fk_analysis_receipt_lake_outbox_receipt_id_analysis_processing_receipt"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("event_id", name=op.f("pk_analysis_receipt_lake_outbox")),
        sa.UniqueConstraint("receipt_id", name=op.f("uq_analysis_receipt_lake_outbox_receipt_id")),
    )
    op.create_index(
        "ix_analysis_receipt_outbox_delivery",
        "analysis_receipt_lake_outbox",
        ["delivery_status", "event_id"],
    )
    op.create_table(
        "analysis_daily_completion",
        sa.Column("completion_id", sa.Uuid(), nullable=False),
        sa.Column("household_id", sa.String(50), nullable=False),
        sa.Column("appliance_type", sa.String(50), nullable=False),
        sa.Column("target_date", sa.Date(), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("input_snapshot_id", sa.String(64), nullable=False),
        sa.Column("analysis_run_id", sa.String(100), nullable=False),
        sa.Column("analysis_evidence_snapshot_id", sa.String(64), nullable=False),
        sa.Column("session_manifest_set_id", sa.String(64), nullable=False),
        sa.Column("analysis_status", sa.String(20), nullable=False),
        sa.Column("delivery_status", sa.String(20), nullable=False),
        sa.Column("coverage_ratio", sa.Numeric(7, 6), nullable=False),
        sa.Column("max_unanalyzed_seconds", sa.BigInteger(), nullable=False),
        sa.Column("quality_policy_version", sa.String(50), nullable=False),
        sa.Column("baseline_eligible", sa.Boolean(), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("revision >= 1", name=op.f("ck_analysis_daily_completion_revision_positive")),
        sa.CheckConstraint("coverage_ratio BETWEEN 0 AND 1", name=op.f("ck_analysis_daily_completion_coverage_ratio")),
        sa.CheckConstraint("max_unanalyzed_seconds >= 0", name=op.f("ck_analysis_daily_completion_max_unanalyzed_seconds_nonnegative")),
        sa.CheckConstraint("analysis_status IN ('COMPLETE', 'INCOMPLETE', 'ERROR', 'UNKNOWN')", name=op.f("ck_analysis_daily_completion_analysis_status")),
        sa.CheckConstraint("delivery_status IN ('COMPLETE', 'PENDING')", name=op.f("ck_analysis_daily_completion_delivery_status")),
        sa.PrimaryKeyConstraint("completion_id", name=op.f("pk_analysis_daily_completion")),
        sa.UniqueConstraint("household_id", "appliance_type", "target_date", "revision", name="uq_analysis_daily_completion_scope_revision"),
    )
    op.create_index("ix_analysis_daily_completion_scope", "analysis_daily_completion", ["target_date", "household_id"])


def downgrade() -> None:
    op.drop_index("ix_analysis_daily_completion_scope", table_name="analysis_daily_completion")
    op.drop_table("analysis_daily_completion")
    op.drop_index("ix_analysis_receipt_outbox_delivery", table_name="analysis_receipt_lake_outbox")
    op.drop_table("analysis_receipt_lake_outbox")
    op.drop_table("analysis_receipt_lake_batch")
    op.drop_index("ix_analysis_receipt_household_measured", table_name="analysis_processing_receipt")
    op.drop_table("analysis_processing_receipt")
