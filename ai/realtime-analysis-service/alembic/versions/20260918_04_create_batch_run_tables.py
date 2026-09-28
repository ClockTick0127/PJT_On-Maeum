"""Create persistent batch and retention execution history.

Revision ID: 20260918_04
Revises: 20260918_03
Create Date: 2026-09-18
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "20260918_04"
down_revision: str | None = "20260918_03"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "batch_run",
        sa.Column(
            "run_id",
            sa.Uuid(),
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("pipeline_name", sa.String(length=50), nullable=False),
        sa.Column("target_date", sa.Date(), nullable=False),
        sa.Column("run_version", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("dry_run", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("input_version", sa.String(length=200), nullable=True),
        sa.Column("output_version", sa.String(length=200), nullable=True),
        sa.Column(
            "details",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'SKIPPED')",
            name=op.f("ck_batch_run_status"),
        ),
        sa.CheckConstraint(
            "run_version >= 1",
            name=op.f("ck_batch_run_version_positive"),
        ),
        sa.PrimaryKeyConstraint("run_id", name=op.f("pk_batch_run")),
        sa.UniqueConstraint(
            "pipeline_name",
            "target_date",
            "run_version",
            name=op.f("uq_batch_run_pipeline_date_version"),
        ),
    )
    op.create_index(
        op.f("ix_batch_run_pipeline_date_status"),
        "batch_run",
        ["pipeline_name", "target_date", "status"],
    )

    op.create_table(
        "batch_step_run",
        sa.Column(
            "step_run_id",
            sa.Uuid(),
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("step_name", sa.String(length=50), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("input_count", sa.BigInteger(), nullable=True),
        sa.Column("output_count", sa.BigInteger(), nullable=True),
        sa.Column("input_bytes", sa.BigInteger(), nullable=True),
        sa.Column("output_bytes", sa.BigInteger(), nullable=True),
        sa.Column(
            "details",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'SKIPPED')",
            name=op.f("ck_batch_step_run_status"),
        ),
        sa.CheckConstraint(
            "attempt >= 1",
            name=op.f("ck_batch_step_run_attempt_positive"),
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["batch_run.run_id"],
            name=op.f("fk_batch_step_run_run_id_batch_run"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("step_run_id", name=op.f("pk_batch_step_run")),
        sa.UniqueConstraint(
            "run_id",
            "step_name",
            "attempt",
            name=op.f("uq_batch_step_run_run_step_attempt"),
        ),
    )


def downgrade() -> None:
    op.drop_table("batch_step_run")
    op.drop_index("ix_batch_run_pipeline_date_status", table_name="batch_run")
    op.drop_table("batch_run")
