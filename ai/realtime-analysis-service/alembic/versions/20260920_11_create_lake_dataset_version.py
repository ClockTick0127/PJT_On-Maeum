"""Track lake batch runs and the active dataset version per date.

Revision ID: 20260920_11
Revises: 20260920_10
Create Date: 2026-09-20

``batch_run``은 날짜별 성공 여부만 기록해서 늦게 도착한 데이터나 규칙 변경에 따른 수정
재처리를 표현하지 못한다. ``lake_batch_run``은 입력 스냅샷·규칙·설정 버전을 실행 식별에
넣고, ``lake_dataset_version``은 날짜마다 어떤 실행 결과를 읽어야 하는지를 가리킨다.
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "20260920_11"
down_revision: str | None = "20260920_10"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "lake_batch_run",
        sa.Column(
            "run_id",
            sa.Uuid(),
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("job_name", sa.String(length=50), nullable=False),
        sa.Column("target_date", sa.Date(), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("input_snapshot_id", sa.String(length=64), nullable=True),
        sa.Column("rule_version", sa.String(length=50), nullable=False),
        sa.Column("config_version", sa.String(length=100), nullable=False),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "details",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "status IN ('WAITING_INPUT', 'RUNNING', 'VALIDATING', 'SUCCEEDED', 'FAILED')",
            name=op.f("ck_lake_batch_run_status"),
        ),
        sa.CheckConstraint("attempt >= 1", name=op.f("ck_lake_batch_run_attempt_positive")),
        sa.PrimaryKeyConstraint("run_id", name=op.f("pk_lake_batch_run")),
        sa.UniqueConstraint(
            "job_name",
            "target_date",
            "attempt",
            name=op.f("uq_lake_batch_run_job_date_attempt"),
        ),
    )
    op.create_index(
        "ix_lake_batch_run_job_date_status",
        "lake_batch_run",
        ["job_name", "target_date", "status"],
    )

    op.create_table(
        "lake_dataset_version",
        sa.Column(
            "version_id",
            sa.Uuid(),
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("dataset_name", sa.String(length=64), nullable=False),
        sa.Column("target_date", sa.Date(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("output_path", sa.Text(), nullable=False),
        sa.Column("manifest_path", sa.Text(), nullable=False),
        sa.Column("row_count", sa.BigInteger(), nullable=False),
        sa.Column("input_snapshot_id", sa.String(length=64), nullable=False),
        sa.Column("rule_version", sa.String(length=50), nullable=False),
        sa.Column("config_version", sa.String(length=100), nullable=False),
        sa.Column(
            "published_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "status IN ('ACTIVE', 'SUPERSEDED')",
            name=op.f("ck_lake_dataset_version_status"),
        ),
        sa.CheckConstraint(
            "row_count >= 0",
            name=op.f("ck_lake_dataset_version_row_count_nonnegative"),
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["lake_batch_run.run_id"],
            name=op.f("fk_lake_dataset_version_run_id_lake_batch_run"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("version_id", name=op.f("pk_lake_dataset_version")),
        sa.UniqueConstraint(
            "dataset_name",
            "target_date",
            "run_id",
            name=op.f("uq_lake_dataset_version_dataset_date_run"),
        ),
    )
    # 날짜마다 활성 버전은 하나뿐이다. 후속 집계가 같은 날짜를 두 번 읽는 일을 DB가 막는다.
    op.create_index(
        "ix_lake_dataset_version_active",
        "lake_dataset_version",
        ["dataset_name", "target_date"],
        unique=True,
        postgresql_where=sa.text("status = 'ACTIVE'"),
    )


def downgrade() -> None:
    op.drop_index("ix_lake_dataset_version_active", table_name="lake_dataset_version")
    op.drop_table("lake_dataset_version")
    op.drop_index("ix_lake_batch_run_job_date_status", table_name="lake_batch_run")
    op.drop_table("lake_batch_run")
