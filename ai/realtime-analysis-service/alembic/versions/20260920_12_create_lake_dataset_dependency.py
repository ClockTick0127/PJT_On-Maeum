"""Record which upstream dataset versions each lake run consumed.

Revision ID: 20260920_12
Revises: 20260920_11
Create Date: 2026-09-20

상위 날짜가 재처리되면 그 날짜를 소비한 하위 결과는 낡은 것이 된다. 알림(큐)만으로는
유실될 수 있으므로, 소비한 상위 버전을 활성화와 같은 트랜잭션에 남겨 "내 활성 결과가 쓴
버전"과 "지금 활성인 버전"을 직접 비교할 수 있게 한다.
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "20260920_12"
down_revision: str | None = "20260920_11"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "lake_dataset_dependency",
        sa.Column(
            "dependency_id",
            sa.Uuid(),
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("consumer_run_id", sa.Uuid(), nullable=False),
        sa.Column("upstream_dataset_name", sa.String(length=64), nullable=False),
        sa.Column("upstream_target_date", sa.Date(), nullable=False),
        # 상위 쪽은 FK로 묶지 않는다. 상위 버전 행이 정리된 뒤에도 기록은 남아야 한다.
        sa.Column("upstream_run_id", sa.Uuid(), nullable=False),
        sa.Column("upstream_version_id", sa.Uuid(), nullable=False),
        sa.Column(
            "recorded_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["consumer_run_id"],
            ["lake_batch_run.run_id"],
            name=op.f("fk_lake_dataset_dependency_consumer_run_id_lake_batch_run"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "dependency_id", name=op.f("pk_lake_dataset_dependency")
        ),
        sa.UniqueConstraint(
            "consumer_run_id",
            "upstream_dataset_name",
            "upstream_target_date",
            name=op.f("uq_lake_dataset_dependency_consumer_upstream"),
        ),
    )
    op.create_index(
        "ix_lake_dataset_dependency_upstream",
        "lake_dataset_dependency",
        ["upstream_dataset_name", "upstream_target_date"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_lake_dataset_dependency_upstream", table_name="lake_dataset_dependency"
    )
    op.drop_table("lake_dataset_dependency")
