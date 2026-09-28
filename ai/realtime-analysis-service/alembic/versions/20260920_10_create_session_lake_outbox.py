"""Capture appliance usage session changes for the HDFS session lake.

Revision ID: 20260920_10
Revises: 20260919_09
Create Date: 2026-09-20

Adds ``appliance_usage_session.lake_version``, the ``session_lake_outbox``
change table, the ``session_lake_batch`` load history table, and the
PostgreSQL trigger that writes an outbox row inside the same transaction as
every session change. The trigger is defined only here as SQL so that the ORM
models stay database-neutral for the SQLite regression tests.
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "20260920_10"
down_revision: str | None = "20260919_09"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


TRIGGER_FUNCTION = "appliance_usage_session_capture_lake_change"
TRIGGER_NAME = "trg_appliance_usage_session_lake_outbox"

# 세션 변경 포착 규칙
#   INSERT : lake_version을 1로 고정하고 전체 내용을 INSERT 이벤트로 기록한다.
#   UPDATE : lake_version을 제외한 모든 컬럼이 같으면 이벤트를 만들지 않고 버전도
#            유지한다. 하나라도 다르면 버전을 1 올리고 전체 내용을 기록한다.
#   DELETE : 직접 삭제와 부모 CASCADE 삭제 모두 OLD만 사용해 (마지막 버전 + 1)의
#            DELETE 이벤트를 만든다. 부모 행은 이미 없을 수 있어 조인하지 않는다.
# INSERT/UPDATE 이벤트에는 가구·가전·관측일을 부모에서 조인해 함께 담는다.
# 부모 FK가 보장되므로 조인 결과는 항상 존재한다. 이벤트 행은 세션 변경과 같은
# 트랜잭션에 속하므로 롤백되면 함께 사라진다.
CREATE_TRIGGER_FUNCTION = f"""
CREATE OR REPLACE FUNCTION {TRIGGER_FUNCTION}() RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    parent_household_id varchar(50);
    parent_appliance_type varchar(50);
    parent_observation_date date;
    content jsonb;
BEGIN
    IF TG_OP = 'DELETE' THEN
        INSERT INTO session_lake_outbox (session_id, session_version, operation, payload)
        VALUES (
            OLD.id,
            OLD.lake_version + 1,
            'DELETE',
            to_jsonb(OLD) - 'lake_version'
        );
        RETURN OLD;
    END IF;

    IF TG_OP = 'INSERT' THEN
        NEW.lake_version := 1;
    ELSE
        IF (to_jsonb(NEW) - 'lake_version') = (to_jsonb(OLD) - 'lake_version') THEN
            NEW.lake_version := OLD.lake_version;
            RETURN NEW;
        END IF;
        NEW.lake_version := OLD.lake_version + 1;
    END IF;

    SELECT o.household_id, a.appliance_type, o.observation_date
      INTO parent_household_id, parent_appliance_type, parent_observation_date
      FROM household_activity_daily AS a
      JOIN household_observation_daily AS o ON o.id = a.observation_daily_id
     WHERE a.id = NEW.activity_daily_id;

    content := (to_jsonb(NEW) - 'lake_version')
        || jsonb_build_object(
            'household_id', parent_household_id,
            'appliance_type', parent_appliance_type,
            'observation_date', parent_observation_date
        );

    INSERT INTO session_lake_outbox (session_id, session_version, operation, payload)
    VALUES (NEW.id, NEW.lake_version, TG_OP, content);
    RETURN NEW;
END;
$$;
"""

CREATE_TRIGGER = f"""
CREATE TRIGGER {TRIGGER_NAME}
BEFORE INSERT OR UPDATE OR DELETE ON appliance_usage_session
FOR EACH ROW EXECUTE FUNCTION {TRIGGER_FUNCTION}();
"""


def upgrade() -> None:
    # 기존 행은 버전 1에서 시작한다. 기존 행의 INSERT 이벤트는 만들지 않으며
    # 초기 전체 적재가 현재 상태를 레이크로 옮긴다.
    op.add_column(
        "appliance_usage_session",
        sa.Column(
            "lake_version",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("1"),
        ),
    )
    op.create_check_constraint(
        op.f("ck_appliance_usage_session_lake_version_positive"),
        "appliance_usage_session",
        "lake_version >= 1",
    )

    op.create_table(
        "session_lake_batch",
        sa.Column(
            "batch_id",
            sa.Uuid(),
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("batch_kind", sa.String(length=20), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("ingest_date", sa.Date(), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column(
            "event_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("first_event_id", sa.BigInteger(), nullable=True),
        sa.Column("last_event_id", sa.BigInteger(), nullable=True),
        sa.Column("row_count", sa.BigInteger(), nullable=True),
        sa.Column("file_count", sa.Integer(), nullable=True),
        sa.Column("manifest_path", sa.String(length=500), nullable=False),
        sa.Column(
            "attempt_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("last_error_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "details",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "batch_kind IN ('INITIAL', 'INCREMENTAL')",
            name=op.f("ck_session_lake_batch_batch_kind"),
        ),
        sa.CheckConstraint(
            "status IN ('ASSIGNED', 'FAILED', 'COMPLETED')",
            name=op.f("ck_session_lake_batch_status"),
        ),
        sa.CheckConstraint(
            "event_count >= 0",
            name=op.f("ck_session_lake_batch_event_count_nonnegative"),
        ),
        sa.CheckConstraint(
            "attempt_count >= 0",
            name=op.f("ck_session_lake_batch_attempt_count_nonnegative"),
        ),
        sa.CheckConstraint(
            "schema_version >= 1",
            name=op.f("ck_session_lake_batch_schema_version_positive"),
        ),
        sa.PrimaryKeyConstraint("batch_id", name=op.f("pk_session_lake_batch")),
    )
    op.create_index(
        "ix_session_lake_batch_status_created",
        "session_lake_batch",
        ["status", "created_at"],
    )

    op.create_table(
        "session_lake_outbox",
        sa.Column("event_id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("session_id", sa.Uuid(), nullable=False),
        sa.Column("session_version", sa.Integer(), nullable=False),
        sa.Column("operation", sa.String(length=10), nullable=False),
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "changed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "delivery_status",
            sa.String(length=20),
            nullable=False,
            server_default=sa.text("'PENDING'"),
        ),
        sa.Column("batch_id", sa.Uuid(), nullable=True),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "operation IN ('INSERT', 'UPDATE', 'DELETE')",
            name=op.f("ck_session_lake_outbox_operation"),
        ),
        sa.CheckConstraint(
            "delivery_status IN ('PENDING', 'ASSIGNED', 'DELIVERED')",
            name=op.f("ck_session_lake_outbox_delivery_status"),
        ),
        sa.CheckConstraint(
            "session_version >= 1",
            name=op.f("ck_session_lake_outbox_session_version_positive"),
        ),
        sa.CheckConstraint(
            "(delivery_status = 'PENDING') = (batch_id IS NULL)",
            name=op.f("ck_session_lake_outbox_batch_assigned_matches_status"),
        ),
        # 원본 세션에 대한 FK는 의도적으로 없다. 배치 FK만 둔다.
        sa.ForeignKeyConstraint(
            ["batch_id"],
            ["session_lake_batch.batch_id"],
            name=op.f("fk_session_lake_outbox_batch_id_session_lake_batch"),
        ),
        sa.PrimaryKeyConstraint("event_id", name=op.f("pk_session_lake_outbox")),
        sa.UniqueConstraint(
            "session_id",
            "session_version",
            name=op.f("uq_session_lake_outbox_session_version"),
        ),
    )
    op.create_index(
        "ix_session_lake_outbox_undelivered",
        "session_lake_outbox",
        ["delivery_status", "event_id"],
        postgresql_where=sa.text("delivery_status <> 'DELIVERED'"),
    )
    op.create_index(
        "ix_session_lake_outbox_batch_id",
        "session_lake_outbox",
        ["batch_id"],
    )

    op.execute(CREATE_TRIGGER_FUNCTION)
    op.execute(CREATE_TRIGGER)


def downgrade() -> None:
    op.execute(f"DROP TRIGGER IF EXISTS {TRIGGER_NAME} ON appliance_usage_session")
    op.execute(f"DROP FUNCTION IF EXISTS {TRIGGER_FUNCTION}()")
    op.drop_index("ix_session_lake_outbox_batch_id", table_name="session_lake_outbox")
    op.drop_index("ix_session_lake_outbox_undelivered", table_name="session_lake_outbox")
    op.drop_table("session_lake_outbox")
    op.drop_index("ix_session_lake_batch_status_created", table_name="session_lake_batch")
    op.drop_table("session_lake_batch")
    op.drop_constraint(
        op.f("ck_appliance_usage_session_lake_version_positive"),
        "appliance_usage_session",
        type_="check",
    )
    op.drop_column("appliance_usage_session", "lake_version")
