"""Create the initial analysis_db schema.

Revision ID: 20260909_01
Revises:
Create Date: 2026-09-09
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "20260909_01"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

APPLIANCE_TYPES = (
    "KETTLE",
    "INDUCTION",
    "IRON",
    "MICROWAVE",
    "HAIR_DRYER",
    "VACUUM_CLEANER",
)
APPLIANCE_TYPE_SQL = ", ".join(f"'{item}'" for item in APPLIANCE_TYPES)


def uuid_column() -> sa.Column:
    return sa.Column(
        "id",
        sa.Uuid(),
        nullable=False,
        server_default=sa.text("gen_random_uuid()"),
    )


def upgrade() -> None:
    op.create_table(
        "model_artifact",
        uuid_column(),
        sa.Column("model_name", sa.String(length=100), nullable=False),
        sa.Column("version", sa.String(length=50), nullable=False),
        sa.Column("artifact_uri", sa.String(length=500), nullable=False),
        sa.Column("manifest_uri", sa.String(length=500), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "status IN ('VALIDATED', 'ACTIVE', 'RETIRED')",
            name=op.f("ck_model_artifact_status"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_model_artifact")),
        sa.UniqueConstraint(
            "model_name",
            "version",
            name=op.f("uq_model_artifact_name_version"),
        ),
    )
    op.create_index(
        op.f("uq_model_artifact_single_active"),
        "model_artifact",
        ["status"],
        unique=True,
        postgresql_where=sa.text("status = 'ACTIVE'"),
    )

    op.create_table(
        "routine_baseline",
        uuid_column(),
        sa.Column("household_id", sa.String(length=50), nullable=False),
        sa.Column("appliance_type", sa.String(length=50), nullable=False),
        sa.Column("baseline_type", sa.String(length=30), nullable=False),
        sa.Column("sample_days", sa.SmallInteger(), nullable=False),
        sa.Column("active_days", sa.SmallInteger(), nullable=False),
        sa.Column("daily_use_probability", sa.Numeric(5, 4), nullable=False),
        sa.Column("reliability_weight", sa.Numeric(5, 4), nullable=False),
        sa.Column("baseline_data", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("calculated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            f"appliance_type IN ({APPLIANCE_TYPE_SQL})",
            name=op.f("ck_routine_baseline_appliance_type"),
        ),
        sa.CheckConstraint(
            "sample_days >= 0",
            name=op.f("ck_routine_baseline_sample_days_nonnegative"),
        ),
        sa.CheckConstraint(
            "active_days >= 0",
            name=op.f("ck_routine_baseline_active_days_nonnegative"),
        ),
        sa.CheckConstraint(
            "active_days <= sample_days",
            name=op.f("ck_routine_baseline_active_days_lte_sample_days"),
        ),
        sa.CheckConstraint(
            "daily_use_probability BETWEEN 0 AND 1",
            name=op.f("ck_routine_baseline_daily_use_probability"),
        ),
        sa.CheckConstraint(
            "reliability_weight BETWEEN 0 AND 1",
            name=op.f("ck_routine_baseline_reliability_weight"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_routine_baseline")),
        sa.UniqueConstraint(
            "household_id",
            "appliance_type",
            "baseline_type",
            name=op.f("uq_routine_baseline_household_appliance_type"),
        ),
    )

    op.create_table(
        "analysis_policy",
        uuid_column(),
        sa.Column("policy_code", sa.String(length=50), nullable=False),
        sa.Column("algorithm_type", sa.String(length=50), nullable=False),
        sa.Column("parameters", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("event_type", sa.String(length=50), nullable=False),
        sa.Column("score", sa.SmallInteger(), nullable=False),
        sa.Column("severity", sa.String(length=20), nullable=False),
        sa.Column("cooldown_hours", sa.SmallInteger(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "score BETWEEN 0 AND 100",
            name=op.f("ck_analysis_policy_score"),
        ),
        sa.CheckConstraint(
            "cooldown_hours >= 0",
            name=op.f("ck_analysis_policy_cooldown_hours_nonnegative"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_analysis_policy")),
        sa.UniqueConstraint("policy_code", name=op.f("uq_analysis_policy_code")),
    )

    op.create_table(
        "household_observation_daily",
        uuid_column(),
        sa.Column("household_id", sa.String(length=50), nullable=False),
        sa.Column("observation_date", sa.Date(), nullable=False),
        sa.Column("sample_count", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("expected_sample_count", sa.BigInteger(), nullable=False),
        sa.Column("coverage_ratio", sa.Numeric(5, 4), nullable=False),
        sa.Column("observation_status", sa.String(length=30), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "sample_count >= 0",
            name=op.f("ck_household_observation_daily_sample_count_nonnegative"),
        ),
        sa.CheckConstraint(
            "expected_sample_count >= 0",
            name=op.f(
                "ck_household_observation_daily_expected_sample_count_nonnegative"
            ),
        ),
        sa.CheckConstraint(
            "coverage_ratio BETWEEN 0 AND 1",
            name=op.f("ck_household_observation_daily_coverage_ratio"),
        ),
        sa.CheckConstraint(
            "observation_status IN "
            "('COLLECTING', 'VALID', 'INSUFFICIENT_DATA', 'SENSOR_GAP', "
            "'PROCESSING_ERROR', 'EXCLUDED')",
            name=op.f("ck_household_observation_daily_observation_status"),
        ),
        sa.PrimaryKeyConstraint(
            "id",
            name=op.f("pk_household_observation_daily"),
        ),
        sa.UniqueConstraint(
            "household_id",
            "observation_date",
            name=op.f("uq_household_observation_daily_household_date"),
        ),
    )

    op.create_table(
        "household_activity_daily",
        uuid_column(),
        sa.Column("observation_daily_id", sa.Uuid(), nullable=False),
        sa.Column("appliance_type", sa.String(length=50), nullable=False),
        sa.Column("event_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            f"appliance_type IN ({APPLIANCE_TYPE_SQL})",
            name=op.f("ck_household_activity_daily_appliance_type"),
        ),
        sa.CheckConstraint(
            "event_count >= 0",
            name=op.f("ck_household_activity_daily_event_count_nonnegative"),
        ),
        sa.ForeignKeyConstraint(
            ["observation_daily_id"],
            ["household_observation_daily.id"],
            name=op.f(
                "fk_household_activity_daily_observation_daily_id_"
                "household_observation_daily"
            ),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_household_activity_daily")),
        sa.UniqueConstraint(
            "observation_daily_id",
            "appliance_type",
            name=op.f("uq_household_activity_daily_observation_appliance"),
        ),
    )

    op.create_table(
        "appliance_usage_session",
        uuid_column(),
        sa.Column("activity_daily_id", sa.Uuid(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("max_probability", sa.Numeric(5, 4), nullable=False),
        sa.Column("decision_threshold", sa.Numeric(5, 4), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "ended_at IS NULL OR ended_at >= started_at",
            name=op.f("ck_appliance_usage_session_valid_time_range"),
        ),
        sa.CheckConstraint(
            "max_probability BETWEEN 0 AND 1",
            name=op.f("ck_appliance_usage_session_max_probability"),
        ),
        sa.CheckConstraint(
            "decision_threshold BETWEEN 0 AND 1",
            name=op.f("ck_appliance_usage_session_decision_threshold"),
        ),
        sa.ForeignKeyConstraint(
            ["activity_daily_id"],
            ["household_activity_daily.id"],
            name=op.f(
                "fk_appliance_usage_session_activity_daily_id_household_activity_daily"
            ),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_appliance_usage_session")),
        sa.UniqueConstraint(
            "activity_daily_id",
            "started_at",
            name=op.f("uq_appliance_usage_session_activity_started"),
        ),
    )


def downgrade() -> None:
    op.drop_table("appliance_usage_session")
    op.drop_table("household_activity_daily")
    op.drop_table("household_observation_daily")
    op.drop_table("analysis_policy")
    op.drop_table("routine_baseline")
    op.drop_index("uq_model_artifact_single_active", table_name="model_artifact")
    op.drop_table("model_artifact")
