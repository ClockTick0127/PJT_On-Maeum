from realtime_analysis.database import Base
from realtime_analysis import models  # noqa: F401
from realtime_analysis import scene_pipeline  # noqa: F401
from realtime_analysis import scene_events  # noqa: F401


def test_analysis_schema_contains_expected_tables() -> None:
    assert set(Base.metadata.tables) == {
        "model_artifact",
        "selected_scene_evidence",
        "selected_scene_projection",
        "selected_scene_usage",
        "selected_scene_outbox",
        "routine_baseline",
        "analysis_policy",
        "analysis_event_emission",
        "household_outing_state",
        "household_observation_daily",
        "household_activity_daily",
        "appliance_usage_session",
        "session_lake_batch",
        "session_lake_outbox",
        "lake_batch_run",
        "lake_dataset_version",
        "lake_dataset_dependency",
        "analysis_processing_receipt",
        "analysis_receipt_lake_outbox",
        "analysis_receipt_lake_batch",
        "analysis_daily_completion",
    }


def test_usage_session_keeps_probability_decision_evidence() -> None:
    columns = Base.metadata.tables["appliance_usage_session"].columns

    assert "appliance_type" not in columns
    assert "model_artifact_id" not in columns
    assert "max_probability" in columns
    assert "decision_threshold" in columns
    assert "created_at" not in columns
    assert "updated_at" in columns
    assert columns["ended_at"].nullable is True


def test_usage_session_lake_version_starts_at_one_by_default() -> None:
    table = Base.metadata.tables["appliance_usage_session"]

    column = table.columns["lake_version"]
    assert column.nullable is False
    assert column.server_default is not None
    assert str(column.server_default.arg) == "1"
    assert "ck_appliance_usage_session_lake_version_positive" in {
        constraint.name for constraint in table.constraints
    }


def test_session_lake_outbox_has_no_foreign_key_to_sessions() -> None:
    table = Base.metadata.tables["session_lake_outbox"]

    referred_tables = {
        foreign_key.column.table.name for foreign_key in table.foreign_keys
    }
    assert referred_tables == {"session_lake_batch"}
    assert list(table.primary_key.columns)[0].name == "event_id"
    assert {
        "event_id",
        "session_id",
        "session_version",
        "operation",
        "payload",
        "changed_at",
        "delivery_status",
        "batch_id",
        "delivered_at",
    } == set(table.columns.keys())
    assert "uq_session_lake_outbox_session_version" in {
        constraint.name for constraint in table.constraints
    }
    assert "ix_session_lake_outbox_undelivered" in {
        index.name for index in table.indexes
    }


def test_session_lake_batch_tracks_target_progress_manifest_and_errors() -> None:
    table = Base.metadata.tables["session_lake_batch"]

    assert list(table.primary_key.columns)[0].name == "batch_id"
    for column_name in (
        "batch_kind",
        "status",
        "ingest_date",
        "event_count",
        "first_event_id",
        "last_event_id",
        "manifest_path",
        "attempt_count",
        "last_error",
        "last_error_at",
        "details",
    ):
        assert column_name in table.columns
    assert table.columns["manifest_path"].nullable is False
    assert table.columns["last_error"].nullable is True


def test_event_emission_schema_supports_cooldown_lookup() -> None:
    table = Base.metadata.tables["analysis_event_emission"]

    assert set(table.columns.keys()) == {
        "event_id",
        "household_id",
        "event_type",
        "appliance_type",
        "emitted_at",
    }
    assert list(table.primary_key.columns)[0].name == "event_id"
    assert table.columns["appliance_type"].nullable is True
    assert "ix_analysis_event_emission_cooldown" in {
        index.name for index in table.indexes
    }


def test_outing_schema_stores_only_latest_household_state() -> None:
    state_table = Base.metadata.tables["household_outing_state"]

    assert list(state_table.primary_key.columns)[0].name == "household_id"
    assert {
        "household_id",
        "is_outing",
        "outing_started_at",
        "last_returned_at",
        "last_event_id",
        "last_event_at",
        "updated_at",
    } == set(state_table.columns.keys())
