from datetime import date, datetime, timedelta, timezone

import pytest
import gold_profile.job as job_module
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from realtime_analysis.database import Base
from power_silver.catalog import SilverCatalog
from power_silver.commit import SilverCommitRepository
from power_silver.constants import DATASET_APPLIANCE_USAGE_DAILY, DATASET_SESSION_SLICES
from power_silver.storage import LocalLakeStorage

from gold_profile.config import GoldProfileSettings
from gold_profile.delivery import GoldProfileDeliveryOutbox
from gold_profile.job import (
    DATASET_LOGICAL_USES, DATASET_ROUTINE_BASELINE, DATASET_STATISTICAL_PROFILE,
    run_gold_profile,
)
from gold_profile.input_snapshot import build_profile_snapshot


def _publish_input_day(spark, storage, session_factory, target_date, *, used):
    repository = SilverCommitRepository(session_factory, job_name="analysis-usage-daily")
    handle = repository.start(
        target_date, input_snapshot_id=f"power-{target_date}",
        rule_version="analysis-v1", config_version="receipts=test;sessions=snapshot-1",
    )
    run_id = str(handle.run_id)
    usage_path = f"/nilm/gold/appliance_usage_daily/usage_date={target_date}/run_id={run_id}"
    slice_path = f"/nilm/silver/appliance_session_daily_slices/usage_date={target_date}/run_id={run_id}"
    usage = spark.createDataFrame(
        [(
            target_date, "H001", "KETTLE", "USED" if used else "NOT_USED",
            used, True, 900 if used else None, 1 if used else 0, "COMPLETE", "COMPLETE",
        )],
        "usage_date date, household_id string, appliance_type string, usage_status string, "
        "is_used boolean, baseline_eligible boolean, first_use_second int, "
        "usage_start_count long, analysis_status string, delivery_status string",
    )
    if used:
        start = datetime.combine(target_date, datetime.min.time(), timezone(timedelta(hours=9))).astimezone(timezone.utc) + timedelta(minutes=15)
        # One session id per date: the same id with different content across
        # dates is a conflicting revision and is rejected by the logical-use step.
        slice_rows = [(
            f"session-{target_date.isoformat()}", 1, "H001", "KETTLE",
            start, start + timedelta(minutes=1), False,
        )]
    else:
        slice_rows = []
    slices = spark.createDataFrame(
        slice_rows,
        "session_id string, session_version int, household_id string, appliance_type string, "
        "original_started_at timestamp, original_ended_at timestamp, end_imputed boolean",
    )
    usage.coalesce(1).write.mode("overwrite").parquet(storage.uri(usage_path))
    slices.coalesce(1).write.mode("overwrite").parquet(storage.uri(slice_path))
    manifest = {
        "job": "analysis-usage-daily", "run_id": run_id, "attempt": handle.attempt,
        "target_date": target_date.isoformat(), "input_snapshot_id": f"power-{target_date}",
        "rule_version": "analysis-v1", "config_version": "receipts=test;sessions=snapshot-1",
        "manifest_path": f"/input-manifest/{run_id}.json",
        "outputs": {
            DATASET_APPLIANCE_USAGE_DAILY: {"path": usage_path, "row_count": 1},
            DATASET_SESSION_SLICES: {"path": slice_path, "row_count": len(slice_rows)},
        },
    }
    repository.publish(manifest)


@pytest.mark.spark
def test_job_publishes_one_atomic_shadow_profile(spark, tmp_path):
    storage = LocalLakeStorage(tmp_path / "lake")
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'analysis.db'}")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    as_of = date(2026, 9, 19)
    _publish_input_day(spark, storage, session_factory, as_of - timedelta(days=1), used=True)
    _publish_input_day(spark, storage, session_factory, as_of, used=False)
    settings = GoldProfileSettings(
        lake_local_root=str(tmp_path / "lake"), profile_window_days=2,
        profile_minimum_sample_days=1, profile_minimum_weekday_sample_days=1,
        profile_delivery_mode="ACTIVE",
        spark_master="local[2]",
    )

    result = run_gold_profile(
        settings, as_of, storage=storage, session_factory=session_factory, spark=spark,
        now=datetime(2026, 9, 20, 0, 30, tzinfo=timezone.utc),
    )
    assert result.status == "SUCCEEDED"
    assert result.incomplete is False
    catalog = SilverCommitRepository(session_factory, job_name="gold-profile")
    refs = [
        catalog.active_version(name, as_of)
        for name in (DATASET_ROUTINE_BASELINE, DATASET_LOGICAL_USES, DATASET_STATISTICAL_PROFILE)
    ]
    assert all(ref is not None for ref in refs)
    assert len({ref.run_id for ref in refs}) == 1
    baseline = spark.read.parquet(storage.uri(refs[0].output_path))
    overall = baseline.filter("baseline_scope = 'OVERALL'").collect()[0]
    assert overall.sample_days == 2
    assert overall.active_days == 1
    assert str(overall.daily_use_probability) == "0.5000"
    assert overall.enabled is False
    with session_factory() as session:
        deliveries = session.query(GoldProfileDeliveryOutbox).all()
        assert len(deliveries) == 1
        assert deliveries[0].status == "PENDING"
        assert deliveries[0].payload["household_id"] == "H001"
        assert deliveries[0].payload["profile_version"] == result.run_id
        assert deliveries[0].payload["profile_revision"] == 1
        assert deliveries[0].payload["delivery_mode"] == "ACTIVE"
        assert {row["appliance_type"] for row in deliveries[0].payload["routine_baselines"]} == {"KETTLE"}
        assert {row["metric_name"] for row in deliveries[0].payload["statistics"]}

    repeated = run_gold_profile(
        settings, as_of, storage=storage, session_factory=session_factory, spark=spark,
    )
    assert repeated.reused_run_id == result.run_id
    with session_factory() as session:
        assert session.query(GoldProfileDeliveryOutbox).count() == 1


@pytest.mark.spark
def test_shadow_run_is_rebuilt_as_a_new_active_revision(spark, tmp_path):
    storage = LocalLakeStorage(tmp_path / "lake")
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'analysis.db'}")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    as_of = date(2026, 9, 19)
    for day in (as_of - timedelta(days=1), as_of):
        _publish_input_day(spark, storage, session_factory, day, used=True)
    shadow_settings = GoldProfileSettings(
        lake_local_root=str(tmp_path / "lake"), profile_window_days=2,
        profile_minimum_sample_days=1, profile_minimum_weekday_sample_days=1,
        profile_delivery_mode="SHADOW", spark_master="local[2]",
    )

    shadow = run_gold_profile(
        shadow_settings, as_of, storage=storage,
        session_factory=session_factory, spark=spark,
        now=datetime(2026, 9, 20, 0, 30, tzinfo=timezone.utc),
    )
    with session_factory.begin() as session:
        shadow_delivery = session.query(GoldProfileDeliveryOutbox).one()
        shadow_delivery.status = "PUBLISHED"
        shadow_delivery.published_at = datetime(2026, 9, 20, 0, 31, tzinfo=timezone.utc)

    active_settings = shadow_settings.model_copy(
        update={"profile_delivery_mode": "ACTIVE"}
    )
    active = run_gold_profile(
        active_settings, as_of, storage=storage,
        session_factory=session_factory, spark=spark,
        now=datetime(2026, 9, 20, 1, 0, tzinfo=timezone.utc),
    )
    retried = run_gold_profile(
        active_settings, as_of, storage=storage,
        session_factory=session_factory, spark=spark,
    )

    assert shadow.run_id is not None
    assert active.run_id is not None
    assert active.run_id != shadow.run_id
    assert retried.reused_run_id == active.run_id
    with session_factory() as session:
        deliveries = session.query(GoldProfileDeliveryOutbox).order_by(
            GoldProfileDeliveryOutbox.profile_revision
        ).all()
        assert [(row.delivery_mode, row.profile_revision, row.status) for row in deliveries] == [
            ("SHADOW", 1, "PUBLISHED"),
            ("ACTIVE", 2, "PENDING"),
        ]
        assert deliveries[0].profile_version == shadow.run_id
        assert deliveries[1].profile_version == active.run_id


@pytest.mark.spark
def test_failed_activation_and_outbox_transaction_is_repaired(spark, tmp_path, monkeypatch):
    storage = LocalLakeStorage(tmp_path / "lake")
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'analysis.db'}")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    as_of = date(2026, 9, 19)
    for day in (as_of - timedelta(days=1), as_of):
        _publish_input_day(spark, storage, session_factory, day, used=True)
    settings = GoldProfileSettings(
        lake_local_root=str(tmp_path / "lake"), profile_window_days=2,
        profile_minimum_sample_days=1, profile_minimum_weekday_sample_days=1,
        profile_delivery_mode="ACTIVE", spark_master="local[2]",
    )
    original_enqueue = job_module.enqueue_payloads

    def fail_enqueue(*_args, **_kwargs):
        raise RuntimeError("simulated outbox failure")

    monkeypatch.setattr(job_module, "enqueue_payloads", fail_enqueue)
    with pytest.raises(RuntimeError, match="simulated outbox failure"):
        run_gold_profile(
            settings, as_of, storage=storage,
            session_factory=session_factory, spark=spark,
        )
    with session_factory() as session:
        assert session.query(GoldProfileDeliveryOutbox).count() == 0

    monkeypatch.setattr(job_module, "enqueue_payloads", original_enqueue)
    repaired = run_gold_profile(
        settings, as_of, storage=storage,
        session_factory=session_factory, spark=spark,
    )

    assert repaired.reused_run_id is not None
    with session_factory() as session:
        delivery = session.query(GoldProfileDeliveryOutbox).one()
        assert delivery.profile_version == repaired.reused_run_id
        assert delivery.delivery_mode == "ACTIVE"


@pytest.mark.spark
def test_gold_rejects_when_active_inputs_change_after_selection(spark, tmp_path):
    storage = LocalLakeStorage(tmp_path / "lake")
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'analysis.db'}")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    as_of = date(2026, 9, 19)
    days = (as_of - timedelta(days=1), as_of)
    for day in days:
        _publish_input_day(spark, storage, session_factory, day, used=True)
    settings = GoldProfileSettings(
        lake_local_root=str(tmp_path / "lake"), profile_window_days=2,
        profile_minimum_sample_days=1, profile_minimum_weekday_sample_days=1,
        spark_master="local[2]",
    )
    catalog = SilverCatalog(session_factory)
    selected = build_profile_snapshot(
        as_of, 2,
        catalog.active_versions(DATASET_APPLIANCE_USAGE_DAILY, days),
        catalog.active_versions(DATASET_SESSION_SLICES, days),
        rule_version=settings.profile_rule_version,
        statistic_rule_version=settings.profile_statistic_rule_version,
        analysis_run_id=settings.analysis_run_id,
        timezone_name=f"UTC{settings.business_utc_offset_seconds:+d}s",
    )

    _publish_input_day(spark, storage, session_factory, days[0], used=False)

    with pytest.raises(RuntimeError, match="active analysis versions changed"):
        run_gold_profile(
            settings, as_of, storage=storage, session_factory=session_factory,
            spark=spark, input_snapshot=selected,
        )


@pytest.mark.spark
def test_gold_result_preserves_input_incomplete(spark, tmp_path):
    storage = LocalLakeStorage(tmp_path / "lake")
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'analysis.db'}")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    as_of = date(2026, 9, 19)
    _publish_input_day(spark, storage, session_factory, as_of, used=True)
    settings = GoldProfileSettings(
        lake_local_root=str(tmp_path / "lake"), profile_window_days=2,
        profile_minimum_sample_days=1, profile_minimum_weekday_sample_days=1,
        spark_master="local[2]",
    )

    result = run_gold_profile(
        settings, as_of, storage=storage, session_factory=session_factory, spark=spark,
    )

    assert result.status == "SUCCEEDED"
    assert result.incomplete is True
    assert result.run_id is not None
