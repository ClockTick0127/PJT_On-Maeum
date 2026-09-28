from contextlib import nullcontext
from datetime import date, datetime, timezone
import json
from types import SimpleNamespace

import pytest

from household_report.daily import ASSESSMENT_SCHEMA, export_live_assessments, prepare_daily_input
from household_report.input_snapshot import load_report_input
from household_report.job import build_report
from power_silver.storage import LocalLakeStorage

pytestmark = pytest.mark.spark


class SnapshotConnection:
    def __init__(self, rows):
        self.rows = rows
        self.sql = []
    def __enter__(self):
        return self
    def __exit__(self, *args):
        return False
    def execution_options(self, **options):
        return self
    def begin(self):
        return nullcontext()
    def execute(self, sql, params=None):
        self.sql.append(str(sql))
        return self
    def mappings(self):
        return self
    def partitions(self, size):
        yield from ([self.rows] if self.rows else [])


def test_live_export_reuses_content_and_preserves_empty_schema(spark, tmp_path):
    storage = LocalLakeStorage(tmp_path)
    conn = SnapshotConnection([])
    engine = SimpleNamespace(connect=lambda:conn)
    args = dict(spark=spark, storage=storage, start=date(2026,1,1), end=date(2026,1,1), base='/exports')
    first = export_live_assessments(engine, **args)
    assert spark.read.parquet(storage.uri(first['paths'][0])).count() == 0
    conn.rows = [dict(assessment_id='a1', household_id='h1', assessed_at=datetime(2026,1,1,tzinfo=timezone.utc),
        assessment_status='PARTIAL', risk_score=50, risk_level='CAUTION', profile_version=None,
        policy_version='p1', score_version='s1', indicators='[]', assessment_mode='LIVE_RECORDED')]
    second = export_live_assessments(engine, **args)
    assert first['version_id'] != second['version_id']
    assert second == export_live_assessments(engine, **args)
    assert 'SET TRANSACTION READ ONLY' in conn.sql


def test_committed_inputs_build_all_dashboard_datasets(spark, tmp_path):
    storage = LocalLakeStorage(tmp_path)
    usage = spark.createDataFrame([('h1',date(2026,1,1),'IRON','USED',True,1,60000000)],
        'household_id string, usage_date date, appliance_type string, usage_status string, baseline_eligible boolean, usage_start_count long, usage_duration_us long')
    usage.write.parquet(storage.uri('/usage'))
    for name in ['baseline','statistics']:
        spark.createDataFrame([('h1','gold1')], 'household_id string, profile_version string').write.parquet(storage.uri('/'+name))
    spark.createDataFrame([], ASSESSMENT_SCHEMA).write.parquet(storage.uri('/assessments'))
    historical = {'assessment_mode':'EVENT_TIME_REASSESSMENT', 'assessment_delivery_complete':True,
        'assessment_cutoff':'2026-01-02T00:00:00+09:00',
        'sources':{'assessments':{'version_id':'history1', 'paths':['/assessments']}}}
    storage.write_bytes('/historical.json', json.dumps(historical).encode())
    refs = {name:SimpleNamespace(run_id='gold1', version_id=name, output_path='/'+path)
        for name,path in [('routine_baseline_history','baseline'),('household_statistical_profile','statistics')]}
    catalog = SimpleNamespace(active_version=lambda name,day:refs[name],
        active_versions=lambda name,days:{date(2026,1,1):SimpleNamespace(version_id='u1', output_path='/usage')})
    targets = SimpleNamespace(fingerprint='t1', household_ids_for_date=lambda day,offset:('h1',))
    kwargs = dict(engine=None, spark=spark, storage=storage, catalog=catalog, targets=targets,
        end=date(2026,1,1), window_days=2, historical_manifest='/historical.json')
    path = prepare_daily_input(**kwargs)
    assert path == prepare_daily_input(**kwargs)
    inputs = load_report_input(storage, path)
    assert inputs.document['missing_usage_dates'] == ['2025-12-31']
    manifest_path = build_report(inputs, spark=spark, storage=storage, partitions=1)
    manifest = json.loads(storage.read_bytes(manifest_path))
    assert manifest['assessment_mode'] == 'EVENT_TIME_REASSESSMENT'
    assert manifest['outputs']['household_daily_trends']['row_count'] == 2
    assert manifest['outputs']['appliance_daily_trends']['row_count'] == 2
    assert manifest['outputs']['report_summary']['row_count'] == 1
    assert manifest['outputs']['assessment_detail']['row_count'] == 0
    refs['household_statistical_profile'].run_id = 'other-gold'
    with pytest.raises(ValueError, match='matching committed Gold'):
        prepare_daily_input(**kwargs)
