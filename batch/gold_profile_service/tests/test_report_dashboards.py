import json
from pathlib import Path
import re

import pytest

def _repo_root():
    for parent in Path(__file__).resolve().parents:
        if (parent / 'infrastructure').is_dir():
            return parent
    return None


ROOT = _repo_root()
# The Docker test stage copies only tests/ to /app, so repo-level files are absent there.
pytestmark = pytest.mark.skipif(ROOT is None, reason='repository checkout not available')


def test_dashboard_queries_parse_and_use_fixed_snapshot():
    parse_sql = pytest.importorskip('pglast').parse_sql
    schema = ROOT / 'batch/gold_profile_service/reporting_schema.sql'
    parse_sql(schema.read_text(encoding='utf-8'))
    for file in (ROOT / 'infrastructure/observability/grafana/dashboards').glob('household-daily-*.json'):
        dashboard = json.loads(file.read_text(encoding='utf-8'))
        assert dashboard['timezone'] == 'Asia/Seoul'
        for panel in dashboard['panels']:
            assert panel['datasource']['uid'] == 'nilm-reporting-postgres'
            for target in panel['targets']:
                sql = target['rawSql']
                assert '${report_id:sqlstring}' in sql
                assert '${household:sqlstring}' in sql
                assert '$__timeFilter' not in sql
                # Date categories must not be clipped by Grafana's current time range.
                assert panel['type'] != 'timeseries'
                parse_sql(re.sub(r'\$\{[^}]+:sqlstring\}', "'test'", sql))
        for var in dashboard['templating']['list']:
            if var['type'] == 'query':
                parse_sql(re.sub(r'\$\{[^}]+:sqlstring\}', "'test'", var['query']))


def test_compose_and_datasource_yaml_have_no_duplicate_keys():
    yaml = pytest.importorskip('yaml')
    class UniqueLoader(yaml.SafeLoader):
        pass
    def mapping(loader, node):
        keys = [loader.construct_object(key) for key, value in node.value]
        assert len(set(keys)) == len(keys), 'duplicate YAML keys'
        return yaml.SafeLoader.construct_mapping(loader, node)
    UniqueLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, mapping)
    for name in ['infrastructure/ec2-a/compose.yaml','infrastructure/ec2-b/compose.yaml',
                 'infrastructure/observability/compose.yaml',
                 'infrastructure/observability/grafana/provisioning/datasources/reporting.yaml']:
        yaml.load((ROOT / name).read_text(encoding='utf-8'), Loader=UniqueLoader)
