import assert from 'node:assert/strict'
import { readFileSync, readdirSync, mkdtempSync, writeFileSync, rmSync } from 'node:fs'
import { basename, dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { spawnSync } from 'node:child_process'

const root = dirname(fileURLToPath(import.meta.url))
const read = (path) => readFileSync(join(root, path), 'utf8').replace(/^\uFEFF/, '')
const files = readdirSync(join(root, 'grafana/dashboards')).filter((name) => name.endsWith('.json'))
const dashboards = files.map((name) => JSON.parse(read(`grafana/dashboards/${name}`)))
const uids = new Set()
const expressions = []
for (const dashboard of dashboards) {
  assert(!uids.has(dashboard.uid), `Duplicate dashboard UID: ${dashboard.uid}`)
  uids.add(dashboard.uid)
  assert.equal(dashboard.refresh, '1s')
  assert(dashboard.timepicker.refresh_intervals.includes('1s'))
  const ids = new Set()
  for (const panel of dashboard.panels) {
    assert(!ids.has(panel.id), `Duplicate panel ID: ${dashboard.uid}/${panel.id}`)
    ids.add(panel.id)
    for (const target of panel.targets || []) {
      assert.equal(target.datasource.uid, 'nilm-prometheus')
      if (dashboard.uid.startsWith('nilm-ai-')) {
        assert.equal(target.interval, '1s')
        assert(target.expr.includes('job="ai-analysis"'))
        assert(target.expr.includes('instance=~"$instance"'))
      }
      if (target.expr.includes('nilm_daily_job_')) {
        assert(target.expr.includes('exported_job'))
        assert(!target.expr.includes('$__rate_interval'))
      }
      expressions.push(target.expr.replaceAll('$__rate_interval', '1m').replaceAll('$__range', '2d').replaceAll('$instance', '.*'))
    }
  }
}
const patterns = dashboards.find((dashboard) => dashboard.uid === 'nilm-ai-patterns')
assert(patterns)
for (const metric of ['nilm_pattern_detection_duration_seconds', 'nilm_pattern_detection_total', 'nilm_pattern_events_total', 'nilm_daily_job_duration_seconds', 'nilm_daily_job_runs_total']) {
  assert(patterns.panels.some((panel) => panel.targets.some((target) => target.expr.includes(metric))), `Missing metric: ${metric}`)
}
const config = read('prometheus/prometheus.yml')
const ai = config.split('- job_name: ai-analysis')[1].split('- job_name:')[0]
assert(ai.includes('scrape_interval: 1s'))
assert(ai.includes('scrape_timeout: 1s'))
assert(ai.includes('honor_labels: false'))
assert(read('compose.yaml').includes('GF_DASHBOARDS_MIN_REFRESH_INTERVAL: 1s'))
assert(read('grafana/provisioning/datasources/prometheus.yaml').includes('timeInterval: 15s'))
console.log(`PASS ${dashboards.length} dashboards, ${expressions.length} queries, five metrics, exported_job and refresh settings`)

if (process.argv.includes('--promtool')) {
  const temporary = mkdtempSync(join(root, '.verify-metrics-'))
  try {
    writeFileSync(join(temporary, 'rules.yml'), JSON.stringify({ groups: [{ name: 'dashboard-syntax', rules: expressions.map((expr, index) => ({ record: `verification:query_${index}`, expr })) }] }))
    const labels = 'job="ai-analysis",instance="ai:8000"'
    const tests = {
      rule_files: [], evaluation_interval: '1s', tests: [{ interval: '1s', input_series: [
        { series: `nilm_pattern_detection_total{${labels},pattern="ROUTINE_MISSED",result="detected"}`, values: '0+10x60' },
        { series: `nilm_pattern_events_total{${labels},event_type="ROUTINE_MISSED"}`, values: '0+2x60' },
        { series: `nilm_daily_job_runs_total{${labels},exported_job="activity_index",status="success"}`, values: '1+0x60' },
        { series: `nilm_daily_job_runs_total{${labels},exported_job="baseline_update",status="error"}`, values: '3+0x60' },
        { series: `nilm_daily_job_duration_seconds_sum{${labels},exported_job="activity_index"}`, values: '8+0x60' },
        { series: `nilm_daily_job_duration_seconds_count{${labels},exported_job="activity_index"}`, values: '2+0x60' },
      ], promql_expr_test: [
        { expr: `sum by(pattern,result) (rate(nilm_pattern_detection_total{job="ai-analysis"}[1m]))`, eval_time: '60s', exp_samples: [{ labels: '{pattern="ROUTINE_MISSED",result="detected"}', value: 10 }] },
        { expr: `sum by(event_type) (rate(nilm_pattern_events_total{job="ai-analysis"}[1m]))`, eval_time: '60s', exp_samples: [{ labels: '{event_type="ROUTINE_MISSED"}', value: 2 }] },
        { expr: `sum by(exported_job,status) (nilm_daily_job_runs_total{job="ai-analysis"})`, eval_time: '60s', exp_samples: [{ labels: '{exported_job="activity_index",status="success"}', value: 1 }, { labels: '{exported_job="baseline_update",status="error"}', value: 3 }] },
        { expr: `sum by(exported_job) (increase(nilm_daily_job_runs_total{job="ai-analysis",exported_job="activity_index"}[1m]))`, eval_time: '60s', exp_samples: [{ labels: '{exported_job="activity_index"}', value: 0 }] },
        { expr: `sum by(exported_job) (nilm_daily_job_duration_seconds_sum{job="ai-analysis"}) / sum by(exported_job) (nilm_daily_job_duration_seconds_count{job="ai-analysis"})`, eval_time: '60s', exp_samples: [{ labels: '{exported_job="activity_index"}', value: 4 }] },
        { expr: `sum by(exported_job) (nilm_daily_job_runs_total{job="ai-analysis",exported_job="routine_changed"})`, eval_time: '60s', exp_samples: [] },
      ] }],
    }
    writeFileSync(join(temporary, 'tests.yml'), JSON.stringify(tests))
    for (const command of [['check', 'config', '/etc/prometheus/prometheus.yml'], ['check', 'rules', '/verification/rules.yml'], ['test', 'rules', '/verification/tests.yml']]) {
      const result = spawnSync('docker', ['run', '--rm', '--network', 'none',
        '--mount', `type=bind,source=${join(root, 'prometheus/prometheus.yml')},target=/etc/prometheus/prometheus.yml,readonly`,
        '--mount', `type=bind,source=${join(root, 'prometheus/targets/local')},target=/etc/prometheus/targets,readonly`,
        '--mount', `type=bind,source=${join(root, 'prometheus/resources-disabled')},target=/etc/prometheus/resources,readonly`,
        '--mount', `type=bind,source=${temporary},target=/verification,readonly`,
        '--entrypoint', 'promtool', 'prom/prometheus:v3.5.0', ...command], { stdio: 'inherit' })
      assert.equal(result.status, 0, `promtool ${command.join(' ')} failed`)
    }
  } finally {
    assert.equal(dirname(temporary), root)
    assert(basename(temporary).startsWith('.verify-metrics-'))
    rmSync(temporary, { recursive: true, force: true })
  }
}
