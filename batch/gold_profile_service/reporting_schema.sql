CREATE SCHEMA IF NOT EXISTS reporting;

CREATE TABLE IF NOT EXISTS reporting.report_runs (
    report_id TEXT PRIMARY KEY,
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    gold_run_id TEXT NOT NULL,
    input_snapshot_id TEXT NOT NULL,
    rule_version TEXT NOT NULL,
    manifest_path TEXT NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    manifest JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS reporting.report_rows (
    report_id TEXT NOT NULL REFERENCES reporting.report_runs(report_id),
    household_id TEXT NOT NULL,
    dataset TEXT NOT NULL,
    row_key TEXT NOT NULL,
    payload JSONB NOT NULL,
    PRIMARY KEY (report_id, household_id, dataset, row_key)
);

CREATE INDEX IF NOT EXISTS ix_report_rows_household
    ON reporting.report_rows(household_id, report_id, dataset);

-- Typed serving views reuse the immutable, transactional report snapshot.
-- Only v2 reports with a report_summary are listed; old reports remain archived.
CREATE OR REPLACE VIEW reporting.v_report_index AS
SELECT r.report_id, d.household_id, r.period_start, r.period_end, r.generated_at,
       r.published_at, r.gold_run_id, r.rule_version, r.input_snapshot_id,
       COALESCE(r.manifest->>'assessment_mode', 'UNKNOWN_ORIGIN') AS assessment_mode,
       r.manifest->'input'->>'assessment_completeness_scope' AS completeness_scope,
       r.manifest->'input'->'missing_usage_dates' AS missing_usage_dates
FROM reporting.report_runs r JOIN reporting.report_rows d USING (report_id)
WHERE d.dataset = 'report_summary';

CREATE OR REPLACE VIEW reporting.v_household_daily AS
SELECT report_id, household_id, (payload->>'usage_date')::date AS usage_date,
       (payload->>'max_valid_score')::double precision AS max_valid_score,
       (payload->>'max_partial_score')::double precision AS max_partial_score,
       (payload->>'recorded_count')::bigint AS recorded_count,
       (payload->>'valid_count')::bigint AS valid_count,
       (payload->>'partial_count')::bigint AS partial_count,
       payload->>'assessment_mode' AS assessment_mode,
       payload->>'assessment_data_status' AS assessment_data_status
FROM reporting.report_rows WHERE dataset = 'household_daily_trends';

CREATE OR REPLACE VIEW reporting.v_appliance_daily AS
SELECT report_id, household_id, (payload->>'usage_date')::date AS usage_date,
       payload->>'appliance_type' AS appliance_type,
       payload->>'usage_status' AS usage_status,
       (payload->>'usage_minutes')::double precision AS usage_minutes,
       (payload->>'usage_count')::bigint AS usage_count,
       (payload->>'baseline_eligible')::boolean AS baseline_eligible
FROM reporting.report_rows WHERE dataset = 'appliance_daily_trends';

CREATE OR REPLACE VIEW reporting.v_report_summary AS
SELECT report_id, household_id, (payload->>'usage_date')::date AS report_date,
       payload->>'statement' AS statement
FROM reporting.report_rows WHERE dataset = 'report_summary';

CREATE OR REPLACE VIEW reporting.v_report_evidence AS
SELECT report_id, household_id, payload->>'evidence_type' AS evidence_type,
       payload->>'appliance_type' AS appliance_type, payload->>'statement' AS statement,
       payload->>'quality_status' AS quality_status, payload->>'values_json' AS values_json
FROM reporting.report_rows WHERE dataset = 'evidence';

CREATE INDEX IF NOT EXISTS ix_report_runs_period ON reporting.report_runs(period_end DESC, generated_at DESC);
