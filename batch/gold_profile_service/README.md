# Gold profile batch

실험 ID별 설정·CSV·결과·로그 저장 및 로컬/EC2 bind mount 실행은
[EXPERIMENTS.md](EXPERIMENTS.md)를 참고하세요.

Grafana 일별 보고서·추세 자동화의 설치와 실행 방법은 [REPORTING.md](REPORTING.md)를 참고하세요.

`gold-profile` reads the explicitly selected ACTIVE versions of the previous 28
`appliance_usage_daily` and `appliance_session_daily_slices` partitions. It publishes
two versioned Gold datasets and their versioned Silver episode input:

- `routine_baseline_history`: overall and weekday appliance baselines
- Silver `appliance_logical_uses`: complete cross-day logical-use episodes
- `household_statistical_profile`: cumulative activity, inactivity, duration and recent change metrics

Missing dates do not silently shrink the window: outputs are marked
`INPUT_INCOMPLETE`, and routine baselines are disabled. The manifest records all
selected upstream version IDs and missing dates. This job deliberately does not
update the serving DB baseline and does not generate a risk score.

```console
gold-profile run --as-of 2026-09-19
gold-profile backfill --from 2026-09-01 --to 2026-09-19
gold-profile status --as-of 2026-09-19
gold-profile dirty --as-of 2026-09-19
gold-profile dirty --from 2026-09-01 --to 2026-09-28
```

## Running a business day

`gold-profile run` is one stage of four. `daily` runs them in order, retries each
stage, and reports where it stopped:

```console
gold-profile daily                      # yesterday, business time
gold-profile daily --as-of 2026-09-19
gold-profile daily --attempts 5 --no-publish
```

1. `power-silver run` — clean power and observation days for the date
2. `power-silver usage-daily` — analysis coverage, session slices, `appliance_usage_daily`
3. **window alignment** — see below
4. `gold-profile run` — the profile itself
5. `gold-profile publish` — one outbox sweep

Window alignment is why a plain "aggregate yesterday, then build Gold" sequence does
not work. Gold refuses a window whose dates were prepared from different session
snapshots, and sessions are corrected and deleted after the fact, so aggregating only
yesterday leaves that one date on a newer snapshot than the other 27. Before calling
Gold, `daily` compares each window date's session token (read from the
`appliance_usage_daily` `config_version`) against the target date's and re-aggregates
the ones that differ. Dates whose power Silver is missing cannot be fixed here; they
are reported as `skipped_dates` and are a backfill's job.

The command prints one JSON report. `status` and `exit_code` are the scheduler contract:

| Status | Exit | Meaning |
|---|---:|---|
| `SUCCEEDED` | 0 | Gold committed; an inline outbox sweep had no reported failures |
| `FAILED` | 1 | Code/infrastructure failure after stage retries were exhausted |
| invalid CLI input | 2 | Argument validation failed before the pipeline started |
| `INPUT_INCOMPLETE` | 10 | Inputs/window are not ready; retry or prepare missing Silver dates |
| `SKIPPED` | 11 | Another writer owns the date; no duplicate pipeline was started |
| `PUBLISH_PENDING` | 12 | Gold committed, but delivery is delegated to the durable publisher |

`SUCCEEDED` or `PUBLISH_PENDING` does **not** mean that monitoring consumed the
message or replaced an operational profile. Check the outbox, publisher logs, and the
monitoring profile status separately. A failed run is safe to repeat: completed stages
reuse their manifests when input, rules, configuration, and delivery mode match.

Reruns are idempotent stage by stage: `power-silver run` and `power-silver usage-daily`
return their completed manifests when the input snapshot, confirmed receipt/session
selection, rule and policy are unchanged, so Gold's input snapshot does not move and the
Gold run is reused as well. Only a real change (new Bronze files, a corrected or deleted
session, a rule/policy change, or SHADOW -> ACTIVE) produces a new profile revision.

Delivery is separate and long-lived. The batch only writes outbox rows; a row whose
send failed carries a `next_attempt_at` that means nothing unless something calls the
publisher again:

```console
gold-profile publish            # one sweep
gold-profile publish --loop     # how the gold-profile-publisher service runs
```

The resident publisher is the production owner of retries. It starts only after the
analysis DB migrations have created `gold_profile_delivery_outbox` and Kafka init has
created `gold.household-profile.v1`. It re-reads durable `PENDING` rows after a process
or server restart. The scheduled daily runner uses `--no-publish`, avoiding two
publishers racing the same rows; exit 12 is an accepted hand-off to this daemon.

## EC2 daily schedule

EC2-B uses the checked-in systemd oneshot/timer pair. The default example is 06:30
`Asia/Seoul`, after the default six-hour input deadline. The CLI computes the default
target as yesterday in `BUSINESS_UTC_OFFSET_SECONDS`; the production runner passes each
date explicitly and rechecks the most recent three dates by default.

Deployment copies the runner and unit templates under `/opt/nilm`. Installation is a
separate operator action and does not start anything by itself:

```console
sudo install -m 600 /opt/nilm/gold-daily.env.example /opt/nilm/gold-daily.env
# Edit /opt/nilm/gold-daily.env if the defaults need changing.
sudo /opt/nilm/bin/install-gold-daily-systemd.sh '*-*-* 06:30:00 Asia/Seoul'
systemctl cat gold-profile-daily.timer
sudo systemctl enable --now gold-profile-daily.timer
systemctl list-timers gold-profile-daily.timer
```

Put non-secret runner overrides in `/opt/nilm/gold-daily.env`, for example:

```dotenv
GOLD_DAILY_ATTEMPTS=3
GOLD_DAILY_RETRY_SECONDS=30
GOLD_DAILY_CATCHUP_DAYS=3
```

`Persistent=true` triggers a missed timer after boot. The catch-up window processes
oldest first and safely reuses completed manifests. A date whose inputs are still
incomplete (exit 10) does not stop the newer dates behind it; the runner finishes the
window and then exits 10 so the incomplete date stays visible in systemd. Increase the
window temporarily after a long outage, or run explicit dates manually. `flock` prevents overlapping host runs;
the database date lock remains the cross-process guard. The runner uses
`docker compose run --rm --no-deps`, so it never restarts Bronze/session collectors or
the realtime service.

Manual execution and inspection:

```console
sudo systemctl start gold-profile-daily.service
journalctl -u gold-profile-daily.service -n 200 --no-pager
cd /opt/nilm
docker compose run --rm --no-deps gold-profile \
  gold-profile daily --as-of 2026-09-19 --attempts 3 --retry-seconds 30 --no-publish
```

## Initial load, rerun, and backfill

Before the first Gold run, start the realtime service so DB migrations are current,
start both lake loaders, and confirm receipt/session manifests exist. Prepare the full
profile window (28 dates by default) with power Silver and usage-daily outputs. The
daily command can align dates that have both power and observation datasets, but it
cannot invent missing historical Silver.

- Daily failure: rerun the same `daily --as-of YYYY-MM-DD`. Exit 1 is a code or
  infrastructure failure; inspect the failed stage's `error`.
- Input wait: exit 10 reports missing/skipped/mismatched dates in the JSON. Wait for
  Bronze/session/receipt manifests or prepare those historical Silver dates, then rerun.
- Corrected raw power or sessions: rebuild the affected Silver/usage dates, use
  `gold-profile dirty --from ... --to ...` to inspect affected profile dates, then rerun
  `daily` for those dates oldest first.
- Gold-only history: `gold-profile backfill --from ... --to ...` assumes the aligned
  usage/session inputs already exist; it does not replace the full daily preparation.
- Delivery: inspect `gold-profile-publisher` logs and monitoring's stored status.
  `SHADOW`, `REJECTED`, or a still-`PENDING` outbox is not an operational ACTIVE profile.

## Message contract expected by monitoring-service

The contract is owned by `monitoring-service`, which consumes it. The field names are
the output column names of `routine_baseline.py` and `statistical_profile.py`.

Topic `gold.household-profile.v1`, key `household_id`, one message per household and
profile version (`profile_version` is this job's `run_id`). `delivery_mode` is `SHADOW`
until an operator raises `PROFILE_DELIVERY_MODE`; a `SHADOW` message is stored as
history and never becomes a household's operational profile. Delivery mode is part of
Gold's `config_version`, so rerunning the same date after SHADOW -> ACTIVE creates a new
run/profile version with the next revision. It never relabels the SHADOW history row.

```json
{
  "schema_version": 2,
  "household_id": "house-001",
  "profile_version": "<gold run_id>",
  "profile_revision": 1,
  "delivery_mode": "ACTIVE | SHADOW",
  "as_of_date": "2026-09-19",
  "window_start_date": "2026-08-23",
  "window_end_date": "2026-09-19",
  "effective_from": "2026-09-20T00:00:00+09:00",
  "published_at": "2026-09-20T03:12:40Z",
  "input_snapshot_id": "...",
  "rule_version": "gold-profile-v1",
  "statistic_rule_version": "household-statistics-v1-nearest-rank",
  "quality_status": "READY | INPUT_INCOMPLETE",
  "routine_baselines": [
    {"appliance_type": "KETTLE", "baseline_scope": "OVERALL | WEEKDAY", "weekday": null,
     "sample_days": 26, "active_days": 22, "daily_use_probability": 0.8462,
     "reliability_weight": 1.0, "first_use_time_p50_second": 30600,
     "expected_until_second": 33000, "preferred_window_start_second": 27000,
     "preferred_window_end_second": 33000, "quality_status": "READY", "enabled": true}
  ],
  "statistics": [
    {"metric_name": "CUMULATIVE_ACTIVITY_START_COUNT | INACTIVITY_ELAPSED | LOGICAL_USE_ACTIVE_DURATION | RECENT_ACTIVITY_COUNT_DELTA",
     "appliance_type": null, "weekday_group": "ALL | WEEKDAY | WEEKEND | MON..SUN",
     "time_bucket": "09:30", "sample_count": 20, "eligible_day_count": 20,
     "p50": 1.0, "p90": 3.0, "mad": 0.5, "unit": "count", "quality_status": "READY"}
  ]
}
```

`time_bucket` is the bucket **end** label (`HH:MM`, `24:00` for the last bucket of the
day); the consumer looks statistics up by exactly this label. A captured payload lives in
`backend/monitoring-service/src/test/resources/contracts/gold-household-profile-v2.json`
and is checked from both sides (`tests/test_message_contract_fixture.py` here,
`GoldProfileMessageContractTest` there).

How the consumer treats it:

- Unknown fields are ignored, so new output columns are safe to add.
- A message missing `household_id`, `profile_version`, `as_of_date` or `effective_from` is
  logged and skipped. The consumer never raises, so a bad profile for one household cannot
  stop the stream for the rest.
- Re-sending the same `(household_id, profile_version)` is a no-op, so the publisher may
  retry freely. Delivery does not have to be exactly once.
- `quality_status` other than `READY`, or an empty `routine_baselines` **and**
  `statistics`, is stored as `REJECTED` and never replaces the household's active profile.
- Ordering is `as_of_date`, then `profile_revision`. A lower/equal order is stored as
  history only, so late messages cannot roll an ACTIVE profile back. SHADOW messages
  never participate in operational replacement.

The promotion procedure is: run and publish SHADOW, validate the stored SHADOW profile,
set `GOLD_PROFILE_DELIVERY_MODE=ACTIVE`, rerun the same `as_of_date`, and let the resident
publisher send the new version. Repeating the ACTIVE request reuses that ACTIVE run and
repairs a missing outbox row; an already-PUBLISHED SHADOW row does not consume it.

**`effective_from` is not trusted.** This job currently writes the batch execution time
into it (`job.py` passes `now`), which is not the moment the profile becomes valid for
evaluation. The consumer therefore uses `effective_from` only as a "not before" gate and
enforces the real rule itself: a profile is eligible at evaluation time `T` only when
`as_of_date <= (KST date of T) - 1`. A profile built from data that includes the day being
evaluated is refused regardless of `effective_from`.
