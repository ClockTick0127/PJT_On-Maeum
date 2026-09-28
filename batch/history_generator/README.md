# history-generator

몇 달치 과거 데이터를 규칙으로 만들어 레이크에 직접 쓰는 도구다. 설계와 근거는
`docs/진행상황/이정민/0924/과거_데이터_생성방식_결정과_기간구성_설계.md`에 있다.

레이크 입력은 원본 파형(관측 품질에만 사용)과 세션(모든 사용량·패턴 지표의 출처)이 분리돼
있다. 이 도구는 시나리오 JSON의 규칙 일정표로 세션을 만들고, 그 일정에 맞춰 기존 시뮬레이터
물리 엔진(`infrastructure/mqtt/simulator/engine`)으로 파형을 만든다. AI 모델·MQTT·Kafka·
실시간 서비스는 쓰지 않는다. 운영 데이터 생성기가 아니라 집계·보고서·부하 검증용이다.

## 무엇을 쓰는가

| 산출물 | 위치(기본값, 로컬 compose와 동일) | 형식 |
|---|---|---|
| Bronze power | `/nilm/bronze/power/ingest_date=D/hour=HH/partition=0/part-hist-HH.parquet` (시간당 1개, 전 가구 합침) + `/nilm/manifests/job=bronze-loader/date=D/manifest-0-hist.json` | `power_silver.schemas.BRONZE_POWER_SCHEMA` |
| 분석 receipt | `/nilm/bronze/analysis-processing-receipt/ingest_date=D/batch_id=hist-D/part-HHHHH.parquet` + `/nilm/manifests/job=analysis-receipt-lake-loader/ingest_date=D/batch_id=hist-D/manifest.json` | `session_lake_loader.receipt_lake.RECEIPT_SCHEMA`, 입력 1건당 SUCCEEDED 1건 |
| 세션 | `/nilm/bronze/appliance-session/ingest_date=D/batch_id=hist-D/part-00000.parquet` + `/nilm/manifests/job=session-lake-loader/ingest_date=D/manifest-hist-D.json` | `session_lake_loader.lake_schema.LAKE_SCHEMA`, INSERT 버전 1 |

날짜 단위로 멱등이다. 세 manifest가 모두 있으면 그 날짜는 건너뛰고 `--force`로만 다시 쓴다.
경로는 `BRONZE_BASE`, `BRONZE_MANIFEST_BASE`, `RECEIPT_BRONZE_BASE`, `RECEIPT_MANIFEST_BASE`,
`SESSION_BRONZE_BASE`, `SESSION_MANIFEST_BASE` 환경변수로 바꾼다.

## 시나리오 JSON

`scenarios/demo_3households.json`이 기본안(3가구, 2026-06-25 ~ 09-23)이다. 전체 공통은
`range`, `utc_offset_seconds`, `analysis_run_id`, `topic`뿐이고 나머지는 가구 안에 있다.

```json
{
  "range": {"start": "2026-06-25", "end": "2026-09-23"},
  "households": [
    {
      "household_id": "H008", "seed": 8001, "sampling_interval_seconds": 1,
      "schedule": [
        {"appliance": "kettle", "median": "08:50", "jitter_minutes": 15, "probability": 0.97,
         "duration_seconds": [150, 210], "weekdays": null},
        {"appliance": "induction", "median": "19:00", "jitter_minutes": 20, "probability": 0.9,
         "duration_seconds": [600, 840], "segments": [3, 5], "segment_gap_seconds": [20, 60]}
      ],
      "periods": [
        {"name": "change", "start": "2026-08-20", "end": "2026-09-02", "shift_minutes": {"kettle": 150}},
        {"name": "missing", "start": "2026-09-04", "missing_windows": [["09:00", "15:00"]]},
        {"name": "reduced", "start": "2026-09-06", "end": "2026-09-12", "keep_first_use_only": true},
        {"name": "inactive", "start": "2026-09-13", "end": "2026-09-16", "probability_override": {"*": 0.0}},
        {"name": "long-use", "start": "2026-09-10", "extend_first_use_seconds": {"induction": 10800}}
      ],
      "away": [{"start": "2026-09-08T09:00:00+09:00", "end": "2026-09-08T18:00:00+09:00"}]
    }
  ],
  "load_households": {"count": 1000, "prefix": "L", "sampling_interval_seconds": 10,
                      "templates": ["H008", "H009", "H010"], "weights": [3, 2, 5], "seed": 99,
                      "inactive_fraction": 0.05, "waveform_pool_size": 50}
}
```

- 규칙 하나가 하루 한 번의 사용 후보다. `probability`로 그날 쓸지 정하고, `median ± jitter`
  안에서 시각을, `duration_seconds` 범위에서 길이를 시드로 뽑는다. `segments`는 한 사용을
  여러 세션으로 나눠 usage-daily 병합 규칙(포트·전자레인지·드라이어·청소기 60초, 인덕션 120초,
  다리미 300초)이 실제로 동작하는지 보게 한다. 병합 간격보다 긴 gap은 시나리오 오류다.
- `periods`는 날짜 범위 안에서 규칙을 바꾼다. 시각 이동, 확률 덮어쓰기(`"*"`는 전 가전),
  첫 사용만 유지, 첫 사용 연장, 파형·receipt 결측 구간. 같은 날 여러 period가 겹치면 순서대로
  적용한다.
- 같은 가구·시드·날짜는 항상 같은 일정을 만든다. period 편집은 그 규칙만 바꾸고 같은 날의
  다른 규칙 난수를 흔들지 않는다.
- `load_households`는 데모 가구를 원형으로 시드 흔들기(시각 ±60분, 확률 ±0.1)를 한 부하 가구를
  만든다. 부하 가구의 파형은 원형 가구별 `waveform_pool_size`개 날을 재사용한다(수집률에만 쓰이므로
  세션과 파형이 일치하지 않아도 집계 결과에 영향이 없다).
- `away`는 데이터에 반영되지 않고 `snapshots` 명령이 만드는 historicalAssessment `config.json`의
  `awayPeriods`로만 나간다.

## 명령

```console
history-generator plan scenarios/demo_3households.json --from 2026-09-10 --to 2026-09-16
history-generator write scenarios/demo_3households.json --simulator-dir /simulator            # HDFS
history-generator write scenarios/demo_3households.json --waveform synthetic --lake-local-root /tmp/lake
history-generator targets scenarios/demo_3households.json --existing observation_targets.json --output /work/targets.json
history-generator snapshots scenarios/demo_3households.json --simulator-dir /simulator --output /work/backfill
history-generator profiles --household H008 --output /work/backfill/H008/profiles.json
history-generator run-dates --from 2026-06-25 --to 2026-09-23 --compose-file infrastructure/local/compose.yaml \
    --env OBSERVATION_TARGETS_FILE=/work/targets.json --log run-dates.jsonl
```

- `plan`은 정답표다. 4단계에서 `appliance_usage_daily`의 `usage_count`, `first_use_second`와 대조한다.
- `write`는 HDFS(`HDFS_URL`, `HDFS_USER`, `LAKE_FS_URI` 또는 같은 이름의 옵션) 또는
  `--lake-local-root`에 쓴다. `--waveform`을 생략하면 `--simulator-dir`가 있을 때 시뮬레이터,
  없으면 합성 파형이다. 가구·하루 파형 생성은 시뮬레이터 약 1.2초, 기록 약 0.2초다.
- `run-dates`는 호스트에서 `docker compose run --rm --no-deps gold-profile gold-profile daily --as-of D
  --no-publish`를 날짜 순서로 실행하고 JSON 보고서와 exit code를 `--log`에 남긴다. 28일 창이
  차기 전 날짜의 exit 10(INPUT_INCOMPLETE)은 정상이며 계속 진행한다. exit 1은 멈춘다(`--keep-going`).
- `profiles`는 `gold_profile_delivery_outbox`의 payload를 `as_of_date`, `profile_revision` 순으로 모은다.
  DB 접속은 `DATABASE_URL` 또는 compose와 같은 `DATABASE_HOST` 등 변수를 쓴다.

## 실행 환경

컨테이너에서 실행한다. 이미지는 `nilm-gold-profile:local` 위에 pyarrow·numpy를 더한다.

```console
docker build -t nilm-history-generator:local batch/history_generator
docker run --rm --network nilm-net \
  -v "$PWD/infrastructure/mqtt/simulator:/simulator:ro" -v "$PWD/.work:/work" \
  -e HDFS_URL=http://namenode:9870 -e LAKE_FS_URI=hdfs://namenode:9000 \
  nilm-history-generator:local write /app/scenarios/demo_3households.json --simulator-dir /simulator
```

Windows Git Bash에서는 `MSYS_NO_PATHCONV=1`을 앞에 붙이고, 한글 경로 마운트 문제가 있으면
시나리오·작업 디렉터리를 ASCII 경로에 둔다.

호스트 단위 테스트: Python 3.11 venv에 `pyarrow numpy pytest`를 설치하고 이 디렉터리에서
`pytest`를 실행한다(`pyproject.toml`의 `pythonpath`가 power_silver와 session_lake_loader 소스를
잡는다). Docker: `docker build --target test batch/history_generator`.

## 전체 순서 (설계 문서 5.2절)

1. `plan`으로 일정표 검토.
2. `targets`로 관측 대상 파일을 만들고 monitoring에 가구가 등록돼 있는지 확인.
3. `write`로 Bronze·receipt·세션 기록.
4. `run-dates`로 날짜별 `gold-profile daily`.
5. `profiles`, `snapshots`로 historicalAssessment 입력을 만들고 `backend/monitoring-service`에서
   `./gradlew historicalAssessment --args='<config.json> <result dir>'`.
6. `household-report import-assessments` → `household-report daily --historical-manifest ...` → Grafana.

## 한계

- receipt는 전 입력 SUCCEEDED로 단순화한다(실제 서비스의 warm-up 254초 SKIPPED_WARMUP 없음).
- 세션은 자정을 넘기지 않는다. 자정 근처 규칙은 하루 안으로 잘린다.
- `PROLONGED_APPLIANCE_USE`, `ROUTINE_CHANGED`, `DATA_GAP`는 실시간 서비스 이벤트라 이 경로에서
  나오지 않는다. 과거 위험 점수는 `EVENT_TIME_REASSESSMENT`다.
- 파형은 관측 품질에만 쓰인다. 실제 모델 추론에 넣어 검증하는 용도가 아니다.
