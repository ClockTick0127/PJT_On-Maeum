# power-silver-service — Bronze 전력 → Silver 정제·관측일 일일 배치

Bronze에 쌓인 전력 원본을 읽어 **정제 전력(`power_clean`)** 과 **가구별 관측일
(`household_observation_daily`)** 을 만드는 Spark 일일 배치다. 기존 수집기
(`collection/bronze-loader`)와 DB 기반 집계(`batch/aggregation_service`)는 그대로 둔다.

전력 Silver 확정 후 `power-silver usage-daily --date YYYY-MM-DD`를 실행하면 확정된
receipt·세션 manifest를 결합해 Silver `analysis_coverage`,
`appliance_session_daily_slices`, Gold `appliance_usage_daily`, DB
`analysis_daily_completion` revision을 생성한다. `NOT_USED`는 관측·분석·전달·세션
품질이 모두 적격일 때만 만들며, 나머지 미확인 상태는 `UNKNOWN`으로 보존한다.
같은 전력 Silver 입력·확정 manifest 선택·규칙·완료 정책으로 다시 실행하면 새 버전을
만들지 않고 확정된 실행의 manifest(`reused_run_id`)를 돌려준다. 그래서
`gold-profile daily`를 같은 날짜에 반복해도 Gold 입력 스냅샷이 움직이지 않는다.
`--force`는 이 재사용을 건너뛴다.

```text
Kafka ──▶ bronze-loader ──▶ HDFS Bronze 전력 원본 + 적재 manifest
                                     │
                                     ▼
                       [power-silver-daily (Spark)]
                         입력 확정 → 검증·정규화 → 중복 제거 → 측정일별 분류
                         → 관측일 계산 → 파일 병합 저장 → 출력 검증
                         → 최종 경로로 이동 → 완료 manifest → 활성 버전 반영(DB)
                                     │
                     ┌───────────────┴───────────────┐
                     ▼                               ▼
        Silver power_clean            Silver household_observation_daily
```

이 단계에 **없는 것**: 가전별 사용 판정, 사용 세션 적재(→ `collection/session-lake-loader`),
baseline 계산, 보고서, Gold 계층. 후속 루틴 집계가 여기서 만든 관측일과 Silver 사용 세션을
결합해 `sample_days`·`active_days`를 계산한다.

## 1. 입력

### Bronze 전력 원본

`collection/bronze-loader/loader.py`의 스키마를 그대로 읽는다. `raw_payload`는 읽지
않는다 — 원문이 필요하면 `source_file` + Kafka 좌표로 Bronze를 다시 본다.

### Bronze 적재 manifest

**수집일 폴더만 보고 전날 파일을 고르지 않는다.** manifest의 `business_dates`에 대상
날짜가 들어 있는 파일을 고른다. 그래야 다음날 도착한 전날 데이터가 포함된다.

manifest는 **저장 완료의 증거이지 그날 모든 센서 데이터가 도착했다는 증거가 아니다.**
그래서 "무엇을 읽을지"와 "지금 확정해도 되는지"를 따로 판단한다(→ 3절).

### 관측 대상 가구·계측기 (`src/power_silver/config/observation_targets.json`)

| 필드 | 의미 |
|---|---|
| `household_id` / `device_id` | 관측 대상 가구와 대표 계측기 |
| `effective_from` / `effective_to` | 적용 기간(ISO 8601, 시간대 필수). `null`이면 열린 구간 |
| `sampling_interval_seconds` | 기대 수집 간격. 초기 1초 |
| `observation_enabled` | 관측 대상 여부 |

원본에 등장한 가구만 집계하면 **하루 종일 데이터가 없는 가구를 발견할 수 없다.** 결과는
이 목록을 기준으로 만들고 측정값을 왼쪽 조인한다. 초기 정책은 **가구별 대표 계측기 1개**다.
여러 계측기의 건수를 합산하면 수집률이 부풀려지므로, 같은 가구의 활성 구간이 겹치면 설정
오류로 실행을 멈춘다. 계측기 교체는 구간을 나눠 표현하고 `DEVICE_CHANGED` 표시가 붙는다.

`config_version`은 저장할 때 내용 해시를 붙여 `이름@지문` 형태가 된다. 이름만 같고 내용이
바뀐 설정을 같은 설정으로 오해하지 않기 위해서다.

## 2. 실행 식별자

| 인자 | 의미 |
|---|---|
| `target_date` | 처리할 한국 시간 날짜 |
| `input_snapshot_id` | 확정한 입력 목록의 지문(경로 + 크기 + 수정시각의 sha256) |
| `run_id` | 이번 실행 |
| `rule_version` | 정제·관측 판정 규칙 버전 |
| `config_version` | 관측 대상 설정 버전(+지문) |

적재기는 같은 offset 구간을 같은 경로로 교체할 수 있다. **경로 목록만으로는 완전한
스냅샷이 아니므로** 크기와 수정시각까지 지문에 넣고, Spark가 읽기를 끝낸 뒤 다시 확인해
바뀌었으면 실행을 실패시킨다(`InputChanged`).

## 3. 입력 준비 판정 (`WAITING_INPUT`)

적재기는 Kafka 파티션마다 따로 플러시한다. 한 파티션이 밀려 있으면 그 파티션 데이터가
아직 오지 않은 것이다. 모든 파티션이 날짜 경계를 넘겼다는 증거가 있어야 확정한다.

| 조건 | 판정 |
|---|---|
| 대상 날짜가 아직 끝나지 않음 | `TARGET_DATE_NOT_FINISHED` |
| manifest가 하나도 없음 | `NO_BRONZE_MANIFEST` |
| 어떤 파티션의 최대 측정시각이 날짜 경계 이전이고, 유예시간 이후의 커밋도 없음 | `PARTITIONS_BEHIND:<파티션>` |
| 위를 모두 통과하거나 `INPUT_WAIT_DEADLINE_SECONDS` 경과 | 확정 진행 |

증거는 둘 중 하나다 — 경계 이후 시각의 측정값을 이미 실었거나(측정 진행), 경계 + 유예시간
이후에도 계속 플러시하고 있거나(적재기 생존). 기다리는 동안에는 관측일을 확정하지 않고
`WAITING_INPUT` 실행만 기록한다. **적재 지연을 센서 고장으로 기록하지 않기 위해서다.**

## 4. 정제 규칙

| 처리 | 규칙 |
|---|---|
| 스키마 | 명시적 스키마로만 읽는다. 추론하지 않는다 |
| ID | `message_id` UUID 형식, 가구·기기 ID 길이 1~50 |
| 시각 | 시간대 필수. 형식을 정규식으로 고정한 뒤 초·소수부·오프셋을 따로 계산해 UTC로 정규화 |
| 미래 시각 | `FUTURE_SKEW_SECONDS`를 넘으면 격리 |
| 숫자 | `null`·NaN·무한대 격리 |
| 값 범위 | `active_power ≥ 0`, `current ≥ 0`, `-1 ≤ power_factor ≤ 1` |
| 지연 데이터 | 오래됐다는 이유로 버리지 않는다. 해당 날짜의 수정 실행에 포함된다 |
| 원본 추적 | `source_file` + `topic`/`kafka_partition`/`kafka_offset` 보존 |

격리 사유: `INVALID_MESSAGE_ID`, `INVALID_HOUSEHOLD_ID`, `INVALID_DEVICE_ID`,
`INVALID_MEASURED_AT`, `FUTURE_MEASURED_AT`, `INVALID_NUMBER`, `OUT_OF_RANGE`,
`KAFKA_RECORD_CONFLICT`, `MESSAGE_CONFLICT`, `MEASUREMENT_CONFLICT`.

설정에 없는 가구의 측정값은 **격리하지 않고** `power_clean`에 남긴다(유효한 측정이다).
관측일 결과에는 들어가지 않고, 실행 통계에만 나타난다.

### 중복 제거

| 단계 | 중복 키 | 처리 |
|---|---|---|
| 물리 중복 | `topic + partition + kafka_offset` | Kafka 레코드의 반복 적재 제거 |
| 논리 중복 | `message_id` | 생산자 재전송 제거 |
| 측정 중복 | `household_id + device_id + measured_at_utc` | 같은 측정의 재발행 제거(시뮬레이터 재실행 등) |

같은 키의 내용이 같으면 하나만 남긴다. **내용이 다르면 임의의 한 행을 고르지 않고 충돌
그룹 전체를 격리한다.** 보존 행은 `(수신시각, 입력 파일, 파티션, offset)` 순서로 정해
노드 수가 달라도 같은 행이 남는다. 측정 중복은 `message_id`가 달라도 잡히므로, 같은
측정시각을 새 ID로 다시 발행한 재실행분은 최초 적재분만 남고 나머지는 `DUPLICATE`가 된다.

## 5. 관측일 계산

DB 방식은 수집 건수를 86,400으로 나눈다. 같은 1초에 재전송이 몰리면 관측하지 못한 시간을
관측한 것처럼 만든다. 레이크에서는 **실제로 관측한 시간 구간 수**를 센다.

```text
expected_sample_count = 관측 대상 구간의 기대 슬롯 수 (1초 간격 하루면 86,400)
observed_slot_count   = 정상 측정값이 존재하는 서로 다른 슬롯 수
coverage_ratio        = observed_slot_count / expected_sample_count
```

| 조건 | 상태 |
|---|---|
| 기대 관측량 0 (설정 비활성 등) | `NOT_APPLICABLE` — 0으로 나누지 않는다 |
| 관측 구간 0 | `SENSOR_GAP` |
| 관측률 ≥ 0.95 | `VALID` |
| 그 외 | `INSUFFICIENT_DATA` |

95% 판정은 **반올림 전 값**으로 하고, 저장하는 `coverage_ratio`만 소수점 넷째 자리로
반올림한다. `SENSOR_GAP`은 "유효 관측 없음"이라는 데이터 상태이지 센서 고장 판정이
아니므로, 같은 행에 `duplicate_count`·`invalid_count`를, manifest에 적재 통계를 함께 남긴다.

`max_missing_seconds`는 관측한 슬롯만 정렬해 인접 슬롯의 차이와 구간 시작·끝 경계로
계산한다. 가구마다 86,400행짜리 빈 시간표를 만들지 않는다. 관측 대상이 아닌 시간(설치
이전, 계측기 교체 사이)은 누락으로 세지 않는다.

품질 표시(`quality_flags`): `PARTIAL_DAY`, `DEVICE_CHANGED`, `DUPLICATES_REMOVED`,
`INVALID_ROWS`, `MESSAGE_CONFLICT`.

## 6. 출력

```text
/nilm/silver/power_clean/event_date=YYYY-MM-DD/run_id=<실행ID>/part-*.parquet
/nilm/silver/household_observation_daily/observation_date=YYYY-MM-DD/run_id=<실행ID>/part-*.parquet
/nilm/quarantine/power_silver/target_date=YYYY-MM-DD/run_id=<실행ID>/part-*.parquet
/nilm/manifests/job=power-silver-daily/target_date=YYYY-MM-DD/run_id=<실행ID>/manifest.json
```

`power_clean`: `message_id, household_id, device_id, measured_at_utc, event_date,
active_power, reactive_power, power_factor, current, topic, kafka_partition, kafka_offset,
source_file, ingested_at_utc, run_id, rule_version`. **대상 날짜의 행만 담는다.** 입력
파일에 섞여 있던 다른 날짜의 행은 그 날짜의 실행이 담당한다.

`household_observation_daily`: `household_id, observation_date, device_ids,
valid_measurement_count, observed_slot_count, expected_sample_count, coverage_ratio,
observation_status, quality_flags, first_measured_at, last_measured_at,
max_missing_seconds, duplicate_count, invalid_count, run_id, input_snapshot_id,
rule_version, config_version`.

가구 ID를 읽을 수 없는 오류는 특정 가구의 `invalid_count`에 넣지 않고 manifest의
`row_states.unattributable_rows`로 남긴다.

**파일 크기**: 가구를 디렉터리로 나누지 않는다(스몰 파일이 다시 늘어난다). 압축 후
128~256MiB를 목표로 출력 파티션 수를 정하고, 실제로 쓴 바이트를 manifest와 실행 기록에
남겨 **다음 실행이 그 실측값으로 분할 수를 정한다.** 데이터가 적으면 파일 하나로 둔다.
읽기 설정인 `spark.sql.files.maxPartitionBytes`는 출력 파일 크기를 보장하지 않는다.

## 7. 확정과 재처리

```text
WAITING_INPUT → RUNNING → VALIDATING → SUCCEEDED
                    └──────────────→ FAILED
```

1. 실행별 임시 경로(`/nilm/silver/.staging/...`)에 저장
2. 행 수·키 중복·상태값·날짜·파일 읽기 가능 여부 검증
3. 입력이 그대로인지 재확인 → 검증한 디렉터리를 최종 경로로 이동
4. 완료 manifest 기록
5. DB 트랜잭션으로 활성 실행 버전 변경

4와 5 사이에서 죽으면 파일은 확정됐는데 아무도 읽지 않는 상태가 된다. 다음 실행이 시작할 때
`recover`가 manifest를 찾아 **다시 계산하지 않고 DB만 반영한다.** `lake_dataset_version`이
가리키기 전의 출력은 소비하지 않는다.

### 동시 실행과 뒤늦은 복구

시작·검증·활성화·복구는 모두 **날짜별 단일 작성자 잠금** 안에서 돈다(PostgreSQL advisory
lock). 잠금을 못 잡으면 그 실행은 아무것도 하지 않고 `SKIPPED`로 끝난다(종료 코드 0 —
다른 실행이 그 날짜를 맡았다는 뜻이다). 잠금이 없으면 두 실행이 같은 날짜를 중복 계산하고,
시도 번호 채번(`MAX + 1`)이 경쟁해 유니크 제약 위반으로 죽는다.

날짜당 활성 버전이 하나라는 제약은 "**어느** 하나"만 보장하지 순서를 보장하지 않는다.
오래된 실행의 manifest를 뒤늦게 복구하면서 더 최신 실행을 활성 자리에서 밀어내면, 소비자가
이미 읽은 결과가 과거로 되돌아간다. 그래서 활성화할 때 **현재 활성 실행의 시도 번호와
비교**해서, 자기가 더 오래된 시도면 결과를 `SUPERSEDED`로만 등록하고 활성 자리는 건드리지
않는다. 이때 실행은 `SUCCEEDED`로 남고 `details.activated = false`가 붙는다.

manifest가 주장하는 시도 번호를 DB의 다른 실행이 이미 쓰고 있으면(DB 복원 등으로 실행
기록이 사라진 경우) 번호를 새로 붙여 순서를 꾸며내지 않고 `ConflictingRunHistory`로
멈춘다. DB와 레이크가 어긋난 상태라 사람이 봐야 한다.

| 상황 | 동작 |
|---|---|
| 입력·규칙·설정이 모두 같음 | 이미 성공한 결과 재사용(재계산 없음) |
| 늦게 도착한 데이터 | 새 입력 스냅샷으로 재계산, 이전 버전은 `SUPERSEDED` |
| 규칙·설정 변경 | 새 버전으로 재계산 |
| 실패 | 같은 날짜의 다음 시도(`attempt + 1`) |

**여러 `run_id`를 통째로 읽으면 같은 날짜가 중복된다.** 후속 집계는
`power_silver.catalog.SilverCatalog`로 날짜별 활성 실행 하나만 읽는다.

### 상위 재처리를 하위에 전달하기

과거 날짜가 새 버전으로 바뀌면 그 날짜를 소비한 하위 결과(Gold 일별 요약, baseline)는
낡은 것이 된다. 큐로 알리는 방식은 **알림이 유실되면 조용히 틀린 결과가 남는다.** 그래서
소비 사실 자체를 기록하고, 다시 계산할 날짜는 비교로 구한다.

```text
소비자: catalog.active_versions(...)로 상위 버전을 한 번에 고정 → 계산
      → commit.publish(manifest, depends_on=고정한 버전들)
        └ 활성화와 같은 트랜잭션에서 lake_dataset_dependency에 기록
소비자: catalog.dirty_dates("appliance_usage_daily", "household_observation_daily")
      → 다시 계산할 날짜 목록
```

| 판정 | 의미 |
|---|---|
| `NEVER_PROCESSED` | 상위는 확정됐는데 하위 결과가 아직 없다 |
| `UPSTREAM_CHANGED` | 내 활성 결과가 쓴 상위 실행이 더 이상 활성이 아니다 |
| `DEPENDENCY_UNKNOWN` | 하위 결과는 있는데 무엇을 썼는지 기록이 없다 |

비교가 곧 진실이므로 알림 유실로 인한 누락이 없다. 의존성은 manifest의 `depends_on`에도
실려서, 파일 확정 후 DB 반영 전에 죽어도 `recover`가 같이 되살린다.

`SilverCatalog.active_versions()`는 경로만이 아니라 `run_id`·`version_id`·
`input_snapshot_id`까지 한 조회로 돌려준다. 경로만 받으면 무엇을 소비했는지 기록할 수 없다.

### DB 스키마 (마이그레이션 `20260920_11`)

| 테이블 | 역할 |
|---|---|
| `lake_batch_run` | 실행 한 번: 작업·날짜·시도·상태·입력 스냅샷·규칙·설정·통계·오류 |
| `lake_dataset_version` | 날짜별 데이터셋의 실행 버전과 `ACTIVE`/`SUPERSEDED`. 날짜당 활성 하나(부분 유니크 인덱스) |
| `lake_dataset_dependency` | 실행 하나가 소비한 상위 데이터셋 버전(마이그레이션 `20260920_12`). 상위 쪽은 FK로 묶지 않아 상위 버전 행이 정리돼도 기록이 남는다 |

`batch_run`은 날짜별 성공 여부만 기록해 수정 재처리를 표현할 수 없어서 별도로 둔다.

## 8. 실행

```bash
# 로컬(Compose). HDFS가 떠 있어야 한다.
docker compose -f infrastructure/hdfs/docker-compose.yml up -d
docker compose -f infrastructure/local/compose.yaml run --rm power-silver power-silver run --date 2026-09-19

# 구간 재처리 / 상태 / 복구
docker compose -f infrastructure/local/compose.yaml run --rm power-silver \
  power-silver backfill --from 2026-09-01 --to 2026-09-19
docker compose -f infrastructure/local/compose.yaml run --rm power-silver power-silver status --json
docker compose -f infrastructure/local/compose.yaml run --rm power-silver power-silver recover --date 2026-09-19
```

standalone 클러스터에 제출할 때:

```bash
spark-submit --master spark://<host>:7077 --deploy-mode client \
  /usr/local/lib/python3.11/site-packages/power_silver/entrypoint.py run --date 2026-09-19
```

설정(환경변수)

| 변수 | 기본값 | 의미 |
|---|---|---|
| `DATABASE_HOST/PORT/NAME/USER/PASSWORD` | `localhost/5432/analysis_db/nilm_admin/…` | analysis_db |
| `HDFS_URL`, `HDFS_USER` | `http://namenode:9870`, `root` | manifest·파일 메타데이터용 WebHDFS |
| `LAKE_FS_URI` | `hdfs://namenode:9000` | Spark가 읽고 쓰는 파일시스템 |
| `LAKE_LOCAL_ROOT` | (없음) | 설정 시 HDFS 대신 로컬 디렉터리 사용(개발·테스트) |
| `BRONZE_BASE`, `BRONZE_MANIFEST_BASE` | `/nilm/bronze/power`, `/nilm/manifests/job=bronze-loader` | 입력 |
| `SILVER_BASE`, `QUARANTINE_BASE`, `MANIFEST_BASE`, `STAGING_BASE` | `/nilm/silver` 등 | 출력 |
| `BUSINESS_UTC_OFFSET_SECONDS` | `32400` | 업무 날짜 기준(한국은 서머타임이 없어 고정 오프셋이 안전하다) |
| `OBSERVATION_TARGETS_FILE` | 패키지 내 기본 파일 | 관측 대상 설정 |
| `RULE_VERSION` | `power-silver-v1` | 정제·판정 규칙 버전 |
| `VALID_COVERAGE_RATIO` | `0.95` | 유효 관측 임계값 |
| `FUTURE_SKEW_SECONDS` | `300` | 허용 미래 시각 |
| `INPUT_READY_GRACE_SECONDS` | `1800` | 날짜 경계 이후 적재기 생존 확인 유예 |
| `INPUT_WAIT_DEADLINE_SECONDS` | `21600` | 이 시간이 지나면 있는 입력으로 확정 |
| `MANIFEST_SCAN_BACK_DAYS` | `1` | 대상일 이전 수집일 폴더도 훑는 일수 |
| `POWER_CLEAN_TARGET_FILE_BYTES` | `201326592` | 출력 파일 목표 크기 |
| `SPARK_MASTER` | (없음 = 로컬) | `spark://host:7077` |
| `SPARK_SHUFFLE_PARTITIONS` | `0`(Spark 기본값) | 셔플 파티션 수 |

## 9. 기존 운영과의 연결 — 병행 검증

첫 배포는 **병행 검증 모드**다. DB의 `household_observation_daily`와 baseline 계산은
그대로 두고, Spark 결과는 Silver에만 쌓는다. 실시간 분석기가 계속 갱신하는 DB 관측 행을
Spark가 동시에 덮어쓰지 않는다.

비교해야 할 차이:

- 정상 1Hz 데이터에서는 DB 건수 기반 수집률과 Silver 구간 기반 수집률이 일치해야 한다.
- 중복·버스트 데이터에서는 Silver 쪽이 낮게 나오는 것이 **의도된 차이**다.

보존 정책 주의: 현재 `aggregation_service`의 retention은 **수집일 기준 compaction 완료
마커**를 본다. 이 배치는 **측정일 기준**이라 같은 의미의 마커를 만들지 않는다. Bronze 삭제
조건을 입력 파일별 처리 완료와 연결하기 전까지 기존 삭제 조건을 완화하지 않는다.

## 10. 테스트

```bash
# 이미지 안에서 Spark 로컬 모드로 전부 실행된다(JVM 포함).
docker build --target test --build-context analysis=ai/realtime-analysis-service \
  batch/power_silver_service

# 로컬에서 직접
pip install -e ai/realtime-analysis-service -e "batch/power_silver_service[dev]"
pytest batch/power_silver_service/tests
```

`spark` 표시 테스트는 PySpark나 JVM이 없으면 건너뛴다. Windows에서는 로컬 파일시스템
쓰기에 winutils가 필요하므로 Spark 테스트는 컨테이너에서 돌린다.

`postgres` 표시 테스트는 advisory lock과 부분 유니크 인덱스처럼 **PostgreSQL에서만 확인
가능한 것**을 돌린다(SQLite 테스트는 잠금의 in-process 대체 경로를 탄다). 일회용 DB만
지정해야 한다 — 스키마를 통째로 비운다.

```bash
docker run -d --name nilm-silver-test-pg -e POSTGRES_USER=test -e POSTGRES_PASSWORD=test \
  -e POSTGRES_DB=analysis_test -p 127.0.0.1:55433:5432 postgres:18-alpine
export TEST_DATABASE_URL=postgresql+psycopg://test:test@127.0.0.1:55433/analysis_test
pytest batch/power_silver_service/tests
```

| 테스트 | 기대 결과 |
|---|---|
| 정상 1Hz 하루 | 86,400구간, `VALID`, 누락 0 |
| 정확히 95% 관측 | 82,080구간, `VALID` |
| 95%보다 한 구간 부족 | `INSUFFICIENT_DATA` |
| 등록 가구 데이터 0건 | 관측일 행 생성, `SENSOR_GAP`, 누락 86,400 |
| 같은 Kafka 레코드 재적재 | 결과 변화 없음, `duplicate_count` 증가 |
| 같은 메시지 ID·다른 내용 | 두 행 모두 `MESSAGE_CONFLICT`로 격리 |
| 한국 시간 자정 전후 | 올바른 측정일로 분리 |
| 다음 날 도착한 전날 데이터 | 전날의 수정 실행에 포함, 이전 버전 `SUPERSEDED` |
| 파일 저장 후 DB 반영 실패 | 다음 실행이 재계산 없이 확정, 활성 버전 하나 |
| 오래된 실행의 지연 복구 | 최신 ACTIVE를 되돌리지 않음, 오래된 쪽은 `SUPERSEDED` |
| 다른 실행이 같은 날짜를 점유 | `SKIPPED`, 실행 기록·출력 없음 |
| 남의 시도 번호를 주장하는 manifest | `ConflictingRunHistory`로 중단 |
| 상위 날짜 재처리 | 소비한 하위 결과가 `UPSTREAM_CHANGED`로 잡힘 |
| 의존성 기록 없는 하위 결과 | `DEPENDENCY_UNKNOWN`으로 잡힘 |
| 하위 manifest 후 DB 실패 | 복구가 의존성까지 되살림 |
| 파티션 지연 | `WAITING_INPUT`, 결과 미확정 |
| 셔플 파티션 수 변경 | 보존 행·관측 수치 동일 |
