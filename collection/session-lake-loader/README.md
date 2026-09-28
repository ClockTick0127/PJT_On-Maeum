# session-lake-loader — 가전 사용 세션 HDFS 적재기

`analysis_db.appliance_usage_session`의 **기존 데이터와 이후 변경분(생성·변경·삭제)** 을
HDFS Bronze Parquet로 누락 없이 옮기고, 레이크 데이터만으로 DB의 최신 상태를 복원할 수
있는지 검증한다. 기존 DB 기반 집계(`aggregation_service`)는 그대로 유지한다.

일반 `run` 명령은 세션 변경분과 입력별 분석 처리 증거 outbox를 모두 처리한다.
분석 증거만 수동 실행하려면 `session-lake-loader receipt-incremental --json`을 사용한다.
증거는 `/nilm/bronze/analysis-processing-receipt`, manifest는
`/nilm/manifests/job=analysis-receipt-lake-loader`에 기록된다. 후속 일배치는 receipt가
참조한 `(session_id, session_version)`을 확정된 세션 파일과 대조한다.

```text
appliance_usage_session ──(PostgreSQL 트리거, 같은 트랜잭션)──▶ session_lake_outbox
                                                                     │ 미전달 이벤트
                                                                     ▼
                                    session-lake-loader ── 배치 배정 → Parquet → HDFS 임시 업로드
                                                          → 행 수·크기·체크섬 검증 → rename
                                                          → manifest 확정 → DB 전달 완료 기록
                                                                     │
        verify: manifest → Parquet → session_id별 최대 버전 복원 ──▶ 고정 DB 스냅샷과 비교
```

## 1. DB 스키마 (마이그레이션 `20260920_10`, `ai/realtime-analysis-service`)

| 객체 | 역할 |
|---|---|
| `appliance_usage_session.lake_version` | 내용 버전. INSERT 시 1, 내용이 실제로 바뀐 UPDATE마다 +1. 트리거만 갱신한다. |
| `session_lake_outbox` | 변경 한 건의 전체 내용(`payload` JSONB)과 전달 상태(`PENDING → ASSIGNED → DELIVERED`). 원본 세션 FK 없음. `(session_id, session_version)` UNIQUE. |
| `session_lake_batch` | 적재 배치: ID, 종류(`INITIAL`/`INCREMENTAL`), 대상 이벤트 범위·개수, 상태(`ASSIGNED`/`FAILED`/`COMPLETED`), 저장 날짜, manifest 경로, 시도 횟수, 마지막 오류. |
| 트리거 `trg_appliance_usage_session_lake_outbox` | `BEFORE INSERT OR UPDATE OR DELETE` 행 트리거. **마이그레이션 SQL로만** 생성되며 ORM 모델에는 없다(SQLite 회귀 테스트 유지). |

트리거 규칙

| 변경 | 처리 |
|---|---|
| 생성 | `lake_version = 1`, 전체 내용 + 가구·가전·관측일(부모 조인) 기록 |
| 종료·확률·임계값 등 내용 변경 | `lake_version + 1`, 전체 내용 기록 |
| 실제 값이 같은 갱신 | 이벤트 생성 생략, 버전 유지 (`to_jsonb(NEW) - 'lake_version' = to_jsonb(OLD) - 'lake_version'`) |
| 직접 삭제·CASCADE 삭제 | 부모 조인 없이 `OLD`만으로 `operation = DELETE`, `session_version = 마지막 버전 + 1` |

애플리케이션 코드는 `lake_version`을 쓰지 않는다. 잘못 써도 트리거가 덮어쓴다.
세션 변경과 outbox 기록은 같은 트랜잭션이므로 롤백되면 함께 사라진다.

이번 단계는 물리 삭제를 삭제 이벤트로 전달한다. **DB 보관기간에 따른 자동 세션 삭제는 추가하지
않았다.** 향후 보관기간 정리를 도입하면 "보관기간 만료 삭제"와 "데이터 정정 삭제"를 구분할 표시가
필요하다(예: outbox payload에 삭제 사유 추가).

## 2. 저장 경로·Parquet·manifest

```text
/nilm/bronze/appliance-session/
  ingest_date=YYYY-MM-DD/batch_id=<uuid>/part-00000.parquet          # 증분
  ingest_date=YYYY-MM-DD/batch_id=<uuid>/part-a01-00000.parquet      # 초기(시도 번호 포함)

/nilm/manifests/job=session-lake-loader/
  date=YYYY-MM-DD/manifest-<batch_id>.json
```

- `ingest_date`는 **배치 생성 시(UTC) 확정**해 재시도해도 바뀌지 않는다.
- Parquet 한 행 = 세션 한 버전. 스키마 버전 1: `event_id, operation(INSERT/UPDATE/DELETE/SNAPSHOT),
  session_id, session_version, is_deleted, changed_at, activity_daily_id, household_id, appliance_type,
  observation_date, started_at, ended_at, max_probability, decision_threshold, updated_at, batch_id,
  batch_kind, schema_version`. 시각은 UTC `timestamp[us]`, 확률은 `decimal(5,4)`.
- manifest: 배치 종류·ID, 파일 목록(경로·행 수·바이트·sha256·내용 해시), 행 수, 스키마 버전,
  대상 이벤트 범위, 완료 시각. 초기 적재는 스냅샷 ID(`pg_current_snapshot()`)와 시도 번호도 담는다.
- **manifest에 실린 파일만 확정 파일이다.** 세션 Bronze에는 전력 Bronze retention을 적용하지 않는다.

## 3. 적재 흐름과 장애 복구

증분(`incremental` / `run`)

1. `PENDING` 이벤트를 `event_id` 순으로 최대 `LOADER_BATCH_MAX_EVENTS`건 골라 새 배치에 배정(`ASSIGNED`).
   `updated_at` 커서가 아니라 전달 상태로 고르므로 늦게 커밋된 이벤트도 다음 주기에 잡힌다.
2. 트랜잭션을 닫은 뒤 Parquet 생성 → `<final>.tmp` 업로드 → 크기·sha256·행 수 읽어서 검증 → rename.
3. manifest를 같은 방식으로 확정.
4. 짧은 트랜잭션으로 이벤트 `DELIVERED`, 배치 `COMPLETED` 기록.

실패하면 배치는 `FAILED`(시도 횟수·단계·오류 저장), 이벤트는 그 배치에 그대로 남는다. 다음 주기에
**같은 배치 ID·같은 이벤트**로 재시도한다(지수 백오프 `LOADER_RETRY_BACKOFF_SECONDS`, 상한
`LOADER_RETRY_MAX_BACKOFF_SECONDS`).

| 중단 지점 | 재실행 결과 |
|---|---|
| 업로드 중 | 잔여 `.tmp` 덮어쓰기, 같은 이름의 확정 파일이 있으면 내용 동일성 확인 후 재사용. 내용이 다르면 오류로 중단(덮어쓰지 않음). |
| manifest 확정 후, DB 완료 전 | manifest의 파일들을 크기·sha256으로 검증한 뒤 DB만 완료 처리. 파일을 다시 쓰지 않는다. |
| 초기 적재 도중 | manifest가 없으므로 배치 디렉터리의 미확정 파일을 제거하고 새 스냅샷으로 다시 적재(시도 번호가 파일명에 붙는다). |

초기 전체 적재(`initial-load`)

- 트리거가 있어야 시작한다(변경 포착 먼저 활성화).
- `REPEATABLE READ` 스냅샷에서 모든 세션(미종료 포함)을 가구·가전·관측일과 조인해 읽고 로컬에
  spool한 뒤, **트랜잭션을 닫고** 업로드한다. 적재 중 발생한 변경은 outbox에 그대로 남아 증분으로 전달된다.
- 복구할 수 있는 것은 **현재 DB 상태**다. 이미 덮어쓴 과거 변경 이력은 복원하지 않는다.
- 같은 세션·버전이 SNAPSHOT 행과 outbox 행으로 중복될 수 있다. 버전이 내용을 고정하므로 복원 시 제거된다.
- 완료된 초기 적재가 있으면 `--allow-repeat` 없이는 다시 실행하지 않는다.

## 4. 검증기 (`verify`)

1. manifest를 모두 읽고 파일을 크기·sha256으로 검증한 뒤 Parquet를 읽는다.
2. `session_id`별 **최대 `session_version`** 을 선택한다. 삭제 표시는 최종 상태로 유지하고 낮은 버전의
   뒤늦은 도착은 무시한다. 같은 세션·버전인데 내용이 다르면 `conflict`로 실패한다.
3. `REPEATABLE READ` 스냅샷으로 DB 상태를 고정하고, **읽은 manifest에 속하지 않은 outbox 이벤트**를
   "전달 전"으로 분류해 비교한다. 전달 전 이벤트로 설명되는 차이는 `pending`, 설명되지 않는 차이는 `error`다.

| 판정 | 의미 |
|---|---|
| `CONTENT_MISMATCH` | 같은 버전인데 내용이 다름 |
| `MISSING_IN_LAKE` | DB에 있는데 레이크에 없고 전달 전 이벤트로도 설명되지 않음(초기 적재 누락 등) |
| `LAKE_BEHIND` / `LAKE_AHEAD` | 전달 전 이벤트로 설명되지 않는 버전 차이 |
| `MISSING_DELETE_IN_LAKE` | DB에서 사라졌는데 tombstone도, 전달 전 DELETE도 없음 |
| `NOT_LOADED_YET`, `LAKE_BEHIND_PENDING`, `DELETE_PENDING` | 전달 전 이벤트가 있는 정상 대기 |

종료 코드 0 = 일치, 1 = 오류.

## 5. 실행

로컬(Compose, `infrastructure/local`)

```bash
# HDFS(nilm-hdfs)와 로컬 스택이 같은 nilm-net에 있어야 한다.
docker compose -f infrastructure/hdfs/docker-compose.yml up -d
docker compose -f infrastructure/local/compose.yaml up -d --build session-lake-loader

# 초기 전체 적재 (run 서비스와 같은 DB 잠금을 쓰므로 동시에 돌아도 한쪽만 진행한다)
docker compose -f infrastructure/local/compose.yaml run --rm --no-deps session-lake-loader session-lake-loader initial-load

# 증분 1회 / 검증 / 상태
docker compose -f infrastructure/local/compose.yaml run --rm --no-deps session-lake-loader session-lake-loader incremental
docker compose -f infrastructure/local/compose.yaml run --rm --no-deps session-lake-loader session-lake-loader verify
docker compose -f infrastructure/local/compose.yaml run --rm --no-deps session-lake-loader session-lake-loader status
```

운영(EC2-B)은 `infrastructure/ec2-b/compose.yaml`의 `session-lake-loader` 서비스로 실행한다
(`SESSION_LAKE_LOADER_IMAGE`, Jenkins `Deploy bronze loader` 단계). 초기 적재는 배포 후 한 번
`docker compose run --rm --no-deps session-lake-loader session-lake-loader initial-load` 로 수행한다.

설정(환경변수)

| 변수 | 기본값 | 의미 |
|---|---|---|
| `DATABASE_HOST/PORT/NAME/USER/PASSWORD` | `localhost/5432/analysis_db/nilm_admin/…` | analysis_db |
| `HDFS_URL`, `HDFS_USER` | `http://namenode:9870`, `root` | WebHDFS |
| `SESSION_BRONZE_BASE` | `/nilm/bronze/appliance-session` | Parquet 루트 |
| `SESSION_MANIFEST_BASE` | `/nilm/manifests/job=session-lake-loader` | manifest 루트 |
| `LOADER_POLL_SECONDS` | `60` | `run` 실행 주기 |
| `LOADER_BATCH_MAX_EVENTS` | `5000` | 배치 하나의 최대 이벤트 수 |
| `LOADER_MAX_BATCHES_PER_CYCLE` | `20` | 한 주기에 만드는 최대 배치 수 |
| `LOADER_ROWS_PER_FILE` | `200000` | Parquet 파일 하나의 최대 행 수 |
| `LOADER_RETRY_BACKOFF_SECONDS` / `LOADER_RETRY_MAX_BACKOFF_SECONDS` | `30` / `900` | 실패 배치 재시도 백오프 |
| `LOADER_MAX_ATTEMPTS` | `0`(무제한) | 초과한 배치는 `FAILED`로 남고 `status`에 STALLED로 표시 |
| `LOADER_SPOOL_DIR` | 임시 폴더 | 초기 적재 Parquet 임시 보관 위치 |
| `LAKE_LOCAL_ROOT` | (없음) | 설정 시 HDFS 대신 로컬 디렉터리 사용(개발용) |
| `HTTP_HOST`, `HTTP_PORT` | `0.0.0.0`, `8000` | `/health`, `/ready`, `/metrics` |

## 6. 운영 상태 확인

- `session-lake-loader status [--json]`: 미전달 건수(PENDING/ASSIGNED), 최장 대기시간, 종류·상태별 배치 수,
  최근 실패 배치의 시도 횟수·단계·오류 원인, 미완료 초기 적재.
- Prometheus(`/metrics`, `run` 모드): `session_lake_outbox_undelivered_events`,
  `session_lake_outbox_oldest_undelivered_seconds`, `session_lake_batches{kind,status}`,
  `session_lake_batches_stalled`, `session_lake_batches_processed_total{kind,result}`,
  `session_lake_batch_failures_total{kind,stage}`, `session_lake_rows_written_total{kind}`,
  `session_lake_loader_last_cycle_timestamp_seconds`.
- DB: `session_lake_batch.last_error`, `details.errors`(최근 10회), `session_lake_outbox.delivery_status`.

## 7. 테스트

| 환경 | 위치 | 내용 |
|---|---|---|
| SQLite | `ai/realtime-analysis-service/tests`, `batch/aggregation_service/tests` | 기존 분석·집계 회귀 유지(`lake_version` 기본값 1) |
| SQLite | `collection/session-lake-loader/tests/test_loader.py` 등 | 배정·업로드 실패·재실행·manifest 후 중단 복구·중복/역순 복원·초기 적재 중 변경·상태 요약 |
| PostgreSQL 컨테이너 | `ai/.../tests/test_session_lake_postgres.py` | 실제 마이그레이션 upgrade/downgrade, 트리거 버전, 동일 값 갱신 생략, 롤백, 직접·CASCADE 삭제 |
| PostgreSQL 컨테이너 | `collection/session-lake-loader/tests/test_postgres_end_to_end.py` | 초기 적재(REPEATABLE READ) 중 변경 → 증분 → 삭제 → 검증, 실행 잠금 |

```bash
docker run -d --name nilm-lake-test-pg -e POSTGRES_USER=test -e POSTGRES_PASSWORD=test \
  -e POSTGRES_DB=analysis_test -p 127.0.0.1:55432:5432 postgres:18-alpine
export TEST_DATABASE_URL=postgresql+psycopg://test:test@127.0.0.1:55432/analysis_test
pytest ai/realtime-analysis-service/tests
pytest collection/session-lake-loader/tests
```

`TEST_DATABASE_URL`이 없으면 `postgres` 표시 테스트는 건너뛴다(Docker 이미지 `test` 단계 포함).
테스트 DB는 `DROP SCHEMA public CASCADE`로 초기화되므로 일회용 DB만 지정한다.

## 8. 이번 단계에서 제외한 것

Spark 실행 환경과 운영용 Silver 생성, 원본 전력 기반 관측일 재계산, 루틴 집계의 DB → 레이크 전환,
보고서 생성, Kafka 세션 토픽·CDC 도입, 레이크 자동 삭제, 다중 노드 성능 실험.
