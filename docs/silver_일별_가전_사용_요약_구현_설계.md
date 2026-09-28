# Silver 입력 기반 일별 가전 사용 요약 구현 설계

- 작성 기준: 2026-09-20 작업 트리의 코드. 배포 완료 여부는 별도 확인 대상.
- 문서 상태: 2026-09-20 1차 구현 반영. 입력별 receipt, receipt 레이크 적재,
  분석 커버리지, 세션 복원·일별 사용 요약, 완료 revision이 코드에 반영됐다.
- 목표: DB의 사용 세션을 직접 조회하던 일별 집계를 레이크 입력으로 계산하고, 기존 결과와 병행 검증한다.
- 범위: 세션 Bronze 복원 → Silver 일별 세션 조각 → Gold 일별 가전 요약. baseline 계산·보고서 렌더링·원본 모델 재추론은 후속 작업이다.

## 1. 주요 결정

1. 기존 `session-lake-loader`와 `power-silver-service`를 입력 생산자로 재사용한다.
2. 일별 가전 사용 요약은 집계 데이터이므로 `/nilm/gold/appliance_usage_daily`에 둔다. “Silver에서 생성”은 Silver 입력으로 생성한다는 의미로 사용한다.
3. 결과 한 행의 단위는 `(usage_date, household_id, appliance_type)`다. 실행 버전은 별도 관리한다.
4. 관측 대상 가구 × 적용 가능한 가전종류를 먼저 만들고 사용 결과를 LEFT JOIN한다. 세션이 없는 날도 행을 만든다.
5. `미사용(false)`과 `판정 불가(null)`를 구분한다. 원본 관측률이 높다는 이유만으로 가전 추론이 완료되었다고 가정하지 않는다.
6. 세션은 날짜로 먼저 거르지 않고, 고정한 입력의 전체 버전 중 최신 상태를 복원한 다음 날짜에 배정한다.
7. 초기 출력은 shadow 용도다. 기존 DB baseline·알림·활동지수 테이블을 덮어쓰지 않는다.

## 2. 현재 구현과 새로 필요한 부분

| 구성 | 현재 확인된 구현 | 이번 설계에서 필요한 작업 |
|---|---|---|
| 세션 레이크 전달 | DB 트리거/outbox, 초기 적재, 증분, DELETE, manifest, 버전 복원 검증 | Spark 복원기와 일별 요약 연결 |
| 전력 Silver | 정제 전력, 슬롯 기준 수집률, 관측일, ACTIVE 버전 카탈로그 | 관측일의 버전까지 고정해 읽기 |
| 배치 메타데이터 | `lake_batch_run`, `lake_dataset_version` | 새 job/dataset 등록, 의존성·재처리 관리 |
| 가전 분석 완료 증거 | 현재 세션 Bronze 스키마에는 없음 | 가구·가전·날짜별 분석 완료 계약 추가 |
| 일별 가전 요약 | 현재 Python 계산기에 일부 규칙 존재 | Spark 계산, 저장, 검증, 비교 기능 |

관련 코드:

- `collection/session-lake-loader/src/session_lake_loader/lake_schema.py`: 세션 Parquet 계약, `CONTENT_FIELDS`
- `collection/session-lake-loader/src/session_lake_loader/verifier.py`: 동일 버전 충돌, 최신 버전 복원
- `batch/power_silver_service/src/power_silver/schemas.py`: 관측일 출력 계약
- `batch/power_silver_service/src/power_silver/catalog.py`: 활성 버전 조회
- `batch/power_silver_service/src/power_silver/commit.py`: manifest와 DB 활성화
- `batch/aggregation_service/src/aggregation_service/daily_activity_index.py`: 논리적 사용 병합·횟수·시간
- `batch/aggregation_service/src/aggregation_service/baseline_updater.py`: 날짜별 첫 유효 사용

## 3. 데이터 흐름과 서비스 경계

```text
세션 Bronze manifest + Parquet ─→ 최신 세션 상태 복원 ─→ 날짜 경계로 자르기
                                                        ↓
                                               Silver session slices
                                                        ↓
                                       논리적 사용 병합·10초 조건 적용
                                                        ↓
ACTIVE Silver 관측일 ───────────────→ 관측·분석 품질 판정과 LEFT JOIN
분석 완료 증거 + 가전 적용 설정 ────────┘                 ↓
                                             Gold appliance_usage_daily
                                                        ↓
                                        이후 baseline / 활동지수 / 보고서
```

신규 서비스 제안: `batch/appliance_usage_service`, Python 패키지 `appliance_usage`, job `appliance-usage-daily`.
현재 `power-silver`가 사용하는 PySpark 3.5.6과 버전을 맞춘다. 새 Spark/Java 버전 도입은 이 작업에서 분리한다.
DB는 활성 출력 위치·실행 이력·완료 증거 등 제어 정보 조회에 사용한다. 사용 세션 본문은 레이크에서 읽는다.

## 4. 입력 계약

### 4.1 관측일

`household_observation_daily`의 대상 날짜 ACTIVE 버전 하나를 선택한다.

필수 필드: `household_id`, `observation_date`, `observation_status`, `expected_sample_count`,
`observed_slot_count`, `coverage_ratio`, `quality_flags`, `run_id`, `input_snapshot_id`, `config_version`.

- 현재 상태값은 `VALID`, `INSUFFICIENT_DATA`, `SENSOR_GAP`, `NOT_APPLICABLE`이다. DB 관측 테이블의 상태 집합과 혼동하지 않는다.
- 수집률은 기존 Silver 결과를 그대로 사용한다. 사용 세션 수로 재계산하지 않는다.
- 대상 날짜의 ACTIVE 버전이 없으면 `WAITING_INPUT`. 빈 관측일로 대체하지 않는다.
- `PARTIAL_DAY`는 설정상 일부 시간만 관측한 날이다. v1 정책에서는 baseline 표본에서 제외한다.
- `SilverCatalog.active_paths()`는 경로만 반환한다. 신규 `active_versions()` 인터페이스로 run/version ID·manifest 경로도 한 트랜잭션에서 고정할 수 있게 확장한다.

### 4.2 세션 변경 이력

기존 Bronze 필드를 그대로 읽는다:

```text
session_id, session_version, is_deleted, operation,
household_id, appliance_type, observation_date,
activity_daily_id, started_at, ended_at,
max_probability, decision_threshold, updated_at,
event_id, changed_at, batch_id, batch_kind, schema_version
```

- 파일은 확정 manifest 목록에 실린 것만 읽는다. 임시 파일 또는 폴더 glob 전체를 읽지 않는다.
- 같은 세션·버전은 `CONTENT_FIELDS`의 정규화된 값이 동일해야 한다. SNAPSHOT/INSERT 구분, event ID, 배치 ID 등 전달 메타데이터 차이는 충돌이 아니다.
- DELETE 행은 가구·가전·관측일이 null일 수 있다. 먼저 최신 버전을 선택한 뒤 삭제 여부를 적용한다.
- `max_probability`와 `decision_threshold`는 추적용이다. 이 단계에서 다시 비교해 세션을 임의로 버리지 않는다. 세션 생성기의 시간·히스테리시스 판정을 대체할 수 없다.
- 현재 계약에는 모델 버전·분석 진행률·가전 개체 ID가 없다. 이를 가정하거나 만들어 채우지 않는다. 모델 버전 미상은 manifest에 `UNKNOWN`으로 남긴다.
- 같은 가전종류의 여러 물리 기기는 구별할 수 없다. v1 집계 단위는 가전종류다.

### 4.3 분석 완료 증거 — 신규 필수 계약

`세션 0건`은 정상 미사용, 분석 중단, 전달 지연 모두에서 발생한다. outbox 대기 건수 0도 분석 완료의 증거는 아니다.

신규 `analysis_daily_completion` 계약을 정의한다:

| 필드 | 의미 |
|---|---|
| `household_id`, `appliance_type`, `target_date` | 완료 판정 범위 |
| `completion_id`, `revision` | 증거 버전 |
| `input_snapshot_id` 또는 처리한 입력 구간 목록 | 어느 전력 입력을 분석했는지 |
| `analysis_pipeline_version`, `model_version` | 실제 실행한 분석 버전 |
| `expected_inference_units`, `processed_inference_units` | 모델 윈도우·슬롯 등 명시된 단위의 처리량 |
| `status`, `failed_unit_count` | COMPLETE/INCOMPLETE/ERROR |
| 세션 전달 확인 목록 또는 완료 장벽 참조 | 해당 분석 결과의 세션 변경분이 레이크에 도달했다는 증거 |

분석 서비스가 입력 구간을 마감하고 변경 이벤트를 기록한 뒤 완료 증거를 발행한다. 적재기는 그 증거가 참조하는 이벤트의 전달을 확인해야 한다.
`MAX(event_id)` 이하가 모두 완료되었다고 추정하지 않는다. ID 할당 순서와 DB 커밋 순서가 다를 수 있으므로 실제 이벤트 집합 또는 직렬화된 완료 장벽을 사용한다.
전력 Silver가 재처리되면 기존 완료 증거가 새 입력까지 포괄하는지 다시 검사한다. 입력 버전이 다르다는 이유만으로 자동 호환 처리하지 않는다.

이 계약이 구현되기 전에도 shadow 계산은 가능하다. 단, `analysis_status=UNKNOWN`, `baseline_eligible=false`로 기록하고 세션이 없다는 이유로 확정 미사용을 만들지 않는다.
완료 증거는 종료가 누락된 세션을 자동으로 종료시키는 근거가 아니다.
shadow 비교기는 품질 마스킹 전의 진단용 논리 사용 계산값을 별도 비교 산출물로 저장한다. 이 값은 운영 Gold의 확정 사용값으로 소비하지 않는다. 따라서 완료 계약 구현 전에도 기존 계산기와 산술·병합 규칙을 비교할 수 있다.

### 4.4 가전 적용 설정

기존 6종(`KETTLE`, `INDUCTION`, `IRON`, `MICROWAVE`, `HAIR_DRYER`, `VACUUM_CLEANER`)과 관측 가구 목록을 사용한다.
이는 가전 보유 여부가 아니라 모델 적용 대상 목록이다. 가전 미보유를 추정하지 않는다.
향후 가구별 모델 지원 여부를 추가할 때는 유효 기간과 설정 버전을 보존한다. 미지원 가전은 `NOT_APPLICABLE`로 남긴다.

## 5. 출력 계약

### 5.1 Silver `appliance_session_daily_slices`

행 단위: `(usage_date, session_id)`; 현재 실행에서 선택된 최신 버전만 포함.

| 필드 | 타입 | 정의 |
|---|---|---|
| `usage_date`, `household_id`, `appliance_type` | date/string/string | 실제 겹치는 업무 날짜와 분류 |
| `session_id`, `session_version` | string/int | 원천 식별자 |
| `source_observation_date` | date | DB 부모 관측일; 비교·추적용 |
| `original_started_at`, `original_ended_at` | timestamp UTC | 원본 구간 |
| `clipped_started_at`, `clipped_ended_at` | timestamp UTC | 해당 날짜로 자른 구간 |
| `duration_us` | long | 구간 길이, 마이크로초 정수 |
| `started_in_day`, `end_imputed` | boolean | 당일 시작 여부, 종료시각 대체 여부 |
| `quality_flags` | array<string> | OPEN_SESSION 등 |
| `run_id`, `input_snapshot_id`, `rule_version` | string | 실행 추적 |

삭제 상태·격리 행은 slice 출력에서 제외하되, 삭제 tombstone은 상태 복원 이력에서 유지한다.
미종료 세션의 진단용 slice에는 날짜 종료시각을 대입하고 `end_imputed=true`로 표시한다. 확정 요약에는 쓰지 않는다.

### 5.2 Gold `appliance_usage_daily`

| 필드 | 타입 | 정의 |
|---|---|---|
| `usage_date`, `household_id`, `appliance_type` | date/string/string | 업무 키 |
| `observation_status`, `coverage_ratio` | string/double | Silver 관측일에서 복사 |
| `analysis_status` | string | COMPLETE/INCOMPLETE/ERROR/UNKNOWN |
| `usage_status` | string | USED/NOT_USED/UNKNOWN/NOT_APPLICABLE |
| `baseline_eligible` | boolean | baseline 표본 포함 여부 |
| `is_used` | boolean nullable | 확정 사용 true, 확정 미사용 false, 판정 불가 null |
| `logical_use_count` | long nullable | 해당 날짜와 겹치는 유효 논리 사용 수 |
| `usage_start_count` | long nullable | 유효 논리 사용 중 해당 날짜에 시작한 수 |
| `usage_duration_us` | long nullable | 유효 논리 사용의 실제 세션 시간 합계 |
| `first_use_at` | timestamp nullable | 첫 유효 논리 사용의 clipped 시작 |
| `first_use_second` | int nullable | 업무 자정 이후 초, 소수 초 버림 |
| `last_use_end_at` | timestamp nullable | 마지막 유효 논리 사용의 끝 |
| `source_session_count`, `rejected_short_use_count` | long | 진단용 입력/10초 미만 사용 수 |
| `open_session_count`, `overlap_count` | long | 품질 진단 |
| `quality_flags` | array<string> | 정렬·중복 제거한 품질 표시 |
| `observation_run_id`, `completion_id` | string nullable | 고정된 상위 버전 |
| `run_id`, `input_snapshot_id`, `rule_version`, `config_version` | string | 요약 재현 정보 |
| `schema_version`, `calculated_at` | int/timestamp | 계약·계산 시각 |

추적 데이터는 manifest에도 보존한다. 모든 session ID를 요약 행 배열에 모으지 않는다. slice 데이터셋을 조인해 추적한다.
UNKNOWN 행의 확정 횟수·시간·시각은 null이다. NOT_USED 행은 횟수·시간 0, 시각 null이다.
가전별 소비전력량은 모델이 가전별 전력을 제공하지 않으므로 이번 출력에 넣지 않는다.

## 6. 계산 규칙

### 6.1 최신 상태 복원

1. manifest 파일 해시·행 수·스키마를 검사하고 명시적 Spark schema로 읽는다.
2. `(session_id, session_version)`별 내용 동일성을 검사한다. 충돌은 임의 선택하지 않고 실행 실패 처리한다.
3. 동일 내용 중복을 제거한다.
4. `row_number() over (partition by session_id order by session_version desc)`로 최신 버전을 선택한다.
5. 최신 행이 DELETE이면 현재 사용에서 제외한다. tombstone은 이후 낮은 버전 도착에 의한 부활 방지용으로 보존한다.
6. 살아 있는 행의 ID·가전종류·구간·확률 범위를 검증한다. 관련 가구·날짜를 알 수 있는 오류는 해당 요약을 UNKNOWN으로 만들고, 영향 범위를 알 수 없으면 실행 실패한다.

초기 구현은 고정한 manifest 전체를 읽어 복원한다. 입력 크기 증가 후에는 상태 스냅샷 + 아직 처리하지 않은 manifest 집합으로 최적화한다.
이는 해당 snapshot 시점에 알려진 최신 정정 상태다. 과거 날짜의 당시 판단을 재현하려면 당시 snapshot을 지정해야 하며, 현재 manifest 전체를 다시 읽는 것은 수정된 과거 결과를 생성하는 작업이다.
증분 커서는 event ID 최댓값이 아니라 처리한 manifest ID 집합이다. SNAPSHOT에는 event ID가 없고 늦게 커밋된 작은 ID도 도착할 수 있다.
완료된 initial-load가 없다면 과거 데이터가 완전하다고 선언하지 않는다.

### 6.2 날짜 경계와 미종료 세션

Spark 세션 시간대는 기존과 동일하게 UTC로 고정한다. 업무 날짜 경계는 `Asia/Seoul` 자정에서 UTC instant로 변환한다.

```text
day_start = target_date 00:00 Asia/Seoul 의 UTC instant
day_end   = 다음 날 00:00 Asia/Seoul 의 UTC instant
대상 조건 = started_at < day_end AND (ended_at IS NULL OR ended_at > day_start)
slice_start = max(started_at, day_start)
slice_end   = min(ended_at 또는 day_end, day_end)
```

- 모든 구간은 `[start, end)`다. 자정에 종료된 세션은 다음 날에 포함하지 않는다.
- `slice_end <= slice_start`는 사용 기여 0이며 진단 통계에 남긴다. 음수 원본 구간은 오류다.
- 날짜별로 먼저 자르고 병합한다. 자정 양쪽 6초+6초는 각각 10초 미만이므로 두 날 모두 유효 사용이 아니다.
- 미종료 세션은 진단 slice만 생성하고 해당 가구·가전·날짜를 UNKNOWN 처리한다. 현행 코드의 하루 끝까지 사용한 것으로 계산하는 동작과 다른 품질 정책이다.
- 오래 열린 세션을 임의로 종료하지 않는다. OPEN_SESSION 경고와 수정을 요청할 수 있는 지표를 남긴다.

### 6.3 논리적 사용 병합

| 가전종류 | 허용 세션 간격 |
|---|---:|
| KETTLE / MICROWAVE / HAIR_DRYER / VACUUM_CLEANER | 60초 |
| INDUCTION | 120초 |
| IRON | 300초 |

그룹 키는 `(usage_date, household_id, appliance_type)`다. 시작·끝·session ID 순으로 안정적으로 정렬한다.
이전 모든 행의 최대 종료시각을 계산한다. `lag(ended_at)`만 사용하면 포함 관계인 세션에서 그룹이 잘못 갈라진다.

Spark SQL 개념 예시(단계별 CTE로 window 중첩을 피한다):

```sql
WITH previous AS (
  SELECT *, MAX(end_us) OVER (
    PARTITION BY usage_date, household_id, appliance_type
    ORDER BY start_us, end_us, session_id
    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
  ) AS previous_max_end
  FROM closed_slices
), boundaries AS (
  SELECT *, CASE WHEN previous_max_end IS NULL
    OR start_us - previous_max_end > merge_gap_us
    THEN 1 ELSE 0 END AS starts_group
  FROM previous
), numbered AS (
  SELECT *, SUM(starts_group) OVER (
    PARTITION BY usage_date, household_id, appliance_type
    ORDER BY start_us, end_us, session_id
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS logical_group
  FROM boundaries
)
SELECT usage_date, household_id, appliance_type, logical_group,
       MIN(start_us) AS first_us, MAX(end_us) AS last_us,
       SUM(end_us - start_us) AS active_us,
       MIN(original_start_us) AS original_first_us
FROM numbered
GROUP BY usage_date, household_id, appliance_type, logical_group;
```

- 간격은 `<=`이면 병합한다. 꺼진 간격은 active_us에 더하지 않는다.
- active_us가 `10,000,000` 이상인 논리 사용만 인정한다. 10초 조건은 개별 세션이 아니라 병합 후 적용한다.
- 서로 다른 session ID의 시간 중복은 현행 계산이 중복 합산한다. v1에서는 중복을 표시하고 해당 요약을 UNKNOWN으로 막아 확정 시간 과대계상을 방지한다.
- 중복 구간 합집합 계산은 별도 규칙 버전으로 도입할 수 있다. 동일 종류의 복수 기기인지 데이터 오류인지 확인 없이 조용히 규칙을 바꾸지 않는다.
- duration은 정수 마이크로초로 합산하고 표시할 때만 초로 변환한다. 활동지수의 Python round와 Spark round 차이는 별도 어댑터에서 맞춘다.

### 6.4 사용 횟수·첫 사용 시각

```text
logical_use_count  = 유효 논리 사용 그룹 수
usage_start_count  = day_start <= original_first_us < day_end 인 그룹 수
usage_duration_us  = 유효 그룹 active_us 합
first_use_at       = 유효 그룹 first_us 최솟값
first_use_second   = floor((first_use_at - day_start) / 1초)
```

전날 23:59~당일 00:05 세션은 당일 사용으로 포함하고 첫 사용은 00:00, 당일 시작 횟수는 0이다.
`logical_use_count`와 `usage_start_count`를 분리해야 활동지수에서 자정을 넘는 사용을 새 사용으로 중복 집계하지 않는다.

### 6.5 품질 판정과 빈 행

순서: 적용 대상 여부 → 관측 품질 → 분석 완료/전달 증거 → 세션 품질 → 사용 여부.

| 조건 | usage_status / is_used | baseline_eligible |
|---|---|---|
| 가전 미지원 또는 관측 NOT_APPLICABLE | NOT_APPLICABLE / null | false |
| 관측 VALID 아님 또는 PARTIAL_DAY | UNKNOWN / null | false |
| 분석 미완료·실패·미확인 또는 변경분 전달 대기 | UNKNOWN / null | false |
| 미종료·영향 범위를 아는 잘못된 세션·시간 중복 | UNKNOWN / null | false |
| 위 조건 통과 + 유효 논리 사용 1개 이상 | USED / true | true |
| 위 조건 통과 + 유효 논리 사용 없음 | NOT_USED / false | true |

10초 미만 세션만 있는 경우는 분석이 완료되었다면 NOT_USED다. 현재의 유효 사용 정의에 따른 미사용이며 세션 원본 자체가 없었다는 뜻은 아니다.
진단용으로 발견한 세션 수는 남기되 UNKNOWN 행을 NOT_USED로 fillna하지 않는다.
동일 세션·버전의 내용 충돌은 위 행 단위 품질 판정 이전에 실행 전체를 실패시킨다.

## 7. 현행 DB 집계와 비교 시 의도된 차이

| 항목 | 현재 코드 | 신규 기본 정책 |
|---|---|---|
| 관측률 | DB sample_count 기반 | 기존 Silver의 관측 슬롯 기반 |
| 미종료 | day_end로 대체 | 진단만 계산, 표본 제외 |
| 시간 중복 | 세션 길이를 합산 | 표시 후 표본 제외 |
| baseline 날짜 배정 | 부모 observation_date로 조인 후 해당 날짜로 자름 | 실제 구간이 겹치는 모든 날짜 |
| 활동지수 날짜 배정 | 실제 시간 겹침 기준 | 동일 |
| 부분 관측일 | DB VALID 조건으로 포함 가능 | PARTIAL_DAY 제외 |
| 분석 누락 | 관측 VALID와 세션 부재만으로 미사용 가능 | 분석 완료 증거 없으면 UNKNOWN |

따라서 전체 데이터에 무조건 완전 일치를 요구하지 않는다. 정상·완료·비중복·당일 세션 fixture는 정확히 일치해야 한다.
경계 사례는 위 차이 코드별로 분류한다. 기존 baseline의 부모 날짜 배정에 의존하는 운영 결과가 있으면 전환 전에 영향 건수를 보고한다.
전환 시 baseline의 sample_days는 가전별 eligible 날짜 수가 된다. 동일 가구라도 분석 품질이 다르면 가전별 분모가 달라지는 정책 변경이다.

## 8. 입력 스냅샷과 완료 프로토콜

`input_snapshot_id`는 아래 canonical manifest의 SHA-256으로 만든다:

```text
target_date
observation version_id + run_id + manifest checksum
session manifest IDs + 파일 경로/sha256/행 수
completion 증거 버전 + 입력 범위
가전 적용 설정 hash + rule_version + schema_version
선택적으로 직전 세션 상태 스냅샷 ID
```

실행 중 ACTIVE 버전이 바뀌어도 입력 경로를 다시 조회해 섞지 않는다. 새 버전은 dirty-date 작업으로 처리한다.
세션 전달 완료 manifest는 파일 완성의 증거이며 하루 전체 분석 완료 증거와 구분한다.

저장 경로 제안:

```text
/nilm/silver/appliance_session_daily_slices/usage_date=D/run_id=R/part-*.parquet
/nilm/gold/appliance_usage_daily/usage_date=D/run_id=R/part-*.parquet
/nilm/manifests/job=appliance-usage-daily/target_date=D/run_id=R/manifest.json
/nilm/staging/appliance-usage-daily/run_id=R/...
```

1. 날짜별 실행 잠금 획득 및 선행 입력 확인.
2. 스냅샷 고정, 같은 입력·규칙의 ACTIVE 결과가 있으면 재사용.
3. 임시 경로에 두 출력을 기록하고 읽기 검증.
4. 불변 run 경로로 rename, 출력 목록·검증 결과·입력 참조가 있는 manifest 기록.
5. 단일 DB 트랜잭션으로 두 dataset 버전과 run 성공 상태를 반영.
6. 소비자는 ACTIVE 버전을 한 번에 고정하여 읽는다. `run_id=*` 전체를 읽지 않는다.

파일 rename과 DB commit은 하나의 트랜잭션이 아니다. manifest 이후 DB 실패 시 파일을 검증하고 활성화만 복구한다.
날짜별 단일 작성자 잠금은 시작·검증·활성화·복구 모두 적용한다. 오래된 완료 실행을 복구하면서 더 최신 ACTIVE 결과를 되돌리지 않도록 입력 revision 또는 expected-current CAS 검사도 추가한다.
기존 `SilverCommitRepository(job_name=...)`는 재사용 후보지만, 위 경쟁·복구 조건을 검증한 후 공통화한다. 기존 구현이 이 조건을 모두 보장한다고 가정하지 않는다.

실행 상태는 기존 WAITING_INPUT/RUNNING/VALIDATING/SUCCEEDED/FAILED를 재사용한다.
SUCCEEDED는 데이터셋 생성 성공이며 모든 가구의 baseline 적격을 뜻하지 않는다. UNKNOWN 수와 이유를 metrics/manifest에 별도로 남긴다.
초기 적재·관측일 등 필수 입력이 없으면 WAITING_INPUT. 구조적 충돌·파일 손상은 FAILED. 일부 가구의 분석 완료 증거 부족은 UNKNOWN 행으로 표현할 수 있다.

## 9. 수정·삭제·지연 도착 재처리

- 새 세션 상태와 이전 상태가 영향을 주는 날짜의 합집합을 dirty-date로 등록한다.
- 날짜나 가구·가전이 바뀌면 이전 위치와 새 위치를 모두 재집계한다.
- 삭제는 직전 살아 있던 버전 또는 직전 상태 인덱스로 가구·가전·기간을 찾는다. DELETE 행의 null 분류값으로 날짜를 결정하지 않는다.
- 삭제 이전 내용이 전혀 없으면 영향 범위를 추정하지 않는다. 추적 가능한 이전 snapshot/index 복구 전까지 운영 확정을 막는다.
- 이전 미종료 세션의 종료·삭제는 과거 공개한 날짜까지 되돌아가 수정해야 한다. 고정 1~3일 lookback만으로 처리하지 않는다.
- 상위 관측일 ACTIVE 버전 또는 분석 완료 증거 변경도 dirty-date를 만든다.
- dirty 작업 등록은 버전 활성화와 같은 DB 트랜잭션의 outbox/queue에 기록하고, 주기적으로 의존성 차이를 비교하여 유실을 복구한다.
- 각 대상 날짜는 고정된 입력으로 전량 재계산해 새 run으로 교체한다. 숫자에 단순 +/- 누적하지 않는다.
- 날짜 D의 요약 변경은 D~D+27 기준일의 28일 baseline에 영향을 줄 수 있다. 이미 생성된 범위까지만 갱신하고 보고서도 버전을 남긴다.
- 과거 baseline 재계산으로 과거 알림을 자동 재발송하지 않는다. 최신 적용 기준보다 오래된 결과가 현재 DB baseline을 덮어쓰지 않게 한다.
- DB 보관기간 만료 삭제와 데이터 정정 삭제는 현재 계약상 구별되지 않는다. 보관기간 정리를 도입하기 전에 삭제 사유를 추가해야 장기 레이크 이력이 지워지지 않는다.

## 10. 후속 baseline 연결 계약

최근 28일의 Gold 활성 버전을 날짜별로 고정한 뒤 가구·가전별로 계산한다:

```text
sample_days = count(baseline_eligible = true)
active_days = count(baseline_eligible = true AND is_used = true)
사용일비율 = active_days / sample_days
첫 사용 분포 = eligible AND used 인 날짜별 first_use_second
```

- NOT_USED도 sample_days에 포함한다. UNKNOWN/NOT_APPLICABLE은 분모·분자에서 제외한다.
- 사용일 없음 또는 표본 부족 처리는 기존 baseline 정책을 명시적으로 적용한다.
- P10/P50/P90은 기존 `ceil(p*N)`번째 값을 고르는 규칙을 유지한다. 일별 요약 생성 단계에서는 백분위수를 계산하지 않는다.
- 14일 최소 표본, 요일별 최소 4일, 사용일비율 0.70은 후속 baseline 규칙 버전으로 관리한다.
- 현재 reliability 식은 최소 표본 이상에서 항상 1이다. 이번 일별 요약 구현 중에 임의로 변경하지 않는다.

## 11. 모듈과 실행 인터페이스 제안

```text
batch/appliance_usage_service/
  src/appliance_usage/
    config.py / schemas.py / constants.py
    input_snapshot.py        관측 버전·세션 manifest·완료 증거 고정
    session_state.py         버전 충돌 검사, 최신 상태·tombstone 복원
    readiness.py             분석·전달 완료 판단
    daily_slices.py          날짜 경계 자르기
    logical_uses.py          window 함수 기반 병합
    daily_summary.py         spine 생성·LEFT JOIN·품질/사용 판정
    validation.py            계약·산술·파일 검증
    writer.py / catalog.py / commit.py
    invalidation.py          수정 영향 날짜와 후속 작업 등록
    comparison.py            DB 계산기와 shadow 비교
    job.py / cli.py / entrypoint.py
  tests/
  Dockerfile / pyproject.toml / README.md
```

제안 명령(아직 구현되지 않음):

```text
appliance-usage run --date 2026-09-19 --mode shadow
appliance-usage backfill --from 2026-09-01 --to 2026-09-19
appliance-usage recover --date 2026-09-19
appliance-usage compare --date 2026-09-19
appliance-usage status --json
```

단일 노드 시험은 명시적 `local[2]`, 적은 셔플 파티션에서 시작하고 측정 후 조정한다. 현재 power-silver의 “master 미설정”을 local 모드 보장으로 사용하지 않는다.
Spark 윈도우 연산은 가구·가전·날짜로 분할한다. 원천 세션 전체를 driver에 collect하거나 pandas로 모으지 않는다.
기간 backfill은 최신 세션 상태를 한 번 복원해 여러 날짜에 재사용한다. 날짜마다 전체 Bronze를 재스캔하는 비용을 피한다.
노드 확장 시 동일 입력 snapshot과 설정으로 실행해 정렬된 업무 결과 해시를 비교한다. run_id·계산 시각·파일명·파일 분할 차이는 결과 비교에서 제외한다.

## 12. 검증·완료 기준

| 사례 | 기대 결과 |
|---|---|
| 정상 관측 + 분석 완료 + 세션 없음 | 6종 행, NOT_USED, 분모 포함 |
| 정상 관측 + 분석 완료 증거 없음 | UNKNOWN, 분모 제외 |
| 원본 0건 / NOT_APPLICABLE | 각각 UNKNOWN / NOT_APPLICABLE |
| 완료된 9.999999초 세션 / 10초 세션 | 각각 미사용 / 사용 |
| 6초 + 60초 간격 + 6초 전자레인지 | 한 사용, 실제 12초, 첫 세션 시작시각 |
| 6초 + 60초 초과 간격 + 6초 | 둘 다 10초 미만, 미사용 |
| 인덕션 120초·다리미 300초 경계 | 경계 포함 병합 |
| 긴 세션 안에 짧은 세션 포함 | previous-max 기준으로 그룹 유지, 중복 플래그, UNKNOWN |
| 전날 23:59~당일 00:05 | 당일 첫 사용 00:00, start_count=0 |
| 정확히 자정에 종료 | 다음 날 slice 없음 |
| 자정 양쪽 6초씩 | 두 날 모두 10초 미만 |
| 미종료 → 종료 이벤트 도착 | UNKNOWN → 새 버전 확정, 관련 과거 날짜 재계산 |
| SNAPSHOT과 동일 버전 INSERT 중복 | 동일 결과 |
| 동일 버전·다른 내용 | FAILED, ACTIVE 유지 |
| v3 DELETE 뒤 늦은 v2 UPDATE | 삭제 유지 |
| 날짜·가구 변경 / DELETE | 이전·새 날짜 모두 재계산 |
| 입력 또는 설정 변경 | 새 snapshot/run, 이전 버전 SUPERSEDED |
| manifest 후 DB 실패 | 파일 재계산 없이 복구 |
| 오래된 실행의 지연 복구 | 최신 ACTIVE를 되돌리지 않음 |
| 중복 실행 / 다중 worker 수 | 활성 버전 하나, 업무 결과 동일 |
| 정상 데이터 DB 비교 | 첫 사용·횟수·시간 일치, 정책 차이는 별도 분류 |

출력 불변 조건:

- 업무 키 중복 0, spine 행 수와 출력 행 수 일치.
- `baseline_eligible => observation_status=VALID AND analysis_status=COMPLETE` 및 품질 조건 충족.
- `USED => first_use_second IS NOT NULL`, `0 <= first_use_second < 86400`.
- `NOT_USED => 횟수=0 AND 시간=0 AND 시각=NULL`.
- `UNKNOWN => is_used IS NULL AND baseline_eligible=false`.
- 정상 비중복 행의 `usage_duration_us <= 86,400,000,000`.
- `usage_start_count <= logical_use_count`.
- 두 출력의 run_id·snapshot이 같고 모든 참조 manifest를 재조회할 수 있음.

지표: 단계별 소요시간, 입력 버전 수, 최신 세션 수, 중복/충돌/삭제/미종료 수, UNKNOWN 사유별 건수,
eligible 비율, 재처리 지연, DB 대비 차이, Spark shuffle/spill, 출력 파일 수·크기.
Prometheus label에는 household_id/session_id를 넣지 않고 상세 내용은 진단 데이터에 저장한다.

구현 완료의 1차 기준은 레이크만으로 위 fixture를 통과하고 shadow 출력이 재현되는 것이다.
운영 baseline 전환의 추가 기준은 분석 완료 증거 확보, 의도된 차이의 검토, 후속 재처리와 이전 버전 복구 검증이다.

## 13. 구현 순서

1. 입력 스키마와 snapshot, 최신 세션 복원·삭제·충돌 처리.
2. slice와 Spark 논리 사용 계산, 관측 spine과 품질 판정, shadow 저장.
3. 버전 카탈로그·완료 manifest·재실행·복구·dirty-date 연결.
4. 분석 완료 계약 생산자/적재 확인 구현. 그전까지 UNKNOWN 정책 유지.
5. DB 병행 비교 및 정책 차이 검증.
6. Gold 소비 baseline·보고서 연결 후 운영 전환. 멀티노드 성능 비교는 같은 snapshot으로 별도 수행.

## 14. 공식 구현 참고

- [Spark 3.5.6 Window Functions](https://spark.apache.org/docs/3.5.6/sql-ref-syntax-qry-select-window.html): 최신 버전 선택과 누적 최대 종료시각·그룹 번호 계산.
- [Spark 3.5.6 Parquet Files](https://spark.apache.org/docs/3.5.6/sql-data-sources-parquet.html): 명시적 스키마와 Parquet 입출력. ACTIVE 버전·다중 출력 확정은 이 프로젝트가 추가로 관리하는 계약이다.
