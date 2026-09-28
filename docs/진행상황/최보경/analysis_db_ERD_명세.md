# Analysis DB MVP ERD 명세

> 작성일: 2026-09-08  
> 대상 서비스: `realtime-analysis-service`  
> DB: PostgreSQL `analysis_db`

## 1. 목적

`analysis_db`는 실시간 전력 원본을 저장하는 DB가 아니다. AI 분석 서비스가 사용하는 모델 정보,
가구별 활동 기준선, 이상 감지 정책, 일 단위 데이터 품질과 가전 사용 결과를 저장한다.

| 데이터 | 저장 위치 |
| --- | --- |
| 초 단위 원본 전력 | Kafka 및 S3 |
| 최근 299초 추론 버퍼 | `realtime-analysis-service` 메모리 |
| AI 모델 파일 | 로컬 볼륨 또는 S3 |
| AI 모델 위치·상태 | `model_artifact` |
| 가구별 평소 활동 기준 | `routine_baseline` |
| 이상 감지 조건 | `analysis_policy` |
| 가구별 일일 데이터 품질 | `household_observation_daily` |
| 가전별 일일 활동 요약 | `household_activity_daily` |
| 개별 가전 사용 시간 | `appliance_usage_session` |

---

## 2. 테이블 관계

```text
household_observation_daily
가구 한 곳의 하루 데이터 관측 결과
              1
              │
              │ observation_daily_id
              ▼
              N
household_activity_daily
가전별 하루 사용 결과 요약
              1
              │
              │ activity_daily_id
              ▼
              N
appliance_usage_session
개별 가전 사용 세션
```

두 관계 모두 자식 테이블이 자체 UUID PK를 가지므로 **1:N 비식별 관계**다.

| 부모 | 자식 | 관계 | FK |
| --- | --- | --- | --- |
| `household_observation_daily` | `household_activity_daily` | 1:0..N | `observation_daily_id` |
| `household_activity_daily` | `appliance_usage_session` | 1:0..N | `activity_daily_id` |

- 일일 관측 결과 하나에서 대상 가전별 활동 요약이 여러 건 생성될 수 있다.
- 가전별 일일 활동 한 건에서 사용 세션이 여러 건 생성될 수 있다.
- 사용하지 않은 가전은 일일 활동의 `event_count`가 0이며 사용 세션은 없다.
- `model_artifact`, `routine_baseline`, `analysis_policy`는 서비스가 직접 조회하는
  설정·기준 테이블이므로
  MVP에서는 물리 FK 없이 독립적으로 둔다.

---

## 3. AI 모델 정보

### `model_artifact`

AI 모델 파일 자체가 아니라 모델의 이름, 버전, 저장 위치와 사용 상태를 저장한다.
`realtime-analysis-service`는 시작할 때 `ACTIVE` 상태의 모델을 조회하고 `artifact_uri`에서 모델을 로딩한다.

| 컬럼 | 타입 | 필수 | 설명 |
| --- | --- | --- | --- |
| `id` | UUID | Y | 모델을 구분하는 고유 ID |
| `model_name` | VARCHAR(100) | Y | 모델 이름. 예: `nilm-tcn` |
| `version` | VARCHAR(50) | Y | 모델 버전. 예: `v1.0.0` |
| `artifact_uri` | VARCHAR(500) | Y | 모델 파일이 있는 로컬 또는 S3 경로 |
| `manifest_uri` | VARCHAR(500) | Y | 모델 입출력·전처리·threshold manifest 경로 |
| `status` | VARCHAR(20) | Y | 모델의 현재 사용 상태 |
| `created_at` | TIMESTAMPTZ | Y | 모델 정보가 DB에 등록된 시각 |

#### 상태값

| 값 | 의미 |
| --- | --- |
| `VALIDATED` | 평가가 끝나 운영 적용이 가능하지만 아직 사용하지 않는 모델 |
| `ACTIVE` | 현재 `realtime-analysis-service`가 사용하는 모델 |
| `RETIRED` | 이전에 사용했거나 더 이상 사용하지 않는 모델 |

#### 모델 manifest 관리 항목

하나의 멀티라벨 모델이 6종 가전을 동시에 출력하므로 `appliance_type`은 모델 테이블에 두지 않는다.
입력 규격과 가전 출력 순서, 가전별 threshold는 모델과 함께 배포하는 manifest에서 관리한다.

```json
{
  "model_name": "nilm-tcn",
  "version": "v1.0.0",
  "sample_hz": 1,
  "window_seconds": 299,
  "features": [
    "active_power",
    "reactive_power",
    "power_factor",
    "current"
  ],
  "outputs": [
    {"index": 0, "appliance_type": "KETTLE", "threshold": 0.62},
    {"index": 1, "appliance_type": "INDUCTION", "threshold": 0.71},
    {"index": 2, "appliance_type": "IRON", "threshold": 0.58},
    {"index": 3, "appliance_type": "MICROWAVE", "threshold": 0.63},
    {"index": 4, "appliance_type": "HAIR_DRYER", "threshold": 0.67},
    {"index": 5, "appliance_type": "VACUUM_CLEANER", "threshold": 0.65}
  ]
}
```

#### 제약조건

- `model_name + version` 조합은 중복될 수 없다.
- MVP에서는 `ACTIVE` 모델을 하나만 사용한다.

---

## 4. 가구별 가전 활동 기준선

### `routine_baseline`

가구가 특정 가전을 평소 얼마나 자주, 어느 시간대에 사용하는지 계산한 기준값을 저장한다.
실제 활동 결과와 비교할 때 사용하는 **평소 기준**이다.

| 컬럼 | 타입 | 필수 | 설명 |
| --- | --- | --- | --- |
| `id` | UUID | Y | 기준선을 구분하는 고유 ID |
| `household_id` | VARCHAR(50) | Y | 기준선을 적용할 가구 ID |
| `appliance_type` | VARCHAR(50) | Y | 전자레인지, 전기포트 등 대상 가전 |
| `baseline_type` | VARCHAR(30) | Y | 기준선을 계산한 알고리즘 종류 |
| `sample_days` | SMALLINT | Y | 기준선 계산에 사용한 정상 관측 일수 |
| `active_days` | SMALLINT | Y | 정상 관측일 중 가전 사용이 감지된 일수 |
| `daily_use_probability` | DECIMAL(5,4) | Y | 일 사용확률. `active_days / sample_days` |
| `reliability_weight` | DECIMAL(5,4) | Y | 해당 가전을 활동 판단에 반영할 가중치 |
| `baseline_data` | JSONB | Y | 시간대 확률 등 알고리즘별 추가 기준값 |
| `enabled` | BOOLEAN | Y | 현재 이상 판단에 사용할 기준선인지 여부 |
| `calculated_at` | TIMESTAMPTZ | Y | 기준선을 마지막으로 계산한 시각 |

#### 주요 값

| 항목 | 예시 | 의미 |
| --- | --- | --- |
| `baseline_type` | `DAILY_USAGE_RATE` | 정상 관측일 중 사용일 비율로 기준선 계산 |
| `sample_days` | `20` | 최근 기간에서 정상 데이터가 있었던 날이 20일 |
| `active_days` | `17` | 20일 중 해당 가전을 17일 사용 |
| `daily_use_probability` | `0.8500` | 정상 관측일의 85%에서 사용 |
| `reliability_weight` | `0.6500` | 이상 판단 점수에 0.65 가중치로 반영 |

#### `baseline_data` 예시

```json
{
  "hourly_probability": {
    "07": 0.10,
    "08": 0.65,
    "09": 0.40,
    "18": 0.20,
    "19": 0.70
  },
  "preferred_windows": [
    {"start": "07:00", "end": "10:00"},
    {"start": "18:00", "end": "21:00"}
  ]
}
```

#### 계산 및 갱신 원칙

- `daily_use_probability = active_days / sample_days`
- 센서 장애일과 처리 실패일은 `sample_days`에서 제외한다.
- MVP에서는 최초 유효 활동 기록을 모은 뒤 생성하고 이후 주기적으로 같은 행을 갱신한다.
- `household_id + appliance_type + baseline_type` 조합은 중복될 수 없다.
- 시간대 패턴은 PoC상 편차가 크므로 초기에는 단독 위험 판단보다 보조 지표로 사용한다.

---

## 5. 이상 감지 정책

### `analysis_policy`

일일 활동 결과와 기준선을 어떤 알고리즘으로 해석하고, 조건을 만족했을 때 어떤 이벤트를 생성할지 저장한다.

| 컬럼 | 타입 | 필수 | 설명 |
| --- | --- | --- | --- |
| `id` | UUID | Y | 정책을 구분하는 고유 ID |
| `policy_code` | VARCHAR(50) | Y | 코드에서 사용하는 정책 식별자 |
| `algorithm_type` | VARCHAR(50) | Y | 이상 여부를 판단하는 알고리즘 |
| `parameters` | JSONB | Y | 알고리즘 실행에 필요한 세부 조건 |
| `event_type` | VARCHAR(50) | Y | 조건 만족 시 생성할 이벤트 이름 |
| `score` | SMALLINT | Y | 이벤트 위험 점수. 범위는 0~100 |
| `severity` | VARCHAR(20) | Y | 사용자에게 표시할 위험 단계 |
| `cooldown_hours` | SMALLINT | Y | 동일 이벤트의 반복 생성을 막는 시간 |
| `enabled` | BOOLEAN | Y | 현재 정책 사용 여부 |
| `created_at` | TIMESTAMPTZ | Y | 정책이 등록된 시각 |

#### 정책 예시

```json
{
  "policy_code": "DAILY_ACTIVITY_MISSING",
  "algorithm_type": "CONSECUTIVE_INACTIVITY",
  "parameters": {
    "consecutive_inactive_days": 2,
    "minimum_reliable_appliances": 2,
    "minimum_reliability_weight": 0.5
  },
  "event_type": "ROUTINE_MISSED",
  "score": 80,
  "severity": "CONFIRM",
  "cooldown_hours": 48,
  "enabled": true
}
```

#### 컬럼 간 차이

- `policy_code`: 무엇을 감지하는 정책인지 나타낸다.
- `algorithm_type`: 해당 정책을 어떤 방식으로 계산하는지 나타낸다.
- `parameters`: 계산 방식에 필요한 가변 설정이다.
- `score`: 정렬과 계산에 사용하는 숫자 위험도다.
- `severity`: 화면과 업무 처리에 사용하는 단계형 위험도다.

#### 제약조건

- `policy_code`는 중복될 수 없다.
- `score`는 0~100 범위다.
- `cooldown_hours`는 0 이상이다.

---

## 6. 가구별 일일 데이터 관측

### `household_observation_daily`

가구별로 하루 동안 전력 데이터가 정상적으로 수집됐는지 저장한다.
가전 사용 여부가 아니라 **이날의 AI 분석 결과를 신뢰할 수 있는지** 판단하는 테이블이다.

| 컬럼 | 타입 | 필수 | 설명 |
| --- | --- | --- | --- |
| `id` | UUID | Y | 일일 관측 결과를 구분하는 고유 ID |
| `household_id` | VARCHAR(50) | Y | 관측 대상 가구 ID |
| `observation_date` | DATE | Y | 한국 시간 기준 관측 날짜 |
| `sample_count` | BIGINT | Y | 중복·오류를 제외하고 정상 처리한 1Hz 샘플 수 |
| `expected_sample_count` | BIGINT | Y | 계획된 관측 시간 동안 수신해야 하는 샘플 수 |
| `coverage_ratio` | DECIMAL(5,4) | Y | 정상 수집 비율. `sample_count / expected_sample_count` |
| `observation_status` | VARCHAR(30) | Y | 데이터 관측 및 처리 상태 |
| `updated_at` | TIMESTAMPTZ | Y | 관측 결과가 마지막으로 갱신된 시각 |

#### 예상 샘플 수

1Hz로 하루 전체를 관측하면 예상 샘플 수는 86,400개다.

```text
1초당 1개 × 60초 × 60분 × 24시간 = 86,400개
```

센서가 정오에 처음 등록됐다면 해당 날짜의 계획된 관측 시간은 12시간이므로 예상 샘플 수는 43,200개다.
단, 예기치 않은 센서 장애가 발생했다고 예상 샘플 수를 줄이면 안 된다. 장애 시에는 실제 샘플 수만 감소해 수집률이 낮아져야 한다.

#### 관측 상태

| 값 | 의미 |
| --- | --- |
| `COLLECTING` | 당일 데이터 수집 중 |
| `VALID` | AI 분석 결과를 사용할 수 있을 만큼 정상 수집 |
| `INSUFFICIENT_DATA` | 데이터가 부족해 가전 사용 여부 판단 불가 |
| `SENSOR_GAP` | 센서 데이터가 장시간 또는 전혀 들어오지 않음 |
| `PROCESSING_ERROR` | 수집 후 AI 분석 처리에서 오류 발생 |
| `EXCLUDED` | 점검 등 명시적인 사유로 분석 대상에서 제외 |

#### 중요 구분

```text
observation_status = VALID + event_count = 0
→ 데이터는 정상이지만 가전 사용이 감지되지 않음

observation_status = SENSOR_GAP
→ 데이터 자체가 없어 가전 사용 여부를 판단할 수 없음
```

#### 제약조건

- `household_id + observation_date` 조합은 중복될 수 없다.
- `coverage_ratio`는 0~1 범위다.

---

## 7. 가전별 일일 활동 요약

### `household_activity_daily`

정상 관측된 하루 동안 특정 가전 사용이 몇 번 감지됐는지 요약한다.
서비스가 재시작해도 연속 미사용일과 기준선을 계산할 수 있도록 일 단위 결과를 보존한다.

| 컬럼 | 타입 | 필수 | 설명 |
| --- | --- | --- | --- |
| `id` | UUID | Y | 가전별 일일 활동 결과 ID |
| `observation_daily_id` | UUID | Y | `household_observation_daily.id`를 참조하는 FK |
| `appliance_type` | VARCHAR(50) | Y | 전자레인지, 전기포트 등 대상 가전 |
| `event_count` | INTEGER | Y | 해당 날짜에 감지된 사용 세션 수 |
| `updated_at` | TIMESTAMPTZ | Y | 일일 활동 결과가 마지막으로 갱신된 시각 |

#### 사용 여부 판단

```text
event_count > 0
→ 해당 날짜에 가전 사용 감지

event_count = 0
→ 해당 날짜에 가전 사용 미감지
```

사용하지 않은 가전도 `event_count = 0`인 행을 생성한다. 그래야 세션 행이 없는 이유가 미사용인지 미처리인지 구분된다.

#### 제약조건

- `observation_daily_id + appliance_type` 조합은 중복될 수 없다.
- `event_count`는 0 이상이다.

---

## 8. 가전 사용 세션

### `appliance_usage_session`

AI가 감지한 가전 사용 한 번의 시작·종료 시각과 추론 확률을 저장한다.
일일 요약만으로 알 수 없는 시간대 패턴과 사용 지속 시간을 분석할 때 사용한다.

| 컬럼 | 타입 | 필수 | 설명 |
| --- | --- | --- | --- |
| `id` | UUID | Y | 가전 사용 세션을 구분하는 고유 ID |
| `activity_daily_id` | UUID | Y | `household_activity_daily.id`를 참조하는 FK |
| `started_at` | TIMESTAMPTZ | Y | 가전 사용이 시작됐다고 판정한 시각 |
| `ended_at` | TIMESTAMPTZ | N | 가전 사용이 종료됐다고 판정한 시각. 사용 중이면 NULL 가능 |
| `max_probability` | DECIMAL(5,4) | Y | 세션 동안 모델이 출력한 가장 높은 사용 확률 |
| `decision_threshold` | DECIMAL(5,4) | Y | 세션을 ON으로 판정할 때 적용한 manifest 임계값 |
| `updated_at` | TIMESTAMPTZ | Y | 최대 확률 또는 종료 시각을 마지막으로 수정한 시각 |

가구·날짜·가전 정보는 부모 테이블을 통해 확인하므로 세션 테이블에 중복 저장하지 않는다.

```text
appliance_usage_session
→ household_activity_daily에서 가전과 일일 결과 확인
→ household_observation_daily에서 가구와 날짜 확인
```

#### 예시

```text
household_activity_daily
전자레인지 / event_count = 3

appliance_usage_session
├─ 08:10~08:12 / max_probability 0.8700
├─ 12:30~12:32 / max_probability 0.9100
└─ 18:45~18:48 / max_probability 0.8300
```

#### 제약조건

- `max_probability`는 0~1 범위다.
- `decision_threshold`는 0~1 범위다.
- `ended_at`이 존재하면 `started_at`보다 빠를 수 없다.
- `activity_daily_id + started_at` 조합은 중복될 수 없다.
- 일일 활동의 `event_count`는 연결된 세션 수와 일치해야 한다.

---

## 9. 전체 처리 흐름

```text
1. Kafka에서 가구의 전력 데이터 수신
2. household_observation_daily의 샘플 수와 수집률 갱신
3. AI 모델로 6종 가전의 사용 확률 추론
4. 가전 사용이 감지되면 확률·threshold와 함께 appliance_usage_session 생성
5. 해당 household_activity_daily의 event_count 증가
6. 날짜 종료 후 관측 상태와 가전별 일일 활동 확정
7. 일일 활동 결과를 routine_baseline과 비교
8. analysis_policy 조건을 만족하면 analysis.event.v1 발행
```

## 10. 현재 MVP에서 제외한 항목

- 가구별 서로 다른 모델 배포
- 모델 A/B 테스트
- 모델별 세션 FK 추적
- 기준선 버전 이력
- 정책 버전 이력 및 예약 적용
- 별도의 시간별 집계 테이블
- 실제 부재·위험 확정 데이터

시간대 분석은 `appliance_usage_session.started_at`을 시간대별로 집계하고,
기준값은 `routine_baseline.baseline_data`에 저장한다. 조회량이 커지면 이후
`household_activity_hourly` 테이블을 추가한다.

---

## 11. ERDCloud 논리 컬럼명 및 상세 설명

아래의 `논리 이름`은 ERDCloud에서 `L`을 선택했을 때 표시할 한국어 이름이다.
`물리 이름`은 실제 PostgreSQL 컬럼명이므로 영문 그대로 유지한다.

### 11.1 AI 모델 정보 — `model_artifact`

| 논리 이름 | 물리 이름 | 타입 | 상세 설명 |
| --- | --- | --- | --- |
| 모델 ID | `id` | UUID | 모델 정보를 유일하게 구분하는 기본키다. 모델 이름이나 버전이 같아 보여도 이 값으로 정확히 식별한다. |
| 모델 이름 | `model_name` | VARCHAR(100) | 모델의 고정된 이름이다. 예: `nilm-tcn`. 같은 모델을 다시 학습해도 이름은 유지하고 버전을 변경한다. |
| 모델 버전 | `version` | VARCHAR(50) | 같은 모델의 학습·개선 결과를 구분한다. 예: `v1.0.0`. 이전 모델로 되돌리거나 결과를 재현할 때 사용한다. |
| 모델 파일 위치 | `artifact_uri` | VARCHAR(500) | 실제 모델 파일이 저장된 경로다. 예: 로컬 파일 경로 또는 `s3://.../nilm-tcn-v1.pt`. DB에는 모델 파일 자체를 저장하지 않는다. |
| Manifest 위치 | `manifest_uri` | VARCHAR(500) | Feature 순서, 정규화 값, 출력 가전 순서와 threshold가 기록된 manifest 경로다. |
| 모델 상태 | `status` | VARCHAR(20) | 모델의 운영 상태다. `VALIDATED`는 검증 완료, `ACTIVE`는 현재 사용 중, `RETIRED`는 사용 종료를 뜻한다. |
| 등록 시각 | `created_at` | TIMESTAMPTZ | 모델 정보가 `analysis_db`에 처음 등록된 시각이다. 모델 학습 완료 시각과는 다를 수 있다. |

### 11.2 가구별 가전 활동 기준선 — `routine_baseline`

| 논리 이름 | 물리 이름 | 타입 | 상세 설명 |
| --- | --- | --- | --- |
| 기준선 ID | `id` | UUID | 가구·가전별 기준선을 유일하게 구분하는 기본키다. |
| 가구 ID | `household_id` | VARCHAR(50) | 기준선을 적용할 가구의 식별자다. 가구 원본 정보가 다른 DB에 있으면 물리 FK 없이 같은 ID 값만 저장한다. |
| 가전 유형 | `appliance_type` | VARCHAR(50) | 기준선을 계산할 대상 가전이다. 예: `MICROWAVE`, `KETTLE`, `INDUCTION`. |
| 기준선 유형 | `baseline_type` | VARCHAR(30) | 기준선을 어떤 방식으로 계산했는지 나타낸다. MVP의 `DAILY_USAGE_RATE`는 정상 관측일 중 사용일 비율을 사용한다. |
| 유효 관측 일수 | `sample_days` | SMALLINT | 기준선 계산에 실제로 포함한 정상 관측 일수다. 센서 장애일과 처리 실패일은 제외한다. |
| 사용 감지 일수 | `active_days` | SMALLINT | 유효 관측일 중 해당 가전의 사용 세션이 한 번 이상 존재한 날짜 수다. 하루에 여러 번 사용해도 1일로 계산한다. |
| 일일 사용 확률 | `daily_use_probability` | DECIMAL(5,4) | `active_days / sample_days`로 계산한 일 단위 사용 빈도다. 값 0.8500은 정상 관측일의 85%에서 사용했다는 의미다. |
| 신뢰 가중치 | `reliability_weight` | DECIMAL(5,4) | 해당 가전을 가구 활동 판단에 얼마나 중요하게 반영할지 나타내는 0~1 값이다. 자주 사용되고 모델 신뢰도가 높은 가전일수록 높게 설정한다. |
| 기준선 상세 데이터 | `baseline_data` | JSONB | 시간대별 사용확률, 선호 시간 구간 등 기준선 알고리즘마다 달라지는 값을 저장한다. 고정 컬럼 추가 없이 시간대 분석을 확장하기 위한 공간이다. |
| 사용 여부 | `enabled` | BOOLEAN | 해당 기준선을 현재 이상 감지에 사용할지 결정한다. `false`이면 기준선 데이터는 보존하지만 판정에서는 제외한다. |
| 계산 시각 | `calculated_at` | TIMESTAMPTZ | `sample_days`, 사용확률과 시간대 기준을 마지막으로 다시 계산한 시각이다. 기준선이 최신인지 확인할 때 사용한다. |

### 11.3 이상 감지 정책 — `analysis_policy`

| 논리 이름 | 물리 이름 | 타입 | 상세 설명 |
| --- | --- | --- | --- |
| 정책 ID | `id` | UUID | 이상 감지 정책을 유일하게 구분하는 기본키다. |
| 정책 코드 | `policy_code` | VARCHAR(50) | 애플리케이션에서 정책을 찾을 때 사용하는 고정 식별자다. 예: `DAILY_ACTIVITY_MISSING`. |
| 알고리즘 유형 | `algorithm_type` | VARCHAR(50) | 이상 여부를 판단하는 계산 방식이다. 예: `CONSECUTIVE_INACTIVITY`는 연속 미활동 일수를 검사한다. |
| 정책 파라미터 | `parameters` | JSONB | 알고리즘 실행에 필요한 가변 조건을 저장한다. 예: 연속 미활동 2일, 최소 신뢰 가전 2개, 최소 가중치 0.5. |
| 이벤트 유형 | `event_type` | VARCHAR(50) | 정책 조건이 만족됐을 때 발행할 분석 이벤트 이름이다. 예: `ROUTINE_MISSED`. |
| 위험 점수 | `score` | SMALLINT | 이벤트의 위험도를 0~100 숫자로 표현한다. Monitoring에서 사건 우선순위를 정렬하거나 상태를 결정할 때 사용할 수 있다. |
| 심각도 | `severity` | VARCHAR(20) | 사용자 화면과 업무 처리에 사용하는 단계형 위험도다. 예: `WATCH`, `CONFIRM`, `CRITICAL`. |
| 재발생 제한 시간 | `cooldown_hours` | SMALLINT | 같은 정책 이벤트를 다시 생성하지 않을 시간이다. 48이면 최초 발생 후 48시간 동안 같은 이벤트의 반복 발행을 막는다. |
| 활성 여부 | `enabled` | BOOLEAN | 현재 정책을 실행할지 결정한다. `false`이면 정책을 삭제하지 않고 일시 중지한다. |
| 등록 시각 | `created_at` | TIMESTAMPTZ | 정책이 DB에 처음 등록된 시각이다. |

### 11.4 가구별 일일 데이터 관측 — `household_observation_daily`

| 논리 이름 | 물리 이름 | 타입 | 상세 설명 |
| --- | --- | --- | --- |
| 일일 관측 ID | `id` | UUID | 특정 가구의 특정 날짜 관측 결과를 유일하게 구분하는 기본키다. 가전별 일일 활동의 부모 ID로 사용한다. |
| 가구 ID | `household_id` | VARCHAR(50) | 전력 데이터를 수집한 가구의 식별자다. |
| 관측 날짜 | `observation_date` | DATE | 일일 수집 상태를 집계한 날짜다. 서비스 기준 시간대인 `Asia/Seoul` 날짜를 사용한다. |
| 정상 샘플 수 | `sample_count` | BIGINT | 중복과 오류 데이터를 제외하고 정상적으로 처리한 1Hz 샘플 수다. 하루 전체가 정상이면 약 86,400개다. |
| 예상 샘플 수 | `expected_sample_count` | BIGINT | 계획된 관측 시간 동안 원래 수신해야 하는 샘플 수다. 하루 전체 1Hz 수집이면 86,400개다. |
| 정상 수집 비율 | `coverage_ratio` | DECIMAL(5,4) | `sample_count / expected_sample_count`로 계산한다. 0.9525는 예상 데이터의 95.25%를 정상 수집했다는 의미다. |
| 데이터 관측 상태 | `observation_status` | VARCHAR(30) | 이날 데이터를 AI 결과와 기준선 계산에 사용할 수 있는지 나타낸다. `COLLECTING`, `VALID`, `INSUFFICIENT_DATA`, `SENSOR_GAP`, `PROCESSING_ERROR`, `EXCLUDED` 등을 사용한다. |
| 수정 시각 | `updated_at` | TIMESTAMPTZ | 샘플 수, 수집률 또는 관측 상태가 마지막으로 갱신된 시각이다. |

### 11.5 가전별 일일 활동 요약 — `household_activity_daily`

| 논리 이름 | 물리 이름 | 타입 | 상세 설명 |
| --- | --- | --- | --- |
| 일일 활동 ID | `id` | UUID | 특정 가구·날짜·가전의 일일 활동 결과를 유일하게 구분하는 기본키다. 사용 세션의 부모 ID로 사용한다. |
| 일일 관측 ID | `observation_daily_id` | UUID | `household_observation_daily.id`를 참조하는 FK다. 어떤 가구의 어느 날짜 데이터에서 나온 결과인지 연결한다. |
| 가전 유형 | `appliance_type` | VARCHAR(50) | 일일 사용 결과를 집계할 대상 가전이다. 예: `MICROWAVE`, `KETTLE`. |
| 사용 횟수 | `event_count` | INTEGER | 해당 날짜에 만들어진 가전 사용 세션 수다. 0이면 정상 관측됐지만 해당 가전 사용이 감지되지 않았다는 의미다. |
| 수정 시각 | `updated_at` | TIMESTAMPTZ | 새로운 사용 세션이 추가되어 `event_count`가 마지막으로 변경된 시각이다. |

### 11.6 가전 사용 세션 — `appliance_usage_session`

| 논리 이름 | 물리 이름 | 타입 | 상세 설명 |
| --- | --- | --- | --- |
| 사용 세션 ID | `id` | UUID | AI가 감지한 개별 가전 사용 세션을 유일하게 구분하는 기본키다. |
| 일일 활동 ID | `activity_daily_id` | UUID | `household_activity_daily.id`를 참조하는 FK다. 세션이 어느 날짜의 어떤 가전 활동에 속하는지 연결한다. |
| 사용 시작 시각 | `started_at` | TIMESTAMPTZ | AI가 가전 사용이 시작됐다고 판정한 시각이다. 시간대별 사용 패턴의 기준이 된다. |
| 사용 종료 시각 | `ended_at` | TIMESTAMPTZ | AI가 가전 사용이 종료됐다고 판정한 시각이다. 아직 사용 중이거나 종료를 감지하지 못했으면 NULL일 수 있다. |
| 최대 예측 확률 | `max_probability` | DECIMAL(5,4) | 해당 세션 동안 모델이 출력한 가장 높은 사용 확률이다. 판정 근거 확인과 threshold 조정에 사용한다. |
| 판정 임계값 | `decision_threshold` | DECIMAL(5,4) | 해당 세션을 ON으로 판단할 때 manifest에서 적용한 threshold다. |
| 수정 시각 | `updated_at` | TIMESTAMPTZ | 최대 확률 또는 종료 시각을 마지막으로 수정한 시각이다. |

### 11.7 ERDCloud 입력 규칙

- 테이블의 논리 이름과 컬럼의 논리 이름은 한국어로 입력한다.
- 테이블의 물리 이름과 컬럼의 물리 이름은 실제 SQL에 사용할 영문 이름을 유지한다.
- `household_activity_daily.observation_daily_id`는 `household_observation_daily.id`를 참조한다.
- `appliance_usage_session.activity_daily_id`는 `household_activity_daily.id`를 참조한다.
- 두 FK 관계는 모두 1:0..N 비식별 관계로 설정한다.
