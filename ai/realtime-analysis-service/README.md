# Realtime Analysis Service

## 로컬 실행 방법 (Windows CMD)

아래 명령은 저장소를 `C:\Users\SSAFY\Desktop\D201\S15P21D201`에 받은 경우의
예시입니다. 다른 위치에 받았다면 경로만 바꿉니다. Docker Desktop이 실행 중이고
`infrastructure\local\.env` 설정이 끝난 상태를 기준으로 합니다.

### 정식 성능 비교 자동 실행

로컬 전체 Compose와 Prometheus를 먼저 실행한 뒤 다음 명령을 사용하면 Consumer
`1 → 2 → 4` 비교를 자동으로 수행합니다.

```bat
cd /d C:\Users\SSAFY\Desktop\D201\S15P21D201\ai\realtime-analysis-service
python -m pip install -r ..\..\infrastructure\mqtt\simulator\requirements.txt
python tools\consumer_load_test.py --consumers 1,2,4 --houses 40 --hz 1 --duration 300
```

실행기는 각 Consumer 수마다 다음 작업을 수행합니다.

1. 첫 실행 이미지를 빌드하고 Consumer를 지정한 수로 조정
2. Prometheus에서 Consumer 수와 할당 파티션 합계 `24` 확인
3. 여러 가구 시뮬레이터 실행
4. 가구당 299개 입력 처리와 warm-up `0` 대기
5. 안정 구간에서 처리량, lag, 단계별 평균·p95, E2E p95, 오류, DLQ, CPU, 메모리 측정
6. 시뮬레이터 중단 후 lag가 `0`으로 복구되는 시간 측정
7. 다음 Consumer 수로 이동하고 전체 완료 또는 실패 시 Consumer 1개로 복구

결과는 `load-test-results\<실행시각>` 아래에 생성되며 Git에는 포함되지 않습니다.

| 파일 | 내용 |
| --- | --- |
| `report.md` | 처리량 향상률·효율·자동 판정과 단계별 p95 비교표 |
| `summary.csv` | Excel 등에서 비교할 수 있는 전체 핵심 수치 |
| `results.json` | 전체 원본 측정 결과 |
| `metadata.json` | 입력 부하와 측정 설정 |
| `simulator-consumers-*.log` | Consumer 수별 시뮬레이터 로그 |

기본 `40가구 × 1Hz`에서 Consumer 1개의 lag도 계속 `0`이면 `--hz 2`처럼 부하를
높입니다. 모든 Consumer에서 lag가 과도하게 쌓이면 가구 수나 Hz를 낮춥니다. 비교 시에는
모든 Consumer 수에 동일한 값을 사용해야 합니다. 반복 실행에서 기존 이미지를 그대로
사용하려면 `--skip-build`를 추가할 수 있습니다.

전체 옵션은 다음 명령으로 확인합니다.

```bat
python tools\consumer_load_test.py --help
```

### 1. 전체 서비스와 Consumer 1개 실행

```bat
cd /d C:\Users\SSAFY\Desktop\D201\S15P21D201\infrastructure\local
docker compose up -d --build --scale realtime-analysis-service=1
docker compose ps realtime-analysis-service aggregation-service
```

코드를 수정한 뒤 분석 서비스만 다시 빌드하려면 다음 명령을 사용합니다.

```bat
docker compose up -d --build --force-recreate --scale realtime-analysis-service=1 realtime-analysis-service
```

기동 로그는 다음과 같이 확인합니다. `Ctrl+C`는 로그 보기만 종료하며 컨테이너는
계속 실행됩니다.

```bat
docker compose logs -f realtime-analysis-service
```

### 2. 여러 가구 데이터 발생

새 CMD 창을 열고 시뮬레이터를 실행합니다. 최초 한 번은 의존성을 설치해야 합니다.

```bat
cd /d C:\Users\SSAFY\Desktop\D201\S15P21D201\infrastructure\mqtt\simulator
pip install -r requirements.txt
python simulator.py --scenario random --houses 20 --hz 1 --count 0 --quiet
```

- `--houses 20`: `H001`부터 `H020`까지 사용하여 여러 Kafka 파티션에 데이터를 보냅니다.
- `--hz 1`: 가구마다 초당 1개를 보냅니다.
- `--count 0`: 자동 종료하지 않고 계속 실행합니다. 종료할 때는 `Ctrl+C`를 누릅니다.
- 성능 비교 시 `--hz`를 높일 수 있지만 Consumer 1·2·4개 테스트에서 같은 값을 사용해야 합니다.

### 3. Consumer를 2개, 4개로 증설

다중 Consumer 테스트에서는 `compose.scale.yaml`을 함께 사용합니다. 로컬에서는
static membership을 끄고 cooperative-sticky 할당만 사용하며,
`aggregation-service`는 항상 1개로 유지합니다.

```bat
cd /d C:\Users\SSAFY\Desktop\D201\S15P21D201\infrastructure\local

docker compose -f compose.yaml -f compose.scale.yaml up -d --scale realtime-analysis-service=2 --no-recreate realtime-analysis-service
docker compose -f compose.yaml -f compose.scale.yaml ps realtime-analysis-service aggregation-service

docker compose -f compose.yaml -f compose.scale.yaml up -d --scale realtime-analysis-service=4 --no-recreate realtime-analysis-service
docker compose -f compose.yaml -f compose.scale.yaml ps realtime-analysis-service aggregation-service
```

`--no-recreate`는 이미 실행 중인 Consumer를 불필요하게 다시 만들지 않고 새 replica만
추가하도록 합니다. 따라서 cooperative-sticky 리밸런싱과 유지된 파티션의 상태 보존을
확인하기 쉽습니다.

### 4. 리밸런싱과 상태 확인

```bat
docker compose -f compose.yaml -f compose.scale.yaml logs --since=5m realtime-analysis-service | findstr /I /C:"partitions assigned" /C:"partitions revoked" /C:"partitions lost" /C:"ILLEGAL_GENERATION" /C:"Offset store skipped"
```

- Prometheus: <http://localhost:19090>
- Prometheus Targets: <http://localhost:19090/targets>
- Grafana: <http://localhost:13001> (`admin / admin`)

Prometheus에서 다음 쿼리를 차례로 확인합니다.

```promql
count(up{job="ai-analysis"} == 1)
sum(nilm_analysis_consumer_assigned_partitions{job="ai-analysis"})
sum(nilm_analysis_warmup_households{job="ai-analysis"})
sum(nilm_analysis_consumer_lag_messages{job="ai-analysis"})
sum(rate(nilm_analysis_messages_total{job="ai-analysis",status="processed"}[1m]))
```

정상이라면 Consumer 수는 지정한 `1`, `2`, `4`와 같고, 할당 파티션 합은 `24`입니다.
리밸런싱 직후 이동한 가구만 warm-up에 들어갔다가 299개를 다시 모으면 빠지며,
입력이 멈춘 뒤 lag는 최종적으로 `0`이 됩니다.

### 5. 테스트 종료 후 Consumer 1개로 복구

```bat
cd /d C:\Users\SSAFY\Desktop\D201\S15P21D201\infrastructure\local
docker compose -f compose.yaml -f compose.scale.yaml up -d --scale realtime-analysis-service=1 --no-recreate realtime-analysis-service
docker compose -f compose.yaml -f compose.scale.yaml ps realtime-analysis-service aggregation-service
```

DB와 Kafka 데이터를 유지하려면 테스트 종료 시 `docker compose down -v`는 실행하지
않습니다. 리밸런싱 문제와 검증 기록은
[Kafka 다중 Consumer 리밸런싱 트러블슈팅](../../docs/진행상황/최보경/20260920_Kafka_다중_Consumer_리밸런싱_트러블슈팅.md)을
참고합니다.

Kafka 전력 데이터를 검증하고, 가구별 입력 버퍼와 MVP용 가전 ON/OFF 예측을 거쳐
위험 이벤트와 생활 패턴 변화 이벤트를 `analysis.event.v1`로 발행합니다.

이 문서는 전력 데이터 시뮬레이터·MQTT-Kafka Bridge 담당자와
`analysis.event.v1` 이상 이벤트 수신 담당자가 함께 사용하는 연동 계약입니다.

## MVP 처리 흐름

```text
power.raw.v1
  -> 입력 검증 및 가구별 299개 버퍼
  -> FakePredictor 가전 6종 ON 확률
  -> model_manifest.json의 threshold로 ON/OFF 판정
  -> 연속 판정과 히스테리시스로 확정한 상태를 analysis.snapshot.v1로 발행
  -> analysis_db의 활성 routine_baseline과 일일 사용 상태 비교
  -> 기준선 강도가 설정값 이상이면 analysis.event.v1 발행
```

MVP 대상 가전과 Predictor 출력 순서는 AI 실험 결과 및 모델 명세를 기준으로 다음과 같이
고정합니다.

```text
KETTLE
INDUCTION
IRON
MICROWAVE
HAIR_DRYER
VACUUM_CLEANER
```

잘못된 입력은 `dlq.analysis`로 발행합니다. Kafka offset은 정상 처리 또는 DLQ 전송이
완료된 뒤에만 로컬에 저장하고, Consumer가 안정 상태일 때 자동 commit합니다.

## 실시간 Snapshot 수신 담당자 전달 사항

### 수신 대상

| 항목 | 값 |
| --- | --- |
| Kafka 토픽 | `analysis.snapshot.v1` |
| Kafka Record Key | `household_id` |
| Value 형식 | UTF-8 JSON Object |
| 발행 시점 | 입력 윈도우가 완성된 뒤 매 추론 시점 |

기본 입력이 가구별 1Hz이고 모델 윈도우 크기가 299이면, 최초 299개 입력이 쌓인 뒤부터
가구별로 약 1초마다 Snapshot 한 건을 발행합니다. `appliances[].is_on`은 모델 확률을
단순 비교한 값이 아니라 연속 판정과 히스테리시스를 모두 적용한 최종 확정 상태입니다.

### Snapshot 출력 계약

```json
{
  "schema_version": 1,
  "snapshot_id": "8f3b2a19-d6e-4c72-9b12-a1b2c3d4e5f6",
  "household_id": "H001",
  "observed_at": "2026-09-10T00:10:00Z",
  "published_at": "2026-09-10T00:10:00.125Z",
  "measurement": {
    "active_power": 1789.47,
    "reactive_power": 340.01,
    "power_factor": 0.982,
    "current": 8.279
  },
  "appliances": [
    {"appliance_type": "KETTLE", "is_on": false},
    {"appliance_type": "INDUCTION", "is_on": false},
    {"appliance_type": "IRON", "is_on": false},
    {"appliance_type": "MICROWAVE", "is_on": true},
    {"appliance_type": "HAIR_DRYER", "is_on": false},
    {"appliance_type": "VACUUM_CLEANER", "is_on": false}
  ]
}
```

- `snapshot_id`는 원본 입력의 `message_id`를 사용하므로 동일 입력 재처리 시에도 같습니다.
- `observed_at`은 원본 측정 시각, `published_at`은 Snapshot 발행 직전 시각이며 UTC로 냅니다.
- `measurement`는 해당 추론 시점에 수신한 최신 원본 전력값 네 개를 그대로 담습니다.
- `appliances`는 계약에 정한 6종을 고정 순서로 모두 포함하며 확률은 노출하지 않습니다.
- 같은 가구의 Kafka 레코드 순서를 유지할 수 있도록 Key로 `household_id`를 사용합니다.

## 시뮬레이터 담당자 전달 사항

### 전송 대상

| 항목 | 값 |
| --- | --- |
| Kafka 토픽 | `power.raw.v1` |
| Kafka Record Key | JSON의 `household_id`와 같은 값 |
| Value 형식 | UTF-8 JSON Object |
| 전송 주기 | 가구별 1Hz, 1초마다 1건 |
| 측정 시각 | 타임존이 포함된 ISO 8601, UTC `Z` 권장 |

같은 가구의 데이터 순서가 유지되도록 Kafka Record Key에 `household_id`를 사용합니다.

```text
Kafka Key: H001
```

### 필수 입력 계약

```json
{
  "message_id": "8f3b2a19-4d6e-4c72-9b12-a1b2c3d4e5f6",
  "household_id": "H001",
  "device_id": "main",
  "measured_at": "2026-09-08T03:41:05.120Z",
  "active_power": 1789.47,
  "reactive_power": 340.01,
  "power_factor": 0.982,
  "current": 8.279
}
```

| 필드 | 타입 | 필수 | 설명 및 제약 |
| --- | --- | --- | --- |
| `message_id` | UUID | O | 샘플마다 새로 생성하는 고유 ID |
| `household_id` | String | O | 가구 ID, Kafka Key와 같은 값 |
| `device_id` | String | O | 계측기 ID, 현재 주 계측기는 `main` |
| `measured_at` | ISO 8601 Timestamp | O | 실제 측정 시각, 타임존 필수 |
| `active_power` | Number | O | 유효전력, 현재 MVP에서는 0 이상 |
| `reactive_power` | Number | O | 무효전력, 음수 가능 여부는 센서 규격을 따름 |
| `power_factor` | Number | O | 역률, -1 이상 1 이하 |
| `current` | Number | O | 전류, 0 이상 |

`message_id`는 매 메시지마다 달라야 하며, `measured_at`에는 로컬 시각만 보내지 말고
반드시 `Z` 또는 UTC Offset을 포함해야 합니다.

```text
권장: 2026-09-08T03:41:05.120Z
허용: 2026-09-08T12:41:05.120+09:00
거부: 2026-09-08T12:41:05.120
```

### 호환 필드

현재 Bridge가 다음 호환·부가 필드를 함께 전송해도 분석 서비스는 입력을 거부하지 않습니다.
분석 서비스는 표준 필드를 우선 사용하고 아래 필드는 무시합니다.

```json
{
  "house": "H001",
  "device": "main",
  "ts": "2026-09-08T03:41:05.120Z",
  "power_w": 1789.47,
  "voltage": 220.0,
  "apparent_power": 1821.49
}
```

표준 필드가 누락됐다고 호환 필드로 대신 처리하지는 않습니다. 예를 들어 `power_w`가 있어도
`active_power`가 없으면 입력 검증에 실패합니다.

### 시뮬레이터 연동 체크리스트

- [ ] Kafka Key와 `household_id`가 같은가
- [ ] 4개 AI 입력값을 모두 보내는가
- [ ] `message_id`가 매 샘플마다 유일한가
- [ ] `measured_at`에 타임존이 포함됐는가
- [ ] 가구별로 초당 1건을 보내는가
- [ ] `household_id`가 테스트 baseline의 가구 ID와 일치하는가
- [ ] `power.raw.v1`의 메시지 수가 자동으로 증가하는가

입력 JSON이 깨졌거나 필수 필드가 누락되면 원본 입력은 `dlq.analysis`로 전달됩니다.

## 이상 이벤트 수신 담당자 전달 사항

### 수신 대상

| 항목 | 값 |
| --- | --- |
| Kafka 토픽 | `analysis.event.v1` |
| Kafka Record Key | `household_id` |
| Value 형식 | UTF-8 JSON Object |
| 현재 이벤트 유형 | `ROUTINE_MISSED`, `PROLONGED_INACTIVITY`, `PROLONGED_APPLIANCE_USE`, `ROUTINE_CHANGED` |
| 전달 보장 | At-least-once, 논리적 중복 가능 |

### MVP 출력 계약

```json
{
  "event_id": "f46567d2-2b29-486f-9051-8cae3ff28d6a",
  "household_id": "H001",
  "event_type": "ROUTINE_MISSED",
  "occurred_at": "2026-09-08T03:41:06.120000Z",
  "reason": {
    "appliance_type": "MICROWAVE",
    "expected_until": "08:10",
    "normal_days": 12,
    "window_days": 14
  }
}
```

| 필드 | 타입 | 설명 |
| --- | --- | --- |
| `event_id` | UUID | 발행된 이벤트 한 건의 고유 ID |
| `household_id` | String | 이상 후보가 감지된 가구 ID |
| `event_type` | String | 감지된 이벤트 유형 |
| `occurred_at` | ISO 8601 Timestamp | 이상을 판단한 시각, UTC로 발행 |
| `reason` | JSON Object | 알고리즘이 판단에 사용한 근거 |

`reason`은 사용자에게 바로 노출할 한국어 문장이 아니라 알고리즘별 근거 데이터입니다.
화면 문구가 필요하면 수신 서비스에서 해당 값을 이용해 만듭니다.

| 이벤트 | 분류 | 판단 기준 | `reason` 주요 필드 |
| --- | --- | --- | --- |
| `ROUTINE_MISSED` | 위험 | 기대 시각 전까지 해당 가전 사용 없음 | `appliance_type`, `expected_until`, `normal_days`, `window_days` |
| `PROLONGED_INACTIVITY` | 위험 | 예상 수면 구간을 제외한 미활동 시간이 기본 1분(`inactivity_hours` 0.0167) 경과 | `last_activity_at`, `threshold_hours`, `sleep_window` |
| `PROLONGED_APPLIANCE_USE` | 위험 | 위험 가전의 열린 세션이 허용 시간 초과 | `appliance_type`, `started_at`, `allowed_duration_minutes` |
| `ROUTINE_CHANGED` | 정보 | 최근 7일 첫 사용 중앙시각이 이전 21일보다 기본 120분 이상 이동 | `appliance_type`, `previous_time`, `recent_time`, `shift_minutes` |

세 위험 이벤트는 측정 시각을 기준으로 가구별 기본 60초마다 평가합니다.
`ROUTINE_CHANGED`는 전날 관측 마감 후 하루 한 번 평가하며 위험 점수에 직접 반영하지
않습니다. 임계값은 `analysis_policy`의 `parameters`에서 읽습니다.

`PROLONGED_INACTIVITY`는 기본 예상 수면 구간인 `23:00~07:00`과 겹치는 시간을
미활동 누적에서 제외합니다. 임계치 `inactivity_hours`는 시간 단위이며 현재 기본값은
1분(0.0167시간)입니다. 예를 들어 임계치가 6시간일 때 마지막 사용이 21:00에 끝났다면
21:00~23:00의 2시간과 다음 날 07:00~11:00의 4시간을 합쳐 11:00에 임계치에 도달합니다.
수면 구간에 실제 사용이 감지되면 해당 세션의 종료 시각부터 다시 계산합니다.

```text
최근 14일 중 12일 동안 08:10 이전에 확인된 활동이 오늘은 감지되지 않았습니다.
```

### 기준선 강도 필터

`ROUTINE_MISSED` 대상 기준선을 고르는 내부 계산은 다음과 같습니다. 이 값은
이벤트 위험 점수가 아니며 Kafka 메시지에는 포함하지 않습니다.

```text
baseline_strength = normal_days / window_days × 100 × reliability_weight
```

예시 기준선에서는 다음과 같이 86점이 됩니다.

```text
12 / 14 × 100 × 1.0 = 85.71 → 86
```

분석 서비스의 최소 기준선 강도가 80이면 `86 >= 80`인 기준선만 감지에 사용합니다.

### 수신 및 중복 처리 주의사항

- Kafka Consumer Group은 수신 서비스 전용 이름을 사용합니다.
- 비즈니스 처리가 끝난 뒤에 Offset을 커밋합니다.
- `event_id`가 같은 이벤트를 다시 받으면 중복 처리하지 않습니다.
- `event_id`는 논리 이벤트 키로부터 결정적으로 생성하므로 재시작 후 같은 이벤트도 같은 ID를 사용합니다.
- 가전별 이벤트의 `appliance_type`은 `reason`에 둡니다.
- 위험 점수와 심각도는 분석 서비스가 발행하지 않습니다.

프로세스 안에서는 정책의 `cooldown_hours`와 이벤트 ID로 반복 발행을 제한합니다. 재시작
후 재전달될 수 있으므로 수신 서비스도 반드시 `event_id`로 멱등 처리해야 합니다.

### 이상 이벤트 수신 체크리스트

- [ ] `analysis.event.v1`을 전용 Consumer Group으로 구독하는가
- [ ] Kafka Key를 String으로 읽는가
- [ ] Value를 UTF-8 JSON으로 역직렬화하는가
- [ ] `event_type`을 지원하는 유형으로 처리하는가
- [ ] `occurred_at`을 UTC로 파싱하는가
- [ ] `reason`을 고정 문자열이 아닌 JSON Object로 처리하는가
- [ ] 이벤트 처리 완료 후 Offset을 커밋하는가
- [ ] 중복 이벤트가 와도 알림이나 Incident가 중복 생성되지 않는가

### 발행 성공의 범위

분석 서비스의 Producer `flush()`는 이벤트가 Kafka 브로커에 전달됐는지만 확인합니다.
이벤트 수신 서비스가 메시지를 가져가 비즈니스 처리를 완료했는지는 확인하지 않습니다.
수신 완료 응답 이벤트는 현재 MVP 범위에 포함하지 않습니다.

## 일일 활동 지수 계약

하루 단위 절대 활동 지수는 `analysis.activity.v1`으로 발행하며 Kafka Key는
`household_id`입니다. 위험 점수는 이 메시지에 포함하지 않고 모니터링 서비스가 별도로
계산합니다.

```json
{
  "message_id": "8f3b2a19-4d6e-4c72-9b12-a1b2c3d4e5f6",
  "household_id": "H001",
  "activity_date": "2026-09-16",
  "activity_index": 85,
  "data_status": "VALID",
  "components": {
    "usage_count": 5,
    "appliance_type_count": 3,
    "usage_duration_seconds": 2400,
    "usage_count_score": 83,
    "appliance_diversity_score": 100,
    "usage_duration_score": 67
  }
}
```

관측 데이터가 부족하면 0점으로 발행하지 않습니다. `activity_index`는 `null`로 보내고
`components`는 생략합니다.

```json
{
  "message_id": "8f3b2a19-4d6e-4c72-9b12-a1b2c3d4e5f6",
  "household_id": "H001",
  "activity_date": "2026-09-16",
  "activity_index": null,
  "data_status": "INSUFFICIENT_DATA"
}
```

### 계산 및 발행 시점

기본값은 한국 시간 `00:10`에 전날 지수를 한 번 계산해 발행하는 것입니다. 자정 직후
도착하는 지연 샘플을 받을 수 있도록 10분의 마감 여유를 둡니다. 실행 시각은
`ACTIVITY_INDEX_PUBLISH_HOUR`, `ACTIVITY_INDEX_PUBLISH_MINUTE`로 변경할 수 있습니다.

관측률이 `ANALYSIS_OBSERVATION_VALID_COVERAGE_RATIO` 이상인 날만 지수를 계산합니다.
관측이 부족하거나 하루 전체가 비어 있으면 `INSUFFICIENT_DATA`로 발행합니다.

```text
usage_count_score
  = min(논리적 사용 횟수 / 6, 1) × 100

appliance_diversity_score
  = min(사용한 가전 종류 수 / 3, 1) × 100

usage_duration_score
  = min(가전별 상한이 적용된 사용시간 합계 / 3,600초, 1) × 100

activity_index
  = 0.50 × usage_count_score
  + 0.30 × appliance_diversity_score
  + 0.20 × usage_duration_score
```

ON/OFF 히스테리시스가 샘플 단위 노이즈를 제거한 뒤, 일일 집계에서 같은 가전의 가까운
세션을 하나의 논리적 사용으로 다시 묶습니다. 기본 병합 간격은 일반 가전 60초,
인덕션 120초, 다리미 300초입니다. 인버터·온도조절 주기를 여러 번 사용한 것으로
계산하지 않기 위한 값입니다. 10초 미만 논리적 사용은 활동 지수에서 제외합니다.

`usage_duration_seconds`에는 실제 측정 시간을 저장합니다. 점수 계산에만 아래 가전별
상한을 적용하여 장시간 방치가 활동 점수를 계속 올리지 않게 합니다.

| 가전 | 점수 반영 일일 상한 |
| --- | ---: |
| 전기포트 | 10분 |
| 전자레인지 | 30분 |
| 헤어드라이기 | 30분 |
| 다리미 | 60분 |
| 진공청소기 | 90분 |
| 인덕션 | 180분 |

전기포트는 짧은 물 끓이기, 전자레인지·헤어드라이기는 짧은 단발 작업, 다리미·청소기는
가사 작업, 인덕션은 식사 준비라는 사용 특성을 기준으로 MVP 상한을 다르게 두었습니다.
운영 데이터가 쌓이면 이 값은 분포의 상위 분위수로 조정합니다.

같은 가전의 사용 횟수도 점수에는 하루 최대 3회까지만 반영합니다. 원본 횟수와 원본
사용시간은 `components`에 제한 전 값으로 전달합니다. 사용시간은 각 가전의 활성 시간을
합한 값이므로 여러 가전이 동시에 켜져 있으면 겹친 시간도 가전별로 각각 포함됩니다.

`message_id`는 `household_id + activity_date`로 결정적으로 생성합니다. 서비스 재시작이나
재시도로 같은 날짜 메시지가 다시 전달돼도 같은 ID이므로 수신 서비스가 멱등 처리할 수
있습니다.

### 모니터링 서비스 전달 사항

- 이 값은 위험 점수가 아니라 그날의 절대 활동량입니다.
- `VALID`인 메시지만 위험 점수와 EWMA 계산에 사용합니다.
- `INSUFFICIENT_DATA`의 `activity_index=null`을 0점으로 바꾸면 안 됩니다.
- 저장 및 갱신 키는 `message_id` 또는 `household_id + activity_date`를 사용합니다.
- EWMA는 모니터링 서비스에서 유효한 일일 지수에만 적용합니다. 시작값은 첫 유효 지수,
  초기 `alpha`는 `0.3`을 권장합니다.
- 급락은 직전 한 건보다 이전 EWMA와 현재 지수의 차이로 판단하면 일시적 변동에 덜
  민감합니다.
- 일일 발행이므로 기존의 `15분 × 4회` 규칙은 적용할 수 없습니다. 연속 저하는
  `연속 N일` 기준으로 다시 정의해야 합니다.
- 위험 점수를 계산할 때 `activity_index`와 세 component 점수를 동시에 합산하면 같은
  활동을 중복 반영하게 됩니다. 위험 계산에는 최종 지수만 사용하고 components는 설명과
  디버깅에 사용합니다.

## 일일 루틴 기준선 생성과 갱신

일일 활동 지수를 발행한 직후 같은 `activity_date`까지의 데이터를 이용해
`routine_baseline`을 갱신합니다. 기본 스케줄이 한국 시간 `00:10`이므로 전날 관측을 먼저
마감하고 활동 지수를 발행한 다음 기준선을 계산합니다. 샘플 단위 갱신은 하지 않습니다.

```text
전날 관측 마감
  → activity_index 발행
  → 최근 28일 조회
  → VALID 날짜만 선택
  → 가구·가전별 기준선 계산
  → routine_baseline UPSERT
  → 탐지용 메모리 캐시 교체
```

`INSUFFICIENT_DATA`, `SENSOR_GAP`, 처리 실패일은 표본과 사용일 양쪽에서 모두 제외합니다.
유효 관측일이 기본 14일 미만이면 기존 기준선을 유지하고 새 기준선을 만들지 않습니다.

```text
sample_days = 최근 계산 구간의 VALID 날짜 수
active_days = 해당 가전을 10초 이상 사용한 VALID 날짜 수
daily_use_probability = active_days / sample_days
reliability_weight = min(sample_days / 최소 표본일, 1)
```

사용 루틴의 마감 시각은 가전별 일일 첫 사용 시각의 90백분위수(P90)로 계산합니다. 평균보다
늦은 정상 사용을 허용하여 `ROUTINE_MISSED` 오탐을 줄이기 위한 선택입니다. 전체 기준과
함께 요일별 표본도 `baseline_data.weekday_profiles`에 저장하며, 해당 요일의 유효 표본이
기본 4일 이상일 때만 요일별 마감 시각을 사용합니다. 표본이 부족하면 전체 요일 기준으로
대체합니다.

일 사용확률이 기본 `0.70` 이상이고 첫 사용 시각이 있는 행만 `enabled=true`가 됩니다.
탐지 시에는 `analysis_policy`의 `minimum_baseline_strength` 검사도 적용되므로 드물게
사용하는 가전은 이벤트 대상에서 제외됩니다.

같은 `household_id + appliance_type + baseline_type` 행을 갱신하므로 같은 날짜 작업이
재실행되어도 기준선 행이 중복되지 않습니다. 초기 로컬 시연이 가능하도록
`config/baselines.json` 값은 DB에 해당 키가 없을 때만 부트스트랩 행으로 등록되고,
충분한 `VALID` 데이터가 쌓이면 일일 계산 결과로 교체됩니다.

| 환경변수 | 기본값 | 의미 |
| --- | ---: | --- |
| `ROUTINE_BASELINE_WINDOW_DAYS` | 28 | 기준선을 계산할 최근 달력 일수 |
| `ROUTINE_BASELINE_MINIMUM_SAMPLE_DAYS` | 14 | 생성·갱신에 필요한 최소 VALID 일수 |
| `ROUTINE_BASELINE_MINIMUM_WEEKDAY_SAMPLE_DAYS` | 4 | 요일별 기준을 사용하기 위한 최소 표본 |
| `ROUTINE_BASELINE_MINIMUM_DAILY_USE_PROBABILITY` | 0.70 | 기준선 활성화 최소 일 사용확률 |

## 데이터 품질 이벤트 계약

데이터 수집 상태 변화는 `analysis.data-quality.v1`으로 발행하며 Kafka Key는
`household_id`입니다.

```json
{
  "event_id": "1bb4edcf-77ae-44d6-ad7c-b87a6e071f5a",
  "household_id": "H001",
  "event_type": "DATA_GAP",
  "occurred_at": "2026-09-16T10:02:00+09:00",
  "reason": {
    "last_valid_received_at": "2026-09-16T10:00:00+09:00",
    "gap_seconds": 120
  }
}
```

복구 시에는 같은 계약에서 `event_type`을 `DATA_RECOVERED`로 발행합니다. 가구별로
마지막 정상 입력 수신 시각을 추적하며 기본 120초 동안 새 입력이 없으면 `DATA_GAP`을
한 번 발행합니다. 이후 기본 3개의 최신 측정값이 연속으로 확인되면
`DATA_RECOVERED`를 한 번 발행합니다.

공백 상태와 복구 확인 중에는 위험 이벤트 판단을 보류합니다. 공백 전에 열려 있던 가전
세션은 마지막 유효 측정 시각에서 종료하고, 복구 첫 입력에서 모델 버퍼와 히스테리시스
상태를 초기화하여 공백 전후 데이터를 하나의 연속 사용으로 해석하지 않습니다.

```json
{
  "event_id": "b5e4ce49-b101-5a3a-b476-0f93512dd427",
  "household_id": "H001",
  "event_type": "DATA_RECOVERED",
  "occurred_at": "2026-09-16T10:05:05+09:00",
  "reason": {
    "last_valid_received_at": "2026-09-16T10:00:00+09:00",
    "gap_detected_at": "2026-09-16T10:02:00+09:00",
    "gap_seconds": 305
  }
}
```

| 환경변수 | 기본값 | 의미 |
| --- | ---: | --- |
| `ANALYSIS_DATA_GAP_THRESHOLD_SECONDS` | 120 | 입력 공백 판정 시간 |
| `ANALYSIS_DATA_QUALITY_POLL_SECONDS` | 5 | watchdog 검사 주기 |
| `ANALYSIS_DATA_RECOVERY_CONFIRMATION_SAMPLES` | 3 | 복구 확정에 필요한 최신 입력 수 |

## 로컬 실행

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
Copy-Item .env.example .env
python -m realtime_analysis
```

실제 Kafka 환경에 맞게 `.env`의 접속 주소와 토픽을 수정합니다. 기본 baseline은
`config/baselines.json`에 있으며, 현재 예시는 `H001`의 `MICROWAVE` 루틴입니다.

## 상태 확인과 Prometheus 메트릭

분석 프로세스는 기본적으로 `0.0.0.0:8000`에서 내부 관측용 HTTP 서버를 함께 실행합니다.

| 경로 | 성공 상태 | 용도 |
| --- | --- | --- |
| `GET /health` | `200` | 프로세스 생존 확인 |
| `GET /ready` | `200`, 미준비 시 `503` | Kafka, PostgreSQL, 모델 준비 확인 |
| `GET /metrics` | `200` | Prometheus exposition format |

바인딩은 `HTTP_HOST`, `HTTP_PORT`, `READINESS_TIMEOUT_SECONDS` 환경변수로 변경할 수
있습니다. Prometheus는 Compose 내부에서 `realtime-analysis-service:8000/metrics`를
수집하며 이 포트를 공용 인터넷에 공개하지 않습니다.

E2E 지연은 원본 `observed_at`과 Snapshot `published_at`의 차이로 측정합니다.
`ANALYSIS_E2E_CLOCK_SKEW_TOLERANCE_SECONDS`의 기본값은 `0.1`(100ms)이며, 이 범위의
음수 지연은 호스트와 컨테이너 사이의 미세한 시계 차이로 보고 0초로 기록합니다.
허용 범위를 초과한 음수 지연만 `ClockSkew` 오류로 집계합니다.

Grafana의 단계별 레이턴시, E2E 지연, Consumer Lag, 처리량 패널에는 각각
`nilm_analysis_stage_duration_seconds`, `nilm_analysis_e2e_duration_seconds`,
`nilm_analysis_consumer_lag_messages`, `nilm_analysis_messages_total`을 사용합니다.

패턴 감지와 일일 작업은 다음 메트릭으로 별도 계측합니다.

| 메트릭 | 라벨 | 의미 |
| --- | --- | --- |
| `nilm_pattern_detection_duration_seconds` | `pattern` | 실제 패턴 알고리즘 호출 시간 Histogram |
| `nilm_pattern_detection_total` | `pattern`, `result` | 패턴 판정 결과 누적 건수 |
| `nilm_pattern_events_total` | `event_type` | Kafka 발행에 성공한 패턴 이벤트 누적 건수 |
| `nilm_daily_job_duration_seconds` | `job` | 일일 작업 전체 실행시간 Histogram |
| `nilm_daily_job_runs_total` | `job`, `status` | 일일 작업 성공·오류 누적 건수 |

실시간 `pattern`은 `ROUTINE_MISSED`, `PROLONGED_INACTIVITY`,
`PROLONGED_APPLIANCE_USE`, 일일 패턴은 `ROUTINE_CHANGED`를 사용합니다. 실시간 판정
Counter와 Histogram은 공통 평가 주기 및 데이터 유효성 검사를 통과하여 실제 알고리즘을
호출한 경우에만 증가합니다. `result=detected`는 cooldown·중복 제거 전 후보가 나온 경우이며,
최종 발행 성공 건수는 `nilm_pattern_events_total`로 확인합니다.

고정 라벨을 사용하는 Counter는 프로세스 시작 시 가능한 라벨 조합을 `0`으로 노출하여,
Prometheus가 첫 증가 전에 한 번 이상 수집했다면 첫 이벤트도 `rate()`와 `increase()`에
포함됩니다. 동일 가구·가전·날짜의 `ROUTINE_MISSED`가 이미 발행된 경우에는 이후 평가에서
해당 기준선을 조기에 제외합니다. 다른 가전과 다음 날짜의 후보는 계속 평가합니다.

일일 `job`은 `activity_index`, `routine_changed`, `baseline_update`, `status`는
`success`, `error`를 사용합니다. `ROUTINE_CHANGED` 정책이 없으면 패턴 판정 결과에는
`result=skipped`가 기록되지만 일일 작업 자체는 성공으로 종료됩니다.

## analysis_db 스키마

`analysis_db` 접근에는 SQLAlchemy를 사용하고, 테이블 변경 이력은 Alembic으로 관리합니다.
최초 마이그레이션은 다음 6개 테이블을 생성합니다.

```text
model_artifact
routine_baseline
analysis_policy
household_observation_daily
household_activity_daily
appliance_usage_session
```

로컬 Python 실행 전 `.env`의 `DATABASE_*` 값을 맞춘 뒤 아래 명령으로 최신 스키마를
적용합니다.

```powershell
alembic upgrade head
```

로컬 Compose에서는 `realtime-analysis-service` 컨테이너가 시작될 때 같은 명령을 먼저
실행하므로 별도로 적용할 필요가 없습니다. ON/OFF 상태 변화와 ON 유지 중 확률은
Repository를 통해 `household_observation_daily`, `household_activity_daily`,
`appliance_usage_session`에 저장합니다.

```text
TURNED_ON → 일일 관측/활동 UPSERT + 세션 INSERT + event_count 증가
ON 유지   → 열린 세션의 max_probability 갱신
TURNED_OFF → 열린 세션의 ended_at 갱신
```

세션 INSERT와 `event_count` 증가는 하나의 DB 트랜잭션으로 처리하며, 동일한 세션 시작이
재처리되면 기존 세션을 사용해 `event_count`가 중복 증가하지 않도록 합니다.

## 실제 모델 backend

배포용 `realtime-analysis-service`는 `MODEL_BACKEND=real`로 실행하며 저장소에 포함된
가전별 실제 체크포인트 6개를 로드합니다. 입력은 기존 `power.raw.v1`, 출력은 기존
`analysis.snapshot.v1` 계약을 유지합니다. 모델이 요구하는 입력 창은 255초입니다.

```env
MODEL_BACKEND=real
MODEL_ASSET_ROOT=/assets
MODEL_WINDOW_SIZE=255
MODEL_DEVICE=cpu
MODEL_DTYPE=float32
MODEL_THREADS=1
```

실제 모델은 자체 정규화와 가전별 ON/OFF threshold를 사용하므로
`StandardizingPredictor`나 임시 `config/model_manifest.json` threshold를 거치지 않습니다.
체크포인트 로딩 또는 추론 실패 시 FakePredictor로 대체하지 않습니다.

`MODEL_BACKEND=selected_scene`은 고정된 검수 장면 하나와 가전 하나를 재현하는 별도
모드입니다. 전용 입력·출력 계약은 [실제 모델 문서](docs/real-model.md)를 참고합니다.

현재 6개 모델은 서로 다른 선택 장면에서 각각 검증됐습니다. 실시간 서버가 6개 결과를
합칠 수 있다는 사실만으로 임의 가구에 대한 동시 추론 정확도가 보장되지는 않습니다.

`FAKE_ON_APPLIANCES`에 쉼표로 가전명을 지정하면 FakePredictor가 해당 가전을 ON으로
간주할 수 있도록 확률 `1.0`을 반환하고, 나머지는 `0.0`을 반환합니다. 최종 ON/OFF는
`config/model_manifest.json`의 가전별 threshold를 적용해 판정합니다.

```env
FAKE_ON_APPLIANCES=MICROWAVE,HAIR_DRYER
```

빈 값이면 모든 가전을 OFF로 반환하므로 마감 시각 이후 `ROUTINE_MISSED` 흐름을 확인할
수 있습니다.

`MODEL_MANIFEST_FILE`로 Manifest 경로를 변경할 수 있습니다. 서비스 시작 시 입력 shape,
Feature 순서, 출력 가전 순서, sigmoid 출력, `mean`, 양수 `std`와 threshold 범위를
검증합니다. Predictor 호출 직전에 Feature별 `(x - mean) / std`를 적용하고, Predictor가
반환한 확률에는 Manifest의 가전별 threshold를 적용합니다. Snapshot에는 정규화 전 최신
원본 측정값을 담습니다. 현재 Manifest의 `mean=0`, `std=1`과 `0.5` threshold는 실제 모델
전달 전까지 사용하는 중립 임시값입니다.

## 일일 관측 집계

입력 검증을 통과한 가구별 샘플을 `household_observation_daily`에 즉시 누적합니다.

```text
sample_count += 1
coverage_ratio = min(sample_count / expected_sample_count, 1.0)
```

기본 1Hz 전일 수집의 `expected_sample_count`는 86,400이며, 수집 중에는 상태를
`COLLECTING`으로 유지합니다. 다음 날짜의 첫 입력이 들어오면 이전의 `COLLECTING` 행을
다음 기준으로 마감합니다.

```text
sample_count = 0                 → SENSOR_GAP
coverage_ratio >= 유효 기준값   → VALID
그 외                           → INSUFFICIENT_DATA
```

| 환경변수 | 기본값 | 의미 |
| --- | ---: | --- |
| `ANALYSIS_EXPECTED_SAMPLES_PER_DAY` | 86400 | 하루 예상 샘플 수 |
| `ANALYSIS_OBSERVATION_VALID_COVERAGE_RATIO` | 0.95 | `VALID` 최소 수집률 |

현재 유효 수집률 `0.95`는 요구사항의 TBD 값을 설정으로 분리한 임시 운영값입니다.

## ON/OFF 상태 변화

모델 확률은 다음 순서로 안정화합니다.

```text
OFF 상태에서 probability >= threshold가 3회 연속 → TURNED_ON
ON 상태에서 probability <= threshold - 0.05가 3회 연속 → TURNED_OFF
threshold - 0.05 < probability < threshold → 기존 ON 상태 유지
```

마지막 구간은 히스테리시스 영역이다. 확률이 threshold 주변에서 흔들릴 때 ON/OFF가
매초 반복되는 것을 방지한다. 변화가 처음 관측된 시각은 `occurred_at`, 연속 조건을
만족한 시각은 `confirmed_at`으로 구분한다.

| 환경변수 | 기본값 | 의미 |
| --- | ---: | --- |
| `APPLIANCE_ON_CONFIRMATION_SAMPLES` | 3 | ON 확정에 필요한 연속 예측 수 |
| `APPLIANCE_OFF_CONFIRMATION_SAMPLES` | 3 | OFF 확정에 필요한 연속 예측 수 |
| `APPLIANCE_OFF_THRESHOLD_MARGIN` | 0.05 | OFF 판정용 히스테리시스 폭 |

## 단계별 구간 타이밍 로그

Kafka 메시지 한 건을 처리할 때 `realtime_analysis.pipeline_timing` 로거가 JSON 한 줄을
출력합니다. 구간 시간은 시스템 시각 변경의 영향을 받지 않도록
`time.perf_counter_ns()`로 측정합니다.

```json
{"event":"pipeline_timing","status":"processed","message_id":"8f3b2a19-4d6e-4c72-9b12-a1b2c3d4e5f6","household_id":"H001","kafka":{"topic":"power.raw.v1","partition":3,"offset":42},"processing_total_ns":1842000,"stage_durations_ns":{"activity_db":310000,"buffer_append":12000,"deserialize_validate":82000,"inference":1500000,"offset_store":73000,"preprocess":21000,"snapshot_publish_ack":260000,"state_decision_transition":31000},"stage_counts":{"activity_db":1,"buffer_append":1,"deserialize_validate":1,"inference":1,"offset_store":1,"preprocess":1,"snapshot_publish_ack":1,"state_decision_transition":1},"sensor_to_log_ns":2185000,"clock_skew_detected":false}
```

주요 구간 이름은 다음과 같습니다.

| 구간 | 범위 |
| --- | --- |
| `consumer_poll` | Kafka Consumer `poll()` 대기 |
| `deserialize_validate` | JSON 역직렬화와 Pydantic 입력 검증 |
| `observation_db` | 일일 관측 Sample DB 반영 |
| `buffer_append` | 가구별 Sliding Window 추가 |
| `preprocess` | Manifest 기반 Feature 표준화 |
| `inference` | Predictor 단독 실행 |
| `state_decision_transition` | Threshold, Hysteresis, 상태 전이 판정 |
| `activity_db` | 일일 활동과 사용 Session DB 반영 |
| `snapshot_publish_ack` | Snapshot Produce부터 Broker ACK까지 |
| `anomaly_detection` | 기준선 조회와 이상 후보 판정 |
| `event_publish_ack` | 이상 Event Produce부터 Broker ACK까지 |
| `offset_store` | 처리 완료 Offset를 로컬 저장(안정 상태에서 자동 Commit) |
| `dlq_publish_ack` | 잘못된 입력의 DLQ Broker ACK |

같은 구간이 메시지 한 건에서 여러 번 실행되면 `stage_durations_ns`에는 합계,
`stage_counts`에는 호출 횟수를 기록합니다. `status`는 `processed`, `dlq`, `failed` 중
하나이며, 실패 로그에는 예외 메시지 대신 `error_type`만 남겨 민감한 Payload가 로그에
유출되지 않게 합니다.

`sensor_to_log_ns`는 센서 `measured_at`부터 분석 메시지 처리 종료까지의 벽시계
지연입니다. 서로 다른 장비의 시계를 사용하는 E2E 측정에서는 NTP 동기화가 필요하며,
음수이면 `clock_skew_detected=true`로 표시합니다.

## 현재 MVP 제약

- 실제 AI 모델 대신 결정적인 FakePredictor를 사용합니다.
- 실제 학습 `mean`, `std`와 가전별 Validation threshold는 AI 모델 전달 후 교체해야 합니다.
- 당일 `ROUTINE_MISSED` 중복 상태와 사용 여부는 아직 프로세스 메모리에 저장합니다.
- `config/baselines.json`은 최초 DB 부트스트랩과 로컬 테스트 가구 목록에만 사용합니다.
- 재시작하면 299개 버퍼와 당일 활동·발행 상태가 초기화됩니다.
- 완전히 데이터가 들어오지 않은 가구의 `SENSOR_GAP` 판정에는 별도 가구 목록 기반 마감
  스케줄러가 추가로 필요합니다.
- 외출 정보는 `household_outing_state` 한 행에 최근 외출 구간만 보관하므로, 완료된
  과거 외출 여러 건을 조회하는 용도로는 사용하지 않습니다.
- HTTP API는 포함하지 않습니다.

## 외출 연동 패턴 Kafka 검증

외출 메시지를 저장하는 것만으로 패턴 감지가 실행되지는 않습니다. 패턴 감지는
`power.raw.v1` 처리 시 실행되므로 각 확인 시각에 같은 가구의 정상 전력 입력이 필요합니다.

### 준비

```powershell
docker exec nilm-kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:19092 `
  --create --if-not-exists `
  --topic monitoring.household-presence.v1 `
  --partitions 24 `
  --replication-factor 1

docker compose up -d --build realtime-analysis-service
docker compose --profile tools up -d kafka-ui
```

Kafka UI는 `http://localhost:8091`에서 열고, 외출 메시지의 key는 `household_id`와 같은
값을 사용합니다. 적용된 Alembic revision은 `20260919_09`이어야 합니다.

```powershell
docker exec nilm-postgres psql -U nilm_admin -d analysis_db `
  -c "SELECT version_num FROM alembic_version;"
```

### ROUTINE_MISSED 외출 구간 겹침

결정적 입력:

```text
가구: H001
전자레인지 expected_until: 08:10
외출: 07:30~08:00
전자레인지 사용: 없음
```

Kafka UI의 `monitoring.household-presence.v1`에 key `H001`로 차례대로 발행합니다.

```json
{
  "event_id": "da898d82-3183-4d12-ae7c-0e3a9df9772c",
  "household_id": "H001",
  "event_type": "OUTING_STARTED",
  "occurred_at": "2026-09-17T07:30:00+09:00"
}
```

```json
{
  "event_id": "160a931f-f59d-403d-8157-fccdeeb88dbe",
  "household_id": "H001",
  "event_type": "OUTING_ENDED",
  "occurred_at": "2026-09-17T08:00:00+09:00"
}
```

DB의 단일 상태 행에 최근 외출 시작·종료 시각이 남았는지 확인합니다.

```powershell
docker exec nilm-postgres psql -U nilm_admin -d analysis_db -c `
  "SELECT household_id, is_outing, outing_started_at, last_returned_at FROM household_outing_state WHERE household_id='H001';"
```

시뮬레이터 기준 날짜를 `2026-09-17`로 설정하고 H001에서 전자레인지를 사용하지 않는
입력을 `08:10` 이후까지 발행합니다. `analysis.event.v1`에
`H001 + MICROWAVE + ROUTINE_MISSED`가 없어야 성공입니다. 외출과 겹치지 않는 다른
가전의 판단은 계속 수행됩니다.

### PROLONGED_INACTIVITY 외출 보류와 귀가 후 재계산

결정적 입력:

```text
2026-09-16 10:00 마지막 가전 사용 종료
2026-09-16 17:00 OUTING_STARTED
2026-09-16 21:00 원천 입력: 외출 중이므로 감지 보류
2026-09-16 22:00 OUTING_ENDED
2026-09-16 22:00~23:00 깨어 있는 미활동 1시간
2026-09-16 23:00~2026-09-17 07:00 수면 제외
2026-09-17 07:00~11:59 누적 5시간 59분: 이벤트 없음
2026-09-17 12:00 누적 6시간: 이벤트 한 번 발행
```

외출 시작과 귀가 메시지는 다음과 같습니다.

```json
{
  "event_id": "c88be83b-2ac8-47d1-b644-193874289a79",
  "household_id": "H001",
  "event_type": "OUTING_STARTED",
  "occurred_at": "2026-09-16T17:00:00+09:00"
}
```

```json
{
  "event_id": "3dd2e0c8-2832-4dcb-b71d-73007d834827",
  "household_id": "H001",
  "event_type": "OUTING_ENDED",
  "occurred_at": "2026-09-16T22:00:00+09:00"
}
```

각 메시지 발행 후 상태를 확인한 다음 시뮬레이터의 가상 시각을 진행합니다.

```powershell
docker exec nilm-postgres psql -U nilm_admin -d analysis_db -c `
  "SELECT household_id, is_outing, outing_started_at, last_returned_at FROM household_outing_state WHERE household_id='H001';"
```

기대 이벤트의 핵심 값은 다음과 같습니다.

```json
{
  "household_id": "H001",
  "event_type": "PROLONGED_INACTIVITY",
  "occurred_at": "2026-09-17T03:00:00+00:00",
  "reason": {
    "last_returned_at": "2026-09-16T13:00:00+00:00",
    "inactivity_started_at": "2026-09-16T13:00:00+00:00",
    "threshold_hours": 6.0,
    "sleep_window": {"start": "23:00", "end": "07:00"}
  }
}
```

Consumer offset과 lag는 다음 명령으로 확인합니다.

```powershell
docker exec nilm-kafka /opt/kafka/bin/kafka-consumer-groups.sh `
  --bootstrap-server localhost:19092 `
  --describe `
  --group realtime-analysis-service-outing-v1
```

정상 메시지 처리 후 해당 파티션의 lag가 0이어야 합니다. 잘못된 `event_type`이나 타임존
없는 `occurred_at`을 보내면 상태는 바뀌지 않고 `dlq.analysis`에
`VALIDATION_ERROR`가 생성되어야 합니다.
# R3 실제 모델

고정 체크포인트 Predictor와 로컬 실제 추론 실행기는
[실행·통합 범위 안내](docs/real-model.md)를 참고하세요.
