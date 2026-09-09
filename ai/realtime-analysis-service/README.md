# Realtime Analysis Service

Kafka 전력 데이터를 검증하고, 가구별 입력 버퍼와 MVP용 가전 ON/OFF 예측을 거쳐
평소 루틴이 누락된 가구의 이상 이벤트를 `analysis.event.v1`로 발행합니다.

이 문서는 전력 데이터 시뮬레이터·MQTT-Kafka Bridge 담당자와
`analysis.event.v1` 이상 이벤트 수신 담당자가 함께 사용하는 연동 계약입니다.

## MVP 처리 흐름

```text
power.raw.v1
  -> 입력 검증 및 가구별 299개 버퍼
  -> FakePredictor 가전 6종 ON/OFF 결과
  -> JSON baseline과 일일 사용 상태 비교
  -> score가 임계치 이상이면 analysis.event.v1 발행
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
완료된 뒤에만 수동으로 commit합니다.

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
| 현재 MVP 이벤트 의미 | 가구의 평소 활동 루틴 누락 후보 |
| 전달 보장 | At-least-once, 논리적 중복 가능 |

### MVP 출력 계약

```json
{
  "event_id": "f46567d2-2b29-486f-9051-8cae3ff28d6a",
  "household_id": "H001",
  "score": 86,
  "occurred_at": "2026-09-08T03:41:06.120000Z",
  "reason": {
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
| `score` | Integer | 0~100 범위 이상 점수 |
| `occurred_at` | ISO 8601 Timestamp | 이상을 판단한 시각, UTC로 발행 |
| `reason` | JSON Object | 알고리즘이 판단에 사용한 근거 |

`reason`은 사용자에게 바로 노출할 한국어 문장이 아니라 알고리즘별 근거 데이터입니다.
화면 문구가 필요하면 수신 서비스에서 해당 값을 이용해 만듭니다.

```text
최근 14일 중 12일 동안 08:10 이전에 확인된 활동이 오늘은 감지되지 않았습니다.
```

### 현재 점수 의미

현재 MVP의 점수 공식은 다음과 같습니다.

```text
score = normal_days / window_days × 100 × reliability_weight
```

예시 기준선에서는 다음과 같이 86점이 됩니다.

```text
12 / 14 × 100 × 1.0 = 85.71 → 86
```

분석 서비스의 임계치가 80이면 `86 >= 80`이므로 이벤트를 발행합니다.
점수 계산식과 임계치는 MVP 검증용이며 추후 변경될 수 있습니다.

### 수신 및 중복 처리 주의사항

- Kafka Consumer Group은 수신 서비스 전용 이름을 사용합니다.
- 비즈니스 처리가 끝난 뒤에 Offset을 커밋합니다.
- `event_id`가 같은 이벤트를 다시 받으면 중복 처리하지 않습니다.
- 분석 서비스 재시작 시 같은 날짜의 논리적으로 동일한 이벤트가 새 `event_id`로 다시 발행될 수 있습니다.
- 현재 5개 필드 계약에는 `event_type`과 `appliance_type`이 없습니다.
- 현재 토픽의 이벤트는 가구 단위 `ROUTINE_MISSED` 후보로 해석합니다.
- 여러 이벤트 유형이나 가전 표시가 필요해지면 기존 필드 의미를 바꾸지 않고 새 계약을 합의합니다.

MVP에서 논리적 중복을 줄이려면 `event_id` 외에도 다음 조합을 임시 중복 기준으로 사용할 수
있습니다.

```text
household_id + occurred_at의 한국 날짜 + reason.expected_until
```

### 이상 이벤트 수신 체크리스트

- [ ] `analysis.event.v1`을 전용 Consumer Group으로 구독하는가
- [ ] Kafka Key를 String으로 읽는가
- [ ] Value를 UTF-8 JSON으로 역직렬화하는가
- [ ] `score`를 0~100으로 처리하는가
- [ ] `occurred_at`을 UTC로 파싱하는가
- [ ] `reason`을 고정 문자열이 아닌 JSON Object로 처리하는가
- [ ] 이벤트 처리 완료 후 Offset을 커밋하는가
- [ ] 중복 이벤트가 와도 알림이나 Incident가 중복 생성되지 않는가

### 발행 성공의 범위

분석 서비스의 Producer `flush()`는 이벤트가 Kafka 브로커에 전달됐는지만 확인합니다.
이벤트 수신 서비스가 메시지를 가져가 비즈니스 처리를 완료했는지는 확인하지 않습니다.
수신 완료 응답 이벤트는 현재 MVP 범위에 포함하지 않습니다.

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

`FAKE_ON_APPLIANCES`에 쉼표로 가전명을 지정하면 FakePredictor가 해당 가전을 ON으로
반환합니다.

```env
FAKE_ON_APPLIANCES=MICROWAVE,HAIR_DRYER
```

빈 값이면 모든 가전을 OFF로 반환하므로 마감 시각 이후 `ROUTINE_MISSED` 흐름을 확인할
수 있습니다.

## 현재 MVP 제약

- 실제 AI 모델 대신 결정적인 FakePredictor를 사용합니다.
- baseline과 당일 활동 상태는 각각 JSON과 메모리에 저장합니다.
- 재시작하면 299개 버퍼와 당일 활동·발행 상태가 초기화됩니다.
- HTTP API와 DB 연동은 포함하지 않습니다.
