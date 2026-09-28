# 결정적 E2E 시나리오 시뮬레이터 사용 가이드

AI 패턴 감지 검증용 **결정적(deterministic) 시나리오 10종**을 MQTT로 발행하는 방법을 정리한 운영 문서입니다.

물리 엔진·파형 모델·레거시 시연 모드의 상세 설명은 [README.md](README.md)에 있습니다. 이 문서는 "시나리오를 어떻게 돌리고, 결과를 어떻게 읽는가"만 다룹니다.

---

## 1. 두 가지 실행 경로

시뮬레이터에는 성격이 다른 두 경로가 있습니다. **섞어 쓰면 안 되고, 동시에 실행되지도 않습니다** (한쪽이 돌고 있으면 다른 쪽은 409를 반환).

| | 레거시 데모 경로 | **결정적 E2E 경로** |
|---|---|---|
| 진입점 | `POST /api/start` | `POST /api/e2e/runs` |
| 시나리오 | `peak`, `normal_routine`, `routine_missed`, `sensor_fault`, `random`, `manual` | `ACTIVITY_*` 6종, `ROUTINE_CHANGED_*` 3종, `BASELINE_MICROWAVE_20D` |
| 용도 | 발표 시연, 부하 테스트, 파형 눈으로 보기 | **AI 판정 검증** |
| 재현성 | 확률 기반(시나리오에 따라 다름) | 시나리오·기준일·가구가 같으면 항상 동일 |
| 일자별 완료 보고 | 없음 | 있음 (`day_results`) |

**AI 팀에 넘길 데이터를 만들 때는 반드시 E2E 경로를 씁니다.**

---

## 2. 기동

```bash
cd infrastructure/mqtt/simulator
python web_server.py
```

- 웹 패널: `http://127.0.0.1:8085`
- 기본 브로커: `localhost:1883` / `simulator_user`
- 포트 변경: `python web_server.py 8089` 또는 `--web-port 8089`
- TLS 운영 접속: `--tls --host <broker> --user <id> --password <pw> --ca-file ca.crt`

> `--bind-host 0.0.0.0`은 인증 없는 제어 API를 외부에 여는 것이므로 로컬 검증 외에는 쓰지 마세요.

웹 패널의 E2E 제어판에서 드롭다운으로 시나리오를 고르는 방법도 동일하게 동작합니다. 아래 API는 자동화·핸드오프용입니다.

---

## 3. 시나리오 10종

| 시나리오 | 일수 | 발행 샘플 | 내용 |
|---|---:|---:|---|
| `ACTIVITY_NORMAL` | 1 | 86,400 | 전기포트 3회·전자레인지 2회·청소기 1회 (총 2,160초) |
| `ACTIVITY_LOW` | 1 | 86,400 | 12:00 전자레인지 180초 1회 |
| `ACTIVITY_NONE` | 1 | 86,400 | 대상 가전 0회 (기저부하·냉장고만) |
| `ACTIVITY_INSUFFICIENT` | 1 | **82,079** | 22:47:59부터 4,321건 결측 → 수집률 94.999% |
| `ACTIVITY_SESSION_MERGE` | 1 | 86,400 | 인덕션 10:00·10:11 각 600초, 사이 60초 OFF |
| `ACTIVITY_DURATION_CAP` | 1 | 86,400 | 전기포트 10:00부터 7,200초 연속 |
| `ROUTINE_CHANGED_LATER` | 28 | 2,419,200 | 이전 21일 08:00 → 최근 7일 10:30 |
| `ROUTINE_CHANGED_EARLIER` | 28 | 2,419,200 | 이전 21일 10:30 → 최근 7일 08:00 |
| `ROUTINE_CHANGED_WITHIN_THRESHOLD` | 28 | 2,419,200 | 08:00 → 09:30 (90분, 임계값 미만 → 미발행 기대) |
| `BASELINE_MICROWAVE_20D` | 20 | 1,728,000 | 20일 중 17일 전자레인지 08:00 60초 |

목록을 코드에서 직접 받으려면:

```bash
curl -s http://127.0.0.1:8085/api/e2e/scenarios
```

---

## 4. `reference_date`는 **마지막 날**입니다

다일 시나리오에서 가장 자주 나오는 실수입니다. 시작일은 `reference_date - (총일수 - 1)`로 역산됩니다.

| 시나리오 | `reference_date=2026-09-16`일 때 실제 발행 범위 |
|---|---|
| 단일 일자 `ACTIVITY_*` | 2026-09-16 하루 |
| `BASELINE_MICROWAVE_20D` | 2026-08-28 ~ 2026-09-16 |
| `ROUTINE_CHANGED_*` | 2026-08-20 ~ 2026-09-16 |

AI의 루틴/기준선 분석이 기준 시점에서 N일을 거슬러 보기 때문에 API가 종료일을 받도록 설계돼 있습니다.

---

## 5. 실행 시작

```bash
curl -X POST http://127.0.0.1:8085/api/e2e/runs \
  -H 'Content-Type: application/json' \
  -d '{
    "reference_date": "2026-09-16",
    "execution": { "mode": "ACCELERATED" },
    "households": [
      { "household_id": "H001", "scenario": "ACTIVITY_NORMAL" },
      { "household_id": "H002", "scenario": "ACTIVITY_INSUFFICIENT" }
    ]
  }'
```

> `speed`를 생략하면 가구 수에 맞춰 안전 배속이 자동 산출됩니다. 위 예시(2가구)는 가구당 600배속 = 합계 1,200/s로 해결됩니다. 근거는 6절을 보세요.

응답은 `202 Accepted`:

```json
{
  "status": "accepted",
  "run_id": "run_3f0c…",
  "state": "STARTING",
  "households": [{ "household_id": "H001", "scenario": "ACTIVITY_NORMAL", "state": "STARTING" }]
}
```

**필드 규칙**

| 필드 | 필수 | 설명 |
|---|---|---|
| `reference_date` | O | `YYYY-MM-DD`. 시나리오의 마지막 날 |
| `execution.mode` | O | `REALTIME` / `ACCELERATED` / `BURST` |
| `execution.speed` | X | ACCELERATED에서만 지정 가능. **생략하면 안전 배속 자동 산출**(아래 참조). 0보다 큰 유한한 수여야 하며, 다른 모드에서 넣으면 400 |
| `households` | O | 1~10개. `household_id`는 `H001`~`H010`, 중복 불가 |
| `start_time` | X | 발행 개시 **실제 벽시계 시각**(KST). `HH:MM[:SS]` 또는 ISO. 과거면 즉시 시작 |
| `timezone` | X | `Asia/Seoul`만 허용 |

`start_time`은 가상 시각이 아니라 "언제 발행을 시작할지"입니다. 가상 시각은 언제나 해당 날짜 00:00:00 KST부터 1초 간격으로 진행합니다.

가구마다 다른 시나리오를 줄 수 있으므로, H001에 정상 데이터·H002에 결측 데이터를 동시에 태워 비교하는 구성이 가능합니다.

---

## 6. 실행 모드 선택

| 모드 | 발행 속도 | 1일 소요 | 용도 |
|---|---|---|---|
| `REALTIME` | 1건/초 | 24시간 | 실시간 대시보드 시연 |
| `ACCELERATED` (speed=N) | N건/초 | 86,400/N 초 | **일반 검증** (speed 생략 시 자동) |
| `BURST` | 대기 없이 최고속 (실측 1,480~2,700건/초) | 약 1분 | 단일 일자 검증, MQTT 계약 검증 |

**세 모드 모두 샘플을 생략하지 않습니다.** BURST는 sleep만 제거할 뿐 발행 건수는 동일합니다.

### Kafka 도달률과 발행 속도

브리지가 Kafka 확인 후에야 MQTT를 ack하므로 **발행자까지 이어지는 백프레셔 경로가 없습니다.** 브로커의 브리지용 송신 큐가 유일한 완충 장치이고, 넘치면 브로커가 조용히 버립니다(브리지는 에러를 남기지 않습니다).

로컬 스택 실측 (2026-09-20, 브로커 큐 설정 반영 후):

| 설정 | 총 발행 | 실측 속도 | Kafka 도달 | 브로커 유실 | 도달률 |
|---|---:|---:|---:|---:|---:|
| BURST 1가구 (1일) | 86,400 | 1,476/s | 86,400 | 0 | **100%** |
| BURST 2가구 (1일) | 172,800 | 2,037/s | 172,800 | 0 | **100%** |
| BURST 3가구 (1일) | 259,200 | 2,703/s | 259,200 | 0 | **100%** |
| **BURST 1가구 (20일)** | **1,728,000** | **1,541/s** | **1,728,000** | **0** | **100%** |

브로커 큐 설정이 반영되기 전에는 동일 조건(1일 BURST)이 56.4%였습니다. 설정 반영 이후 위 조건에서는 유실이 없습니다.

### 완충 구조 이해하기

- **브리지 실효 처리량: 약 1,477/s** (20일 실행의 드레인 시간 49초에서 역산)
- **브로커 큐 깊이: 100,000건** (`max_queued_messages`)

> **전제 조건: 브로커가 이 큐 설정으로 기동돼 있어야 합니다.**
> `infrastructure/mqtt/config/mosquitto.local.conf`에 `max_inflight_messages 1000` / `max_queued_messages 100000`이 들어 있지만, **mosquitto는 이 옵션을 SIGHUP 리로드로 적용하지 않습니다.** 설정 파일이 바뀐 뒤 브로커를 재시작하지 않으면 기본값(실효 버퍼 약 1,040건)으로 동작하고, 위 무손실 수치는 전부 성립하지 않습니다(같은 조건 1일 BURST가 56.4%까지 떨어집니다).
>
> 적용 여부는 유실 카운터로 확인하세요. 실행 전후 값이 같아야 정상입니다.
>
> ```bash
> docker exec mosquitto-broker mosquitto_sub -h localhost -u simulator_user -P test1234 -t '$SYS/broker/publish/messages/dropped' -C 1
> ```
>
> PowerShell에서는 이 명령을 `sh -c "..."`로 감싸지 마세요. `$SYS`가 빈 문자열로 치환되어 없는 토픽을 구독하고 영원히 끝나지 않습니다.

발행 속도가 브리지 처리량을 넘으면 **초과분이 큐에 누적**됩니다. 20일 BURST는 초당 약 64건씩 쌓여 종료 시점 백로그가 약 72,500건이었습니다 — 큐 안에 들어왔지만 여유가 크진 않습니다.

```
누적량 ≈ (발행 속도 − 1,477) × 발행 시간(초)
이 값이 100,000을 넘으면 그 시점부터 유실
```

| 시나리오 | BURST 소요 | 예상 누적 백로그 | 판정 |
|---|---|---:|---|
| 단일 일자 | 약 1분 | ~4,000 | 안전 (실측 확인) |
| `BASELINE_MICROWAVE_20D` | 약 19분 | ~72,500 | 안전 (실측 확인) |
| `ROUTINE_CHANGED_*` (28일) | 약 26분 | **~100,000** | **경계 — 미검증** |

28일 BURST는 계산상 큐 한계에 정확히 걸치므로 권장하지 않습니다.

### 한계는 **합계 기준**입니다

가구당이 아니라 **브리지로 흘러드는 총 메시지/초**가 기준입니다. 가구를 늘리면 합계 속도가 비례해 올라갑니다.

### 자동 배속 (권장)

`speed`를 생략하면 브리지 처리량 아래(합계 1,200/s)로 자동 산출되어 **큐 누적 자체가 일어나지 않습니다.** 실행 길이와 무관하게 안전합니다.

```json
{ "mode": "ACCELERATED" }
```

| 가구 수 | 해결된 `speed_multiplier` | 합계 |
|---:|---:|---:|
| 1 | 1200.0 | 1,200/s |
| 2 | 600.0 | 1,200/s |
| 10 | 120.0 | 1,200/s |

해결된 값은 스냅샷의 `speed_multiplier`에 그대로 노출됩니다. `speed`를 명시하면 자동 산출은 개입하지 않습니다. 기준값은 [`server/config.py`](server/config.py)의 `DEFAULT_SAFE_AGGREGATE_RATE`입니다.

> 자동 배속은 **REST API에서만** 쓸 수 있습니다. 웹 제어 패널은 ACCELERATED 선택 시 배속 입력을 필수로 요구하므로, 패널에서는 위 표의 값을 직접 넣거나 BURST를 쓰세요.

### 어느 것을 쓸까

| 상황 | 권장 |
|---|---|
| 단일 일자, 빠르게 | **BURST** (약 1분, 실측 무손실) |
| 20일 이하 | BURST 또는 자동 배속 |
| 28일 | **자동 배속** (BURST는 큐 한계 경계) |
| MQTT 계약만 검증 (Kafka 무관) | BURST |
| 실시간 대시보드 시연 | REALTIME |

### 소요 시간

| 시나리오 | BURST | 자동 배속 (1가구 1,200/s) |
|---|---|---|
| 단일 일자 | 약 1분 | 약 1.2분 |
| `BASELINE_MICROWAVE_20D` | 약 19분 | 약 24분 |
| `ROUTINE_CHANGED_*` (28일) | 약 26분 (비권장) | 약 34분 |

---

## 7. 진행 상황 조회

### 세션 전체

```bash
curl -s http://127.0.0.1:8085/api/e2e/runs/{run_id}
```

```json
{
  "run_id": "run_3f0c…",
  "reference_date": "2026-09-16",
  "execution_mode": "ACCELERATED",
  "speed_multiplier": 600.0,
  "overall_status": "RUNNING",
  "households": [ { "...": "가구 스냅샷" } ]
}
```

### 개별 가구

```bash
curl -s http://127.0.0.1:8085/api/e2e/runs/{run_id}/households/H001
```

주요 필드:

| 필드 | 의미 |
|---|---|
| `state` | `STARTING` / `RUNNING` / `PAUSED` / `COMPLETED` / `STOPPED` / `FAILED` |
| `published_samples` | **실제** 발행 확정 수 |
| `planned_publish_samples` | 계획된 발행 수 (둘이 같아야 정상 완주) |
| `omitted_samples` | 계획된 결측 수 |
| `completed_days` / `total_days` | 일자 진행률 |
| `current_activity_date` | 현재 발행 중인 가상 날짜 |
| `day_results` | 완료된 일자별 결과 배열 |
| `last_error` | 실패 시 원인 |

`overall_status` 집계 규칙: 하나라도 `STARTING`이면 `STARTING`, 하나라도 `RUNNING/PAUSING/STOPPING`이면 `RUNNING`, 남은 게 `PAUSED`면 `PAUSED`, 전원이 종료 상태면 `COMPLETED`(전부 완주) / `STOPPED`(중단 섞임) / `FAILED`(실패 포함).

---

## 8. 제어

가구별로 독립 제어됩니다. 한 가구를 멈춰도 나머지는 계속 발행합니다.

```bash
# 일시정지 / 재개 / 개별 중단
curl -X POST http://127.0.0.1:8085/api/e2e/runs/{run_id}/households/H001/pause
curl -X POST http://127.0.0.1:8085/api/e2e/runs/{run_id}/households/H001/resume
curl -X POST http://127.0.0.1:8085/api/e2e/runs/{run_id}/households/H001/stop

# 세션 전체 중단
curl -X POST http://127.0.0.1:8085/api/e2e/runs/{run_id}/stop
```

- 이미 종료된 가구에 제어를 걸면 `409 TASK_ALREADY_TERMINAL`
- **중단된 실행기는 재실행할 수 없습니다.** 다시 돌리려면 새 런을 시작하세요.
- 일시정지 중에는 가상 시각도 멈춥니다. 재개하면 끊긴 지점부터 이어서 발행하므로 `measured_at`에 구멍이 생기지 않습니다.

---

## 9. 완료 판정과 결과 읽기

하루가 끝날 때마다 `day_results`에 한 건씩 확정 기록됩니다.

```json
{
  "scenario": "ACTIVITY_NORMAL",
  "household_id": "H001",
  "run_id": "run_3f0c…",
  "activity_date": "2026-09-16",
  "published_samples": 86400,
  "omitted_samples": 0,
  "status": "COMPLETED"
}
```

**정상 완주 판정 체크리스트**

1. 가구 `state == "COMPLETED"`
2. `published_samples == planned_publish_samples`
3. `len(day_results) == total_days`, 각 건이 `status: "COMPLETED"`
4. `last_error == null`

`published_samples`가 계획치에 못 미치는데 `COMPLETED`인 경우는 없습니다 (완료 직전 불변식 검증에서 걸립니다). 미달이면 `FAILED`로 떨어지고 `last_error`에 원인이 남습니다.

---

## 10. 발행 데이터 규격

- 토픽: `v1/power/sim/{household_id}/main`
- QoS 1, retain 없음
- `message_id`는 `(run_id, household_id, cycle, measured_at)` 기반 UUIDv5 → **같은 런 안에서는** 재전송돼도 ID가 같아 중복 식별에 쓸 수 있습니다. `run_id`는 런마다 새로 발급되므로 **다른 런끼리는 ID가 다릅니다** (같은 날짜를 두 번 태우면 다운스트림에서 중복으로 걸러지지 않습니다)
- `measured_at` / `ts`: 타임존 포함 ISO 8601 (`2026-09-16T07:00:00+09:00`)
- 가상 1초당 1건, 가구별 단조 증가
- **선언된 ON 구간은 연속 가동입니다.** 인덕션·다리미는 실제 기기에선 서모스탯 듀티 사이클로 동작하지만, 일정 기반 실행에서는 선언 구간이 곧 '사람이 사용한 구간'이므로 내부 휴지를 넣지 않습니다. 휴지가 들어가면 AI가 한 번의 사용을 수십 개 세션으로 쪼개고 사용시간 합계가 선언값보다 짧아집니다. (확률 기반 레거시 시뮬레이터의 듀티 사이클은 그대로 유지됩니다)

페이로드 14필드:

```json
{
  "message_id": "…", "household_id": "H001", "device_id": "main",
  "measured_at": "2026-09-16T07:00:00+09:00",
  "active_power": 1712.44, "reactive_power": 152.31, "apparent_power": 1719.2,
  "power_factor": 0.996, "voltage": 220.3, "current": 7.804,
  "house": "H001", "device": "main", "ts": "2026-09-16T07:00:00+09:00", "power_w": 1712.44
}
```

`house` / `device` / `ts` / `power_w`는 구 스키마 호환용 중복 필드입니다.

---

## 11. 발행 검증 도구

MQTT 계층에서 건수·타임스탬프·중복·계획 결측 여부를 독립 검증합니다. 시작부터 완료까지 도구가 직접 수행합니다.

```bash
# 기본 (ACTIVITY_NORMAL, 2026-09-16)
python -m tools.verify_activity_normal_mqtt

# 시나리오·기준일 지정
python -m tools.verify_activity_normal_mqtt \
  --scenario BASELINE_MICROWAVE_20D \
  --reference-date 2026-09-16
```

주요 옵션: `--broker-host/-port/-user`, `--tls-enabled`, `--ca-file`, `--api-url`, `--idle-timeout`(기본 30초), `--overall-timeout`(기본 600초).

> 이 도구는 **BURST로 하드코딩**돼 있습니다. MQTT 계약 검증에는 문제없지만, Kafka까지 적재해야 한다면 도구 대신 `POST /api/e2e/runs`나 웹 패널로 실행하세요. 단일 일자는 BURST로도 무손실이 확인됐고, 28일은 `{"mode": "ACCELERATED"}`(speed 생략 = 자동 안전 배속)를 쓰세요.
>
> 다일 시나리오의 `--overall-timeout`은 슬롯 수에 맞춰 **자동 상향**되므로 직접 올릴 필요가 없습니다(상향 시 안내 메시지가 출력됩니다).

---

## 12. 알려진 제약

**`ROUTINE_CHANGED_*`는 아직 E2E로 확인할 수 없습니다.**
AI에 특정 날짜를 수동 마감하는 HTTP API가 없습니다. 시뮬레이터는 일자별 발행과 완료 보고까지 제공하므로, AI 측 수동 마감 기능이 붙어야 검증이 완성됩니다.

**결정적 데이터 품질 시나리오가 없습니다.**
`SENSOR_FAULT`는 레거시 `/api/start` 경로에만 있고 `reference_date`·일자별 완료 보고·발행 수 계약이 없습니다. E2E 카탈로그의 품질 계열은 `ACTIVITY_INSUFFICIENT` 하나뿐입니다.

**시뮬레이터 초기화 범위.**
런마다 가전 상태·RNG·카운터가 완전히 새로 생성되므로 이전 실행이 남지 않습니다. 단, **AI 서비스의 슬라이딩 버퍼·analysis DB·Kafka·HDFS는 초기화 대상이 아닙니다.** 같은 날짜를 다시 태우면 다운스트림에 중복이 쌓입니다.

**웹 패널에서는 자동 배속을 쓸 수 없습니다.**
제어 패널이 ACCELERATED 선택 시 배속 입력을 필수로 검증합니다. 자동 산출은 `POST /api/e2e/runs`에서 `speed`를 생략했을 때만 동작합니다.

**동시 실행 불가.**
E2E 세션은 한 번에 하나입니다. 이전 `run_id`로 조회하면 `RUN_NOT_FOUND`가 나므로, 결과는 실행 중에 받아 두세요.

---

## 13. 에러 코드

| 코드 | HTTP | 의미 |
|---|---:|---|
| `BAD_REQUEST` | 400 | 필드 누락·형식 오류·허용되지 않은 필드 |
| `RUN_NOT_FOUND` | 404 | 종료됐거나 없는 `run_id` |
| `HOUSEHOLD_NOT_FOUND` | 404 | 세션에 없는 가구 |
| `E2E_SIMULATOR_RUNNING` | 409 | 이미 활성 E2E 세션 존재 |
| `LEGACY_SIMULATOR_RUNNING` | 409 | 레거시 시뮬레이터 실행 중 |
| `TASK_ALREADY_TERMINAL` | 409 | 이미 종료된 가구에 제어 시도 |
| `E2E_MANAGER_UNAVAILABLE` | 503 | 매니저 미주입 또는 종료됨 |
| `E2E_START_TIMEOUT` / `E2E_OPERATION_TIMEOUT` / `E2E_SNAPSHOT_TIMEOUT` | 503 | 이벤트 루프 응답 지연 |
