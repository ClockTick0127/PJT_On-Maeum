# 2026-09-14 진행 상황 공유

> **작성자**: 최준혁 (Data / Pipeline)<br>
> **주요 내용**: NILM MQTT 전력 시뮬레이터 다중 가구 동시 실행 및 가구별 독립 시나리오 확장, 실제 Pause/Resume 및 타임스탬프 연속성 보장, 원자적 상태 스냅샷 및 Reset 실패 계약 정립, 프런트엔드 실제 JS 테스트 및 통합 테스트 스위트 구축

---

## 1. 금일 작업 요약

기존 단일 가구(`H001`) 중심의 시뮬레이터와 웹 대시보드를 **“다중 가구 동시 실행 + 가구별 독립 시나리오 지정”** 구조로 전면 확장하고, 코드 리뷰에서 발견된 동시성 결함, 타임스탬프 단절, 상태 스냅샷 불일치, reset 실패 계약 문제를 체계적으로 해결했다.

주요 작업은 다음과 같다.

- **다중 가구 단일 워커 아키텍처 구현**: 가구마다 별도 스레드를 생성하지 않고 단일 워커 루프가 1초마다 공통 tick을 진행하여 다중 가구의 전력을 계산하고 MQTT로 동시 발행
- **가구별 독립 시나리오 및 라이프사이클 분리**: 각 가구(`H001`~`H010`)에 대해 독립적인 시나리오(`peak`, `routine_missed`, `random`, `manual`) 지정 지원 및 가구별 완주(`completed`) 상태 독립 관리
- **실제 Pause / Resume 구현**: 기존 stop/start 방식 대신 Manager 락 기반의 실제 일시정지/재개 구현, pause 응답 후 MQTT/SSE 무발행 보장, `simulation_date` 없는 실행에서도 resume 후 직전 가상 시각 대비 정확히 `+1초` 타임스탬프 연속성 유지
- **원자적 상태 스냅샷 및 Batch Commit**: `Manager.get_status()`의 `tick_lock` 및 `copy.deepcopy` 격리를 통한 논리적 틱 정합성 확보, 워커 루프 내 단일 임계 구역 기반 batch commit 적용
- **Reset 실패 상태 계약 정립**: 워커 종료 대기 타임아웃 시 HTTP 503 `RESET_FAILED` 반환, reset 초기화 미수행 및 데이터 보존, 종료 요청 전달 명시, 웹 UI의 EventSource 및 차트 상태 보존 처리
- **프런트엔드 안정화 및 키 통일**: `apparentS` 키 완전 통일, 가구별 최고 소비전력 독립 집계, 관찰 가구 드롭다운 완주 가구 유지
- **하위 호환성 보장**: 기존 단일 가구 위치 인자 호출 `SimulatorManager.start("peak", "H001")` 및 단일 가구 API 100% 호환
- **테스트 스위트 구축**: 운영 HTML의 실제 JavaScript를 실행하는 Node.js 테스트 7종 및 Python 다중 가구 통합 테스트 33종 추가, 시뮬레이터 전체 89개 Python 회귀 테스트 100% 통과 검증

---

## 2. 배경 및 핵심 요구사항

실시간 전력 분석 서비스(`realtime-analysis-service`) 및 모니터링 시스템의 고도화에 따라, 여러 가구가 서로 다른 전력 소비 패턴(피크 시연, 루틴 미가동 이상, 무작위 소비, 수동 제어)을 동시에 시뮬레이션하고 이를 파이프라인으로 전송해야 하는 요구가 발생했다.

### 주요 요구 조건
1. **다중 가구 동시 실행**: `H001`부터 `H010`까지 복수의 가구를 한 번에 실행 가능해야 함
2. **가구별 독립 시나리오**: 가구마다 서로 다른 시나리오를 지정할 수 있어야 함
3. **공통 가상 시계**: 모든 가구는 동일한 기준 날짜(`simulation_date`)와 공통 tick의 동일한 가상 타임스탬프(`measured_at`, `ts`, `now_iso`)를 공유해야 함
4. **독립 상태 관리**: 가구별 전력값, 가전 상태, 누적 사이클, 최고 전력, 완주 상태는 완전히 독립적이어야 함
5. **리소스 효율성**: 가구 수만큼 스레드를 띄우지 않고 단일 워커 루프에서 공통 tick으로 일괄 처리해야 함
6. **하위 호환성**: 기존 CLI 및 단일 가구 기반 테스트가 수정 없이 동작해야 함

---

## 3. 다중 가구 시뮬레이터 아키텍처 및 상세 구현

```text
[Web Dashboard / REST API]
       │  POST /api/start (households: [...], simulation_date: "...")
       ▼
[SimulatorManager (server/manager.py)]
       │  - 단일 워커 스레드 관리 (worker_thread)
       │  - 락 계층: lifecycle_lock -> tick_lock -> lock
       │  - active_households, last_metrics_by_house 관리
       ▼
[공통 Tick 루프 (_worker_loop)]  <-- 1초 주기
       ├─ 1. 가상 시각(sim_dt) 계산 (internal_base_dt + cycle - 1)
       ├─ 2. 가구별 독립 전력 계산 (calculate_main_panel_metrics)
       ├─ 3. 가구별 MQTT 비동기 발행 (v1/power/sim/{house}/main)
       ├─ 4. 가구별 완주 판정 (peak 60틱, routine_missed 300틱)
       ├─ 5. 단일 lock 안에서 batch commit (cycles, statuses, metrics)
       └─ 6. 웹 대시보드 브로드캐스트 (SSE 발행)
```

### (1) 단일 워커 기반 공통 Tick 및 가상 시각 동기화
* **단일 워커 루프**: 가구마다 별도 스레드를 생성할 경우 발생할 수 있는 스레드 경합과 타이밍 어긋남을 원천 차단하기 위해, 하나의 백그라운드 워커 스레드가 모든 가구의 tick을 일괄 순회하도록 구현했습니다.
* **가상 시각 결정 우선순위 (규칙 E)**:
  * 실행 대상 가구 중 하나라도 `routine_missed`가 포함되어 있으면 아침 루틴 기준 시각인 `08:10:01 KST`를 공통 시작 가상 시각으로 결정합니다.
  * 모든 가구가 동일한 밀리초 단위 UTC ISO 8601 타임스탬프를 공유하므로 브로커와 분석 서비스에서 완벽한 시간 동기화가 보장됩니다.

### (2) 실제 Pause / Resume 구현 및 타임스탬프 연속성
* **플래그 및 락 기반 일시정지**: 과거의 stop/start 방식 대신 `is_paused` 플래그를 도입하고, 워커 루프가 tick 진입 전 및 완료 후 `is_paused`를 확인하여 대기하도록 구현했습니다.
* **무발행 보장**: pause 응답이 반환된 이후에는 MQTT 발행과 SSE 전송, cycle 증가, 가상 시각 진행이 일절 발생하지 않습니다.
* **타임스탬프 연속성**:
  * `internal_base_dt`는 워커 시작 즉시가 아니라, **MQTT 브로커 연결이 완료되고 첫 계측 tick을 생성하는 시점에 1회만 확정**합니다.
  * 매 tick은 `sim_dt = internal_base_dt + timedelta(seconds=cycle - 1)`로 계산되므로, pause 동안 cycle이 멈춰 있다가 resume되면 직전 timestamp 대비 정확히 `+1.0초`로 연속됩니다.
* **Pause 상태에서의 안전한 Stop/Reset**: pause 대기 중인 워커도 `stop_event.set()` 수신 시 즉각 탈출할 수 있도록 타임아웃 루프를 적용했습니다.

### (3) 원자적 상태 스냅샷 및 Batch Commit (`GET /api/status`)
* **락 획득 순서 통일**: 데드락을 방지하고 틱 계산 도중의 불완전한 상태 조회를 차단하기 위해 `with self.tick_lock: with self.lock:` 순서로 락을 획득하도록 구현했습니다.
* **격리 스냅샷 반환**: 외부에서 status 딕셔너리를 수정하더라도 내부 상태가 오염되지 않도록 `copy.deepcopy`를 적용하여 반환합니다.
* **Batch Commit**: 워커 루프 내에서 global cycle과 가구별 cycle, status, 최신 메트릭을 단일 `with self.lock:` 구역 안에서 한 번에 갱신하여, 동시 조회 시 가구 간 cycle이 어긋나는 레이스 컨디션을 제거했습니다.

### (4) Reset 실패 상태 계약 정립 및 웹 UI 방어
* **계약 내용**:
  * 워커 종료 대기(join 타임아웃 10초) 실패 시, 내부 데이터 필드(`active_households`, `metrics`, 날짜, `cycle`)의 reset 초기화는 수행되지 않고 보존됩니다.
  * 워커에 이미 전달된 종료 요청(`stop_event.set()`)은 취소할 수 없으므로 `is_paused`는 `False`로 전이될 수 있으며 워커는 이후 종료될 수 있습니다.
  * 서버는 HTTP 503 `RESET_FAILED`를 반환하며, 클라이언트는 이후 `GET /api/status`로 최종 상태를 확인해야 합니다.
* **웹 UI 방어**: `waveform_viewer.html`의 `resetSimulation()`은 `/api/reset` 실패 시 SSE 연결을 닫지 않고 기존 차트 및 히스토리를 유지하며, 새로운 시뮬레이션 시작(`startMultiSimulation`)을 중단합니다.

### (5) 프런트엔드 개선 및 `apparentS` 키 통일
* **피상전력 키 통일**: 레거시 `apparentP` 키를 완전히 제거하고 `apparentS`로 통일하여 첫 SSE 수신 렌더링, 가구 전환 시 차트 복원, CSV 다운로드가 오작동 없이 수행되도록 수정했습니다.
* **최고 전력 독립 관리**: 가구별 최고 전력(`maxPowerByHouse`)을 분리하여 다른 가구의 피크가 현재 관찰 가구의 지표를 왜곡하지 않도록 개선했습니다.

---

## 4. MQTT ➡️ Kafka 파이프라인 연동 규격

시뮬레이터가 발행하는 데이터는 `infrastructure/mqtt-kafka-bridge/bridge.py`의 필수 검증 조건을 완벽히 충족하며 Kafka로 전달됩니다.

```text
[Simulator publisher.py]
  │  Topic: v1/power/sim/{house}/main (QoS 1)
  ▼
[Mosquitto MQTT Broker (8883/TLS 또는 1883)]
  │
  ▼
[MQTT-Kafka Bridge (bridge.py)]
  │  필수 검증: (household_id or house) AND (active_power or power_w) AND (measured_at or ts)
  │  Kafka Produce: Key = household_id, Topic = power.raw.v1
  ▼
[Apache Kafka (power.raw.v1)]
  │
  ▼
[Realtime Analysis Service (AI 모델 추론 및 이상 감지)]
```

### 발행 페이로드 스키마 예시
```json
{
  "message_id": "4b6f12d8-21d9-4b13-a7c8-9d8e7f6a5b4c",
  "household_id": "H001",
  "device_id": "main",
  "measured_at": "2026-09-10T05:30:00.000Z",
  "active_power": 1250.4,
  "reactive_power": 320.1,
  "power_factor": 0.968,
  "current": 5.72,
  "house": "H001",
  "device": "main",
  "ts": "2026-09-10T05:30:00.000Z",
  "power_w": 1250.4,
  "voltage": 220.0,
  "apparent_power": 1290.8
}
```

---

## 5. 테스트 및 검증 결과

운영 환경 코드와의 일치성을 검증하기 위해 운영 HTML의 JavaScript를 직접 실행하는 Node.js 테스트와 고도화된 Python 통합 테스트를 구축했습니다.

### (1) Node.js 실제 JS 런타임 테스트 (`test_ui_real_js.js`)
운영 `waveform_viewer.html`의 스크립트 블록을 Node.js `vm` 컨텍스트에 로드하여 실제 브라우저 환경 동작을 검증했습니다. (7/7 통과)

- **Test 1**: 첫 SSE 수신 및 `apparentS` 렌더링 정상 동작 검증
- **Test 2**: 가구별 최고 전력(`maxPowerByHouse`) 독립 관리 검증
- **Test 3**: 가구 전환 시 차트 데이터 및 상태 분리 검증
- **Test 4**: `getCsvContentForHouse` CSV 생성 및 컬럼 헤더 정합성 검증
- **Test 5**: `togglePause` pause/resume 엔드포인트 호출 및 상태 보존 검증
- **Test 6**: `apparentS` 완전 통일 및 로컬 `tick()` 정상 실행 검증
- **Test 7**: `resetSimulation` HTTP 500 실패 시 EventSource 유지, UI 보존 및 start 중단 검증

```text
node infrastructure/mqtt/simulator/tests/test_ui_real_js.js
✔ 운영 JavaScript 코드 vm 로드 및 초기화 성공
[Test 1] 첫 SSE 이벤트 처리 및 apparentS 키 정합성 검증 -> 통과
[Test 2] 가구별 최고 소비전력 독립 관리 검증 -> 통과
[Test 3] 가구 전환 시 차트 데이터 및 상태 독립성 검증 -> 통과
[Test 4] getCsvContentForHouse CSV 생성 및 apparent_power 검증 -> 통과
[Test 5] togglePause 일시정지/재개 API 호출 및 상태 보존 검증 -> 통과
[Test 6] apparentS 키 완전 통일 및 tick() 실행 검증 -> 통과
[Test 7] resetSimulation 실패 처리 및 EventSource 유지 검증 -> 통과
모든 프런트엔드 실제 JavaScript 테스트 통과!
```

### (2) Python 다중 가구 통합 테스트 (`test_multi_house_scenarios.py`)
다중 가구 라이프사이클과 동시성 계약을 실측 검증하는 33개 테스트를 수행했습니다. (33/33 통과)

- 다중 가구 동시 실행 및 단일 워커 공통 tick 검증
- `peak`(60틱) 및 `routine_missed`(300틱) 완주 후 타 가구 지속 발행 검증
- pause 시 MQTT/SSE 무발행 및 resume 후 +1초 타임스탬프 연속성 검증
- 실제 `_stop_and_join()` 실패 경로(`FakeHungWorker`)에서 `stop_event.set()` 전달, `is_paused` False 전이, 데이터 보존 및 HTTP 503 `RESET_FAILED` 검증
- `get_status`의 `copy.deepcopy` 격리 및 50회 연속 조회 시 원자적 cycle 정합성 검증

```text
python -m unittest infrastructure/mqtt/simulator/tests/test_multi_house_scenarios.py -v
Ran 33 tests in 18.023s
OK
```

### (3) 시뮬레이터 전체 Python 회귀 테스트 스위트
기존에 구축된 전력 모델, 시뮬레이션 날짜, 호환성 Facade를 포함한 전체 테스트 스위트를 실행했습니다. (89/89 통과)

```text
python -m unittest discover -s infrastructure/mqtt/simulator/tests -p "test_*.py" -v
Ran 89 tests in 105.437s
OK
```

### (4) Git 포맷 및 구문 검사
```text
git diff --check
(반환 코드 0, 공백 오류 및 충돌 마커 없음)
```

---

## 6. 변경 파일 목록

| 파일 경로 | 구분 | 주요 내용 |
| :--- | :---: | :--- |
| `infrastructure/mqtt/simulator/server/manager.py` | 수정 | 다중 가구 수명주기 관리, 단일 워커 루프, pause/resume, 원자적 get_status 스냅샷, batch commit, reset 503 처리 |
| `infrastructure/mqtt/simulator/server/request_handler.py` | 수정 | 다중 가구 API(`POST /api/start`) 처리, get_status 스냅샷 직결 반환, reset 503 에러 응답 매핑 |
| `infrastructure/mqtt/simulator/waveform_viewer.html` | 수정 | 다중 가구 설정 UI, apparentS 키 통일, reset 실패 시 EventSource/UI 보존 |
| `infrastructure/mqtt/simulator/scenarios.py` | 수정 | 다중 가구 가상 시작 시각 공통 결정 함수(`resolve_multi_simulation_start_time`) 추가 |
| `infrastructure/mqtt/simulator/README.md` | 수정 | 다중 가구 API 스펙, pause/resume/reset 상태 계약 및 가상 시각 규칙 문서화 |
| `infrastructure/mqtt/simulator/tests/test_simulation_date.py` | 수정 | stop 후 날짜 보존 및 reset 시 초기화 검증 단언 갱신 |
| `infrastructure/mqtt/simulator/tests/test_multi_house_scenarios.py` | 신규 | 다중 가구 동시 실행, pause/resume, reset 실패 경로, get_status 원자성 통합 테스트 (33개) |
| `infrastructure/mqtt/simulator/tests/test_ui_real_js.js` | 신규 | 운영 HTML 실제 JS 엔진 런타임 테스트 스위트 (7개) |

---

## 7. 향후 계획

- 운영 환경 배포 후 다중 가구 동시 시뮬레이션에 따른 실시간 분석 서비스(`realtime-analysis-service`) 파이프라인의 이벤트 감지 레이턴시 실측
- 대규모 가구 동시 실행 시 MQTT 브로커 및 Kafka 파티션 부하 모니터링
