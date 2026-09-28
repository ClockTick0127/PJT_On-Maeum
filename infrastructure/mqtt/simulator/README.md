# NILM IoT 스마트홈 전력 시뮬레이터 (Simulator)

스마트홈 메인 분전반(Smart Meter)의 초 단위 전력 계측 환경을 모사하여 Mosquitto MQTT 브로커로 실시간 스트리밍하는 고성능 시뮬레이션 패키지입니다.

> **AI 패턴 감지 검증용 결정적 시나리오 10종을 실행하려면 [E2E_GUIDE.md](E2E_GUIDE.md)를 보세요.** 시나리오 목록, `POST /api/e2e/runs` 규격, 실행 모드 선택 기준, 완료 판정, 알려진 제약을 정리한 운영 문서입니다. 아래 본문은 물리 엔진과 레거시 시연 모드의 상세 설명입니다.

---

## 1. 개요 및 목적

* **실시간 AI 추론 데이터 공급**: 비침습 가전 분리(NILM) 딥러닝 모델(TCN / Seq2Point)의 299초 슬라이딩 윈도우 추론에 필요한 다변량 교류 전력 데이터(유효전력, 무효전력, 역률, 전류, 전압)를 매초 실시간으로 생성하여 공급합니다.
* **대규모 인프라 스트레스 테스트**: 최대 1,000가구 동시 발행(1,000 msg/s)을 지원하여 MQTT 브로커, 카프카 브릿지, Flink 스트림 파이프라인의 처리 한계를 검증합니다.
* **발표용 원클릭 정상/이상/고장 시연(Demo) 지원**: 대시보드 발표를 위해 H001 가구를 대상으로 단 한 번의 클릭으로 초기화부터 MQTT 데이터 발행까지 자동 수행하는 3대 원클릭 시연 모드를 지원합니다:
  * **`[▶ H001 정상 일상]` (`normal_routine`)**: 08:04:58 KST 시작, 08:09:00 정각에 전자레인지가 가동되어 정확히 60초간(60개 활성 샘플) 동작 후 08:10:00에 복귀하는 정상 일상 전력 패턴을 308사이클 동안 결정론적으로 발행.
  * **`[▶ H001 이상 감지]` (`routine_missed`)**: 08:10:01 KST(CLI: 08:15:00 KST) 시작, 08:10 이후 전자레인지 미사용 대기전력 상태를 유지하며 299초 분석 윈도우 충족 후 300사이클 완주하는 루틴 누락 패턴 발행.
  * **`[▶ H001 센서 고장]` (`sensor_fault`)**: 140초 시나리오. cycle 1~10 정상 대기전력 발행 ➡️ cycle 11~130(정확히 120초간) 계측 MQTT 메시지 0건 발행(완전 결측, 0W 허위 데이터 배제) ➡️ cycle 131~140 정상 복구 후 10회 발행(121초 시간 간격 복구) ➡️ 140초 완료.
* **시뮬레이터 초기화 범위 및 AI 서비스 경계**:
  * 시뮬레이터의 초기화(Reset)는 **시뮬레이터 내부 상태**(가전 FSM, 가상 타이머, 가구별 상태, 화면 버퍼, 발행 워커)만을 초기화합니다.
  * AI 서비스의 299개 슬라이딩 버퍼, 당일 활동 기록, analysis DB, Monitoring Incident 알림 및 Kafka/HDFS 데이터는 초기화 대상이 아닙니다.
  * 현재 AI 실시간 판정 모델과 실제 전력 데이터의 파이프라인 연결은 후속 통합 범위이며, 본 시뮬레이터는 향후 정상/이상 판정에 사용할 결정론적인 초 단위 MQTT 전력 데이터를 발행하는 것을 목적으로 합니다.

---

## 2. 파일 구성

| 파일명                      | 역할 및 설명                                                                                                     |
| :-------------------------- | :--------------------------------------------------------------------------------------------------------------- |
| **`simulator.py`**          | **전력 시뮬레이터 호환 Facade & CLI 진입점** (1,000가구 벤치마크, 10초 피크/루틴누락 시연 모드 내장)             |
| **`engine/`**               | **시뮬레이터 핵심 엔진 패키지** (`config`, `profiles`, `state`, `power_model`, `publisher` 책임 분리)            |
| **`web_server.py`**         | **인터랙티브 웹 서버 진입점** (CLI 인자 처리, 서버 초기화 및 실행)                                             |
| **`server/`**               | **웹 서버 모듈 패키지** (`config.py`, `manager.py`, `request_handler.py` 책임 분리)                              |
| **`waveform_viewer.html`**  | 브라우저 기반 실시간 인터랙티브 시각화 대시보드 (원클릭 피크 시연 버튼, 실시간 차트, 가전 상태 칩)               |
| **`assets/waveform.css`**   | 대시보드 공통 스타일                                                                                             |
| **`assets/*.js`**          | 상태·가구·가전·차트·물리 계산·실시간 스트림·제어·이벤트·E2E 기능별 브라우저 코드                                  |
| **`visualize_waveform.py`** | 파형 시뮬레이션 데이터 생성 및 CSV/HTML 리포트 정적 출력 도구                                                    |
| **`requirements.txt`**      | 시뮬레이터 실행에 필요한 최소 의존성 목록 (`aiomqtt`)                                                            |

---

## 3. 물리 엔진 및 전력 모델링

단순 난수가 아닌, 교류(AC) 전력 공학 공식과 AI Hub 실측 데이터셋(EDA 중앙값)을 기반으로 물리적 상호 연동 계산을 수행합니다.

### (1) 다변량 교류 전력 계산식
* **피상전력(Apparent Power)**: $S = \sqrt{P^2 + Q^2}$ (단위: VA)
* **역률(Power Factor)**: $PF = \frac{P}{S}$ (범위: 0.1 ~ 1.0)
* **무효전력(Reactive Power)**: $Q = P \times \frac{\sqrt{1 - PF^2}}{PF}$ (단위: var)
* **부하전류(Current)**: $I = \frac{S}{V}$ (단위: A)

### (2) 환경 노이즈 및 기저 부하
* **전압($V$)**: 220V 기준 1차 자기회귀 모델(AR-1)을 적용하여 212V ~ 228V 사이를 완만하게 변동
* **상시 기저부하**: 가구별 40~65W 상시 대기전력 (Random Walk 드리프트)
* **냉장고 컴프레서 주기**: 15~25분 가동(약 63W, PF 0.78), 20~35분 정지 주기를 갖는 자동 주기 가전 모사

### (3) 6대 타겟 수동 가전 상태 머신 (FSM)
* **가동 전력 결정 방식**: 세션이 켜질 때 가동 전력은 범위 내 난수가 아니라 가전별 **EDA 실측 중앙값(`median_w`)으로 고정**됩니다. 역률은 기존과 같이 범위 내에서 세션마다 뽑히고, 초 단위 가우시안 노이즈(±1~2W)와 기동 돌입 배수는 그대로 적용됩니다.
* **전기포트 (`kettle`)**: 순수 저항 히터, 돌입 없음, 단일 구형파 (가동 전력 1,657W 고정, 실측 범위 1,500 ~ 1,800W, PF 0.98~1.00)
* **인덕션 (`induction`)**: 인버터 유도 가열, 18초 가열 ↔ 10초 휴지 서모스탯 듀티 사이클 반복 (가동 전력 1,463W 고정, 실측 범위 1,300 ~ 1,750W, PF 0.91~0.95)
* **전기다리미 (`iron`)**: 전열선 바이메탈 온도 제어 듀티 사이클 (가동 전력 1,389W 고정, 실측 범위 1,200 ~ 1,550W, PF 0.98~1.00)
* **전자레인지 (`microwave`)**: 마그네트론/변압기 자화 돌입전류 1.35배 기동 피크 (가동 전력 941W 고정, 실측 범위 850 ~ 1,150W, PF 0.88~0.94)
* **헤어드라이기 (`hair_dryer`)**: 열선 및 소형 팬모터 기동 피크 1.20배 (가동 전력 934W 고정, 실측 범위 800 ~ 1,200W, PF 0.94~0.98)
* **진공청소기 (`vacuum_cleaner`)**: 고속 직권 모터 강한 기동 돌입 1.50배 및 강한 지상 무효전력 (가동 전력 819W 고정, 실측 범위 700 ~ 950W, PF 0.75~0.85)

> **듀티 사이클 적용 범위**: 위 인덕션·전기다리미의 가열↔휴지 듀티 사이클은 **확률 기반 레거시 경로(`/api/start`, `engine/power_model.py`)에만** 적용됩니다.
> 결정적 E2E 경로(`POST /api/e2e/runs`, `engine/schedule_runtime.py`)에서는 시나리오가 선언한 ON 구간이 곧 "사람이 가전을 사용한 구간"이므로 내부 휴지 없이 **연속 가동**합니다. 휴지를 넣으면 AI가 한 번의 사용을 수십 개 세션으로 쪼개고 사용시간 합계가 선언값보다 짧아지기 때문입니다. 자세한 내용은 [E2E_GUIDE.md](E2E_GUIDE.md) 10절을 보세요.

### (4) H001 정상 일상 (`normal_routine`) 결정론적 타임라인
발표 시연용 `normal_routine` 시나리오는 H001 가구를 대상으로 아침 08:10 이전의 정상 일상 전력 패턴을 1초 단위로 정밀 생성합니다:
* **대상 가구**: `H001` 전용 (`normal_routine` 시나리오의 할당 대상은 H001로 제한됩니다. CLI `normal_routine` 실행은 H001 단일 가구로 동작합니다. 웹 API와 고급 UI에서는 H001 `normal_routine`을 다른 가구의 `peak`/`random`/`manual` 시나리오와 함께 실행할 수 있지만, 다른 가구에 `normal_routine`을 할당할 수 없으며 `routine_missed`와 함께 실행할 수 없습니다. H001의 랜덤 가전 이벤트는 비활성화되고 전자레인지 외 수동 가전은 OFF로 유지됩니다.)
* **기본 시작 시각**: `08:04:58 KST`
* **총 사이클**: 308초 (308개 초 단위 계측 데이터)
* **정확한 가상 시간표**:
  * `cycle 1` (08:04:58 KST): 아침 정상 루틴 시뮬레이션 시작 (상시 대기전력 및 냉장고 유지)
  * `cycle 242` (08:08:59 KST): 전자레인지 가동 직전 대기전력 유지 마지막 샘플
  * `cycle 243` (08:09:00 KST): **전자레인지 ON** (상태: `STARTING`, `nominal_w: 940.0W`, `pf: 0.91`, `inrush_remaining: 2`, `session_remaining: 61`, `manual_hold: False`)
  * `cycle 302` (08:09:59 KST): **전자레인지 ON 마지막 샘플** (cycle 243~302까지 **정확히 60개 활성 샘플** 생성)
  * `cycle 303` (08:10:00 KST): **전자레인지 명시적 OFF** (`state: OFF`, `session_remaining: 0`) ➡️ 평상시 대기전력 복귀
  * `cycle 308` (08:10:05 KST): 전자레인지 종료 후 5초간 대기전력 발행 후 **시나리오 자동 완료**

### (5) 센서 고장 (`sensor_fault`) 가변 결측 블랙아웃 타임라인
센서 고장 재현을 위해 선택된 가구(H001~H010)의 전력 계측 MQTT 메시지가 **정확히 $D$초(기본 120초, 1~3600초 가변) 동안 완전히 중단**되는 상황을 모사합니다:
* **대상 가구**: `H001`~`H010` 모든 가구 지원 (단일 실행 및 다중 가구 동시 실행 지원)
* **결측 시간 ($D = \text{fault\_duration\_sec}$)**: 기본 120초이며, 1~3600초 사이의 정수로 사용자 지정 가능합니다. 이 값은 실제 대기 시간이 아니라 시뮬레이터의 가상 시각(Virtual Time) 기준입니다.
* **0W 발행 금지 원칙**: 0W는 기기가 연결된 상태의 정상 계측값이므로 센서 고장을 나타낼 수 없습니다. 따라서 고장 구간에는 0W를 포함하여 어떠한 MQTT 메시지도 발행하지 않습니다 (0건 발행).
* **물리 계산값 은닉**: 내부 물리 시뮬레이션 상태 머신은 매초 틱을 진행하되, 계산된 전력값을 실제 측정값처럼 MQTT, UI 수치 카드, CSV에 노출하지 않습니다 (`[센서 고장 / 측정 없음]` 표기).
* **총 사이클**: $D + 20$초 (고장 전 정상 10초 + 고장 $D$초 + 복구 후 정상 10초)
* **총 MQTT 발행 건수**: 결측 시간 $D$와 무관하게 **항상 정확히 20건** (고장 전 10건 + 복구 후 10건)
* **정확한 타임라인**:
  * `cycle 1~10`: 평상시 정상 대기전력 계측 및 MQTT 발행 (10회 발행)
  * `cycle 11`: **센서 고장 시작** 이벤트 발생 ➡️ MQTT 발행 즉시 중단
  * `cycle 11 ~ (10+D)`: **센서 고장 블랙아웃** (정확히 $D$초간 MQTT 메시지 0건 발행). 내부 시뮬레이션 가상 시각은 1초씩 계속 전진.
  * `cycle (11+D)`: **센서 복구** 이벤트 발생 ➡️ 정상 계측 및 MQTT 발행 재개 (`cycle 10`과 `cycle (11+D)`의 `measured_at` 시각 차이는 정확히 $D + 1$초).
  * `cycle (11+D) ~ (20+D)`: 복구 후 정상 대기전력 계측 및 MQTT 발행 (10회 발행)
  * `cycle (20+D)`: **시나리오 완료** 이벤트 발생 ➡️ 총 20건 발행 후 자동 완료 (`status: "completed"`)
* **차트 및 CSV 표현**:
  * **차트**: Chart.js `spanGaps: false` 및 고장 구간 `null` 데이터 버퍼링을 통해 cycle 10과 복구 시점 사이가 이어지지 않고 시각적으로 완전히 끊겨 보입니다.
  * **CSV**: 결측 구간의 빈 행이나 허위 0W를 기록하지 않고, 실제 유효 계측치 20개 행만 추출되어 데이터 무결성을 보장합니다.

---

## 4. 발행 토픽 및 전송 페이로드 규격

* **MQTT 발행 토픽**: `v1/power/sim/{household_id}/main` (기본 QoS 1)
* **전송 페이로드 (JSON)**:
  신규 실시간 분석 서비스(BE-2) 규격과 기존 HDFS Parquet 로더 규격을 모두 포함하는 이중 호환 스키마입니다.

```json
{
  "message_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
  "household_id": "H001",
  "device_id": "main",
  "measured_at": "2026-09-08T07:42:47.034Z",
  "active_power": 3590.2,
  "reactive_power": 412.5,
  "power_factor": 0.993,
  "current": 16.32,

  "house": "H001",
  "device": "main",
  "ts": "2026-09-08T07:42:47.034Z",
  "power_w": 3590.2,
  "voltage": 220.1,
  "apparent_power": 3613.8
}
```

---

## 5. 실행 방법 (`simulator.py`)

### (1) 사전 준비

* **Python 버전**: **Python 3.10 이상** 필수 (비동기 이벤트 루프 및 SSLContext 검증 지원)
* **패키지 의존성 설치**:

```bash
pip install -r requirements.txt
```

*`requirements.txt`는 비동기 MQTT 클라이언트인 `aiomqtt>=2.0.0`을 포함합니다.*

### (2) 커맨드라인 옵션 상세

```bash
python simulator.py --help
```

| 옵션 | 단축키 | 기본값 | 설명 |
| :--- | :--- | :--- | :--- |
| `--scenario` | `-s` | `random` | 실행 시나리오 모드 (`random`: 연속 확률, `peak`: 10초 피크, `routine_missed`: 08:10 루틴 누락 이상치, `normal_routine`: H001 정상 일상 루틴, `sensor_fault`: 가변 센서 고장 결측) |
| `--date` | `-d` | `None` | 가상 기준 날짜 (`YYYY-MM-DD`). 미지정 시 오늘 날짜 사용 |
| `--houses` | `-n` | `10` | 대상 가구 수 (`H001` ~ `H{n:03d}`) |
| `--seed` | | `None` | 난수 seed (0~2147483647). 같은 seed로 다시 실행하면 계측값이 그대로 재현됨. 미지정 시 자동 생성 후 시작 로그에 출력 |
| `--fault-duration-sec` | | `120` | `sensor_fault` 결측 지속 시간 (초 단위, 1~3600 범위 정수, 기본값: 120) |
| `--interval` | `-i` | `1.0` | 데이터 발행 주기 (초 단위) |
| `--hz` | | `None` | 가구당 초당 측정 횟수 (지정 시 `interval = 1/hz` 자동 환산) |
| `--count` | `-c` | `0` | 전송 사이클 수 (`0`: 무한/시나리오 자동완료, `N > 0`: N회 전송 종료. peak: 60, routine_missed: 300, normal_routine: 308, sensor_fault: `0` 또는 $D+20$만 허용하며 그 외의 값은 연결 전 충돌 에러로 차단) |
| `--start-time` | | `None` | 시작 가상 시각 (예: `08:04:58`). CLI 기본값: normal_routine: 오늘 08:04:58 KST, routine_missed: 오늘 08:15:00 KST (웹: 08:10:01 KST) |
| `--host` | | `localhost` | MQTT 브로커 호스트 주소 (**TLS 사용 시 인증서 SAN과 반드시 일치해야 함. EC2-A 로컬 실행 시에도 localhost가 아닌 A 사설 IP 사용**) |
| `--port` | `-p` | `None` | MQTT 브로커 포트 번호 (미지정 시 `MQTT_PORT` 환경변수 또는 TLS 여부에 따라 `8883`/`1883` 자동 결정) |
| `--user` | `-u` | `simulator_user` | MQTT 인증 계정명 (환경변수: `MQTT_USER`) |
| `--password` | | `None` | MQTT 인증 비밀번호 (미지정 시 `MQTT_PASS` 환경변수 또는 `test1234`. **프로세스 노출 및 쉘 히스토리 방지를 위해 환경변수 또는 read 프롬프트 사용 권장**) |
| `--tls` / `--no-tls` | | `None` | MQTT TLS 암호화 연결 활성화 여부 (미지정 시 `MQTT_TLS_ENABLED` 환경변수 또는 `False`) |
| `--ca-file` | | `None` | 브로커 검증에 사용할 CA 인증서 파일 경로 (PEM 형식, 환경변수: `MQTT_CA_FILE`) |
| `--qos` | | `1` | MQTT QoS 레벨 (`0` 또는 `1`) |
| `--quiet` | `-q` | `False` | 요약 모드 (매초 상세 로그 생략, 5초 주기 누적 처리량 통계만 출력) |

> [!IMPORTANT]
> **포트 결정 우선순위**:
> `--port` CLI 명시 > `MQTT_PORT` 환경변수 > TLS 활성화 시 `8883` > 평문 연결 시 `1883`

> [!NOTE]
> **`normal_routine` CLI 가구 수 제한 동작**:
> `normal_routine`은 H001 전용 시나리오입니다.
> `normal_routine`에서는 `--houses 1`과 레거시 기본값 `10`을 `H001` 단일 실행으로 해석합니다.
> 그 외 2 이상의 가구 수는 `ValueError("normal_routine 시나리오는 H001 가구에서만 실행할 수 있습니다.")`로 거절합니다.
>
> | `--houses` 인자 | 처리 결과 및 동작 |
> | :--- | :--- |
> | `--houses` 생략 | 기본값 10을 레거시 기본값으로 해석하고 H001 단일 실행 |
> | `--houses 1` | H001 단일 실행 |
> | `--houses 2~9` | `ValueError` 발생 및 즉시 거절 |
> | `--houses 10` 명시 | 레거시 기본값과 동일하게 H001 단일 실행 |
> | `--houses 11` 이상 | `ValueError` 발생 및 즉시 거절 |

---

### (3) 환경별 실행 가이드

#### 1) 로컬 개발 환경 (평문 1883 연결)

로컬에 구동된 Mosquitto 브로커(`localhost:1883`)로 평문 통신합니다. 별도의 TLS 환경변수 없이 바로 실행할 수 있습니다.

```bash
# H001 정상 일상 308초 시연 (08:04:58 KST 시작, 08:09:00 전자레인지 60초 가동)
python simulator.py --scenario normal_routine

# 피크 60초 시연 모드 (10초 시점 3,000W+ 도달)
python simulator.py --scenario peak

# H001 센서 고장 140초 기본 시연 모드 (120초간 MQTT 무발행 결측 검증)
python simulator.py --scenario sensor_fault

# H001 센서 고장 30초 단축 결측 시연 (총 50사이클, 20건 발행)
python simulator.py --scenario sensor_fault --fault-duration-sec 30

# H001 센서 고장 180초 결측 시연 및 명시적 총 사이클 수 지정
python simulator.py --scenario sensor_fault --fault-duration-sec 180 --count 200

# 08:10 루틴 누락(ROUTINE_MISSED) 300초 이상치 검증 모드
python simulator.py --scenario routine_missed

# 1,000가구 대규모 스트리밍 부하 테스트 (초당 1,000건)
python simulator.py -n 1000 -q
```

---

#### 2) 운영 EC2 환경 (TLS 8883 보안 연결)

운영 Mosquitto 브로커(EC2-A)는 **8883 포트의 TLS 리스너만 사용**하며, 서버 인증서에는 **EC2-A의 사설 IP(`A_PRIVATE_IP`)가 SAN(Subject Alternative Name)**에 등록되어 있습니다.

> [!WARNING]
> * **인증서 SAN 불일치 방지**: EC2-A 머신 내부에서 시뮬레이터를 실행하더라도 `--host`나 `MQTT_HOST`에 `localhost`를 사용할 수 없습니다. 반드시 인증서 SAN에 등록된 사설 IP `<A_PRIVATE_IP>`를 지정해야 TLS 핸드셰이크 검증에 통과합니다.
> * **비밀번호 노출 및 쉘 히스토리 방지**:
>   * **CLI 인자 노출 위험**: `--password <PASS>`를 커맨드라인에 직접 입력하면 프로세스 목록(`ps aux`, `/proc/<pid>/cmdline`)을 통해 동일 머신의 다른 사용자에게 노출될 위험이 있으므로 CLI 인자 전달을 지양합니다.
>   * **쉘 히스토리 노출 주의**: 단순히 쉘에 `export MQTT_PASS=...`를 직접 타이핑하면 `~/.bash_history` 등에 평문으로 저장됩니다. 히스토리 저장을 방지하려면 아래 방법 중 하나를 사용하십시오:
>     * **방법 1 (`read -rsp`)**: 화면 에코 및 쉘 히스토리 없이 프롬프트로 비밀번호 입력
>       ```bash
>       read -rsp "MQTT Password: " MQTT_PASS && export MQTT_PASS
>       ```
>     * **방법 2 (권한 제한 환경파일 생성)**:
>       ```bash
>       umask 077
>       read -rsp "MQTT Password: " MQTT_PASS
>       printf '\n'
>       printf 'MQTT_PASS=%q\n' "$MQTT_PASS" > ~/.nilm_mqtt.env
>       unset MQTT_PASS
>       set -a && source ~/.nilm_mqtt.env && set +a
>       ```
> * **CA 파일 권한 및 예시 경로**:
>   * EC2-A 실행 시 예시: `~/mqtt-ca/ca.crt`
>   * EC2-B 실행 시 예시: `/opt/nilm/mqtt/certs/ca.crt` (서버 권한에 따라 `sudo` 또는 그룹 읽기 권한 필요)

##### 방법 A. 환경변수 기반 실행 (권장)

```bash
# 1. 운영 브로커 접속 환경변수 설정
export MQTT_HOST=<A_PRIVATE_IP>
export MQTT_PORT=8883
export MQTT_USER=<MQTT_USER>
read -rsp "MQTT Password: " MQTT_PASS && export MQTT_PASS
export MQTT_TLS_ENABLED=true
export MQTT_CA_FILE=<CA_FILE_PATH>

# 2. 피크 시연 모드 실행
python simulator.py --scenario peak

# 3. 루틴 누락 검증 모드 실행
python simulator.py --scenario routine_missed
```

##### 방법 B. CLI 옵션 명시 실행

```bash
# 비밀번호는 프롬프트나 환경변수로 안전하게 주입
read -rsp "MQTT Password: " MQTT_PASS && export MQTT_PASS

# CLI 플래그로 운영 TLS 접속 정보 전달
python simulator.py \
  --host <A_PRIVATE_IP> \
  --port 8883 \
  --user <MQTT_USER> \
  --tls \
  --ca-file <CA_FILE_PATH> \
  --scenario peak
```

---

## 6. 실시간 웹 시각화 대시보드 (`waveform_viewer.html` & `web_server.py`)

웹 브라우저에서 직접 버튼을 클릭하여 시뮬레이터를 제어하고, 실시간 전력 파형 시각화와 **실제 Mosquitto MQTT 발행(Kafka 연동)**을 동시에 수행할 수 있습니다.

웹 컨트롤러 HTTP 서버는 보안을 위해 **기본적으로 `127.0.0.1`에만 바인딩**됩니다.

화면 마크업은 `waveform_viewer.html`, 스타일은 `assets/waveform.css`, 동작은 `assets/`의 JavaScript 파일에 있습니다. JavaScript는 HTML에 적힌 순서대로 로드되며, `state.js`가 공통 상태를 초기화하고 `e2e.js`가 E2E 패널을 담당합니다. HTML 파일을 직접 열 때도 같은 디렉터리의 `assets/` 폴더가 필요합니다.

### 정상·이상 루틴 비교 시연 페이지

웹 서버 실행 후 `http://127.0.0.1:8085/routine-demo`에서 발표용 비교 화면을 열 수 있습니다.
동일한 08:08:30~08:10:30 시간축에 다음 두 파형을 함께 표시합니다.

* **평소의 아침**: 08:09~08:10 전자레인지 사용으로 약 1,000W까지 상승한 뒤 대기전력으로 복귀
* **오늘의 아침**: 원래 사용하던 시간에도 약 50~65W 대기전력만 유지되어 루틴 누락 후보로 전환

이 화면은 MQTT 연결 없이도 발표가 가능한 결정적 재생 화면이며, 이상 그래프에는 평소 기대 파형을 점선으로 겹쳐 실제 저전력과 "기대 행동의 부재"를 구분합니다. 실시간 시뮬레이터 상단의 `정상·이상 비교 시연` 버튼으로도 이동할 수 있습니다.

> [!CAUTION]
> * **0.0.0.0 바인딩 및 인증되지 않은 제어 API 노출 위험**:
>   웹 컨트롤러의 제어 API(`/api/start`, `/api/stop`, `/api/device` 등)는 별도의 사용자 인증이 없으므로, `--bind-host 0.0.0.0`으로 외부 네트워크에 개방할 경우 인가되지 않은 사용자가 전력 데이터 발행을 임의로 조작할 수 있습니다.
> * **권장 원격 접속 방식 (SSH 터널링)**:
>   웹 서버는 기본값(`127.0.0.1`)으로 안전하게 유지하고, 로컬 PC에서 SSH 포트 포워딩을 통해 암호화 터널로 접속하십시오:
>   ```bash
>   ssh -i <EC2_KEY.pem> -L 8085:127.0.0.1:8085 ubuntu@<EC2_PUBLIC_IP>
>   # 로컬 브라우저에서 http://localhost:8085 접속
>   ```
> * **부득이하게 `--bind-host 0.0.0.0`을 사용하는 경우**:
>   반드시 EC2 보안 그룹(Security Group)에서 웹 서버 포트(8085)의 인바운드 허용 소스 IP를 관리자 개인 공인 IP(`/32`)로 엄격히 제한하십시오.

### (1) 실행 방법

#### A. 로컬 평문 실행 (기본 localhost:1883)

```bash
python web_server.py
# 특정 웹 포트 지정: python web_server.py 8089
# 브라우저 접속: http://127.0.0.1:8085
```

#### B. 운영 EC2 TLS 실행 (8883 포트 보안 연결)
웹 시뮬레이터 역시 CLI와 동일한 공통 TLS 설정을 지원합니다. 웹 서버 기동 시점에 CA 파일 유효성을 사전 검증하며, 버튼 클릭(`/api/start`) 시에도 동기적으로 TLS 검증을 수행합니다.

```bash
# 환경변수 기반 운영 웹 서버 실행 (권장)
export MQTT_HOST=<A_PRIVATE_IP>
export MQTT_PORT=8883
export MQTT_USER=<MQTT_USER>
read -rsp "MQTT Password: " MQTT_PASS && export MQTT_PASS
export MQTT_TLS_ENABLED=true
export MQTT_CA_FILE=<CA_FILE_PATH>

python web_server.py

# 또는 CLI 인자로 지정하여 실행 (바인딩 호스트 및 웹 포트 지정 가능)
python web_server.py \
  --bind-host 127.0.0.1 \
  --web-port 8085 \
  --host <A_PRIVATE_IP> \
  --port 8883 \
  --user <MQTT_USER> \
  --tls \
  --ca-file <CA_FILE_PATH>
```

* **모드 B. 브라우저 단독 시각화 모드 (네트워크 미연동)**:
  파이썬/도커를 켤 필요 없이 파일 탐색기에서 `waveform_viewer.html`을 더블 클릭하여 로컬 브라우저에서 시각적 데모만 재생합니다.
  ```text
  file:///path/to/infrastructure/mqtt/simulator/waveform_viewer.html
  ```

### (2) 주요 대시보드 기능 및 발표용 원클릭 시연
* **`[▶ H001 정상 일상]` 원클릭 버튼 (초록색)**:
  * 클릭 한 번으로 이전 시뮬레이터를 초기화하고, `H001` 가구 상태를 재생성한 후 `normal_routine` 시나리오를 할당하여 실시간 MQTT 발행을 즉시 시작합니다.
  * 08:04:58 KST부터 대기전력을 발행하고, 08:09:00 정각에 전자레인지가 가동(940W)되어 정확히 60초간(60개 활성 샘플) 유지 후 08:10:00에 OFF 복귀, 08:10:05에 308사이클 완주로 자동 종료됩니다.
* **`[▶ H001 이상 감지]` 원클릭 버튼 (주황색)**:
  * 클릭 한 번으로 이전 시뮬레이터를 초기화하고, `H001` 가구 상태를 재생성한 후 `routine_missed` 시나리오를 할당하여 실시간 MQTT 발행을 즉시 시작합니다.
  * 08:10:01 KST부터 전자레인지 미가동 대기전력 상태를 유지하며 299초 분석 버퍼 충족("299개 분석 입력 데이터 충족 — AI 이상 감지 판정 대기") 후 300초에 "H001 루틴 누락 전력 패턴 발행 완료" 상태로 자동 종료됩니다.
* **`[▶ H001 센서 고장]` 원클릭 버튼 (보라색)**:
  * 클릭 한 번으로 이전 시뮬레이터를 초기화하고, `H001` 가구 상태를 재생성한 후 `sensor_fault` 시나리오를 할당하여 실시간 결측 시뮬레이션을 시작합니다.
  * cycle 1~10 대기전력 정상 발행 후, cycle 11~130 동안 120초간 MQTT 메시지를 한 건도 발행하지 않으며(0W 위조 없이 실제 무발행), cycle 131부터 정상 복구되어 140초 완주 후 자동 완료됩니다.
* **초기화 실패 안전장치**:
  * 원클릭 버튼 실행 시 내부 초기화(reset)가 HTTP 503이나 네트워크 오류로 실패하면 새 시나리오를 시작하지 않으며, 기존 상태와 SSE 스트림을 안전하게 보존하고 오류 안내 메시지를 표시합니다.
* **실시간 제어 바**: 일시정지, 재생, 리셋(발행 중지), 기준 날짜 선택, 관찰 가구 변경(`H001`~`H010` 전체 선택 가능하며, 실행 중인 가구는 `H001 (peak)`처럼 시나리오가 함께 표기됩니다), 재생 속도 조절(1x, 2x, 5x, 10x 배속)
* **실시간 가전 칩 패널**: 6대 가전의 실시간 ON/OFF 상태 및 소비전력 표시 (`normal_routine` 가동 중 전자레인지 칩 ON 활성화 및 종료 시 OFF 복귀)
* **CSV 내보내기**: 시뮬레이션된 시계열 데이터를 엑셀 호환 UTF-8 BOM CSV 파일로 즉시 저장
* **고급 설정 (접이식 UI)**: 다중 가구 동시 설정 테이블 및 기존 개발자 전용 프리셋(피크, 종합, 전체 랜덤, 수동 제어 등)은 `<details>` 영역으로 깔끔하게 접혀 있어 발표 시연에 집중할 수 있습니다.

### (2-1) Seed로 랜덤 실행 재현

실시간 경로(`/api/start`, `simulator.py`)는 가구마다 `random.Random(f"{seed}:{house}")`를 따로 만들어 초기 환경과 매 틱 난수를 뽑습니다.
랜덤 모드에서 흥미로운 파형이 나왔다면 그 실행의 seed로 다시 시작해 같은 값을 다시 볼 수 있습니다.

* **웹**: 제어 바의 `Seed` 입력칸을 비우고 시작하면 서버가 seed를 생성해 `현재 seed`에 표시합니다. 그 값을 입력칸에 넣고 같은 가구/시나리오로 다시 시작하면 재현됩니다.
* **API**: `POST /api/start`의 최상위 `seed`(0~2147483647 정수, 생략/null이면 자동 생성). 응답과 `GET /api/status`에 `seed`가 포함됩니다. 범위 밖이거나 정수가 아니면 400을 반환합니다.
* **CLI**: `python simulator.py --scenario random --seed 12345`
* **시작 시각 고정**: 웹의 `시작 시각` 입력칸 또는 `/api/start`의 `start_time`(`"HH:MM"`/`"HH:MM:SS"`, KST)을 지정하면 기준 날짜(`simulation_date`, 없으면 오늘)의 그 시각부터 시작합니다. CLI는 기존 `--start-time`을 사용합니다. `normal_routine`/`routine_missed`는 시작 시각이 시나리오에 고정되어 있어 지정 시 400을 반환합니다.

재현 조건과 한계:
* 같은 seed + 같은 가구 + 같은 시나리오(`sensor_fault`는 같은 `fault_duration_sec`)이면 틱별 P, Q, S, PF, V, I, 동작 가전이 동일합니다. 가구별로 난수가 독립이라 다른 가구를 함께 돌리는지와 무관합니다.
* 배속, 일시정지, 발행 주기는 값에 영향을 주지 않습니다 (틱 번호 기준으로 생성).
* 시작 시각을 고정하지 않으면 `measured_at`은 실제 시작 시각 기반이라 실행마다 달라집니다. seed + 기준 날짜 + 시작 시각을 모두 고정하면 `measured_at`까지 동일합니다.
* `message_id`는 실행마다 달라집니다.
* 실행 중 가전을 수동으로 ON/OFF하면 그 이후 값은 재현되지 않습니다.

### (3) 시뮬레이션 기준 날짜 선택 및 가상 시각 제어 (New)

#### 1) 웹 화면에서 기준 날짜 선택 방법
* **날짜 선택 입력창**: 브라우저 상단 제어 바의 `시뮬레이션 기준 날짜`(`<input type="date" id="simDateInput">`)에서 원하는 날짜를 선택합니다.
* **초기 기본값**: 브라우저가 실행 중인 로컬 머신 시각이 아닌, 한국 표준시(`Asia/Seoul`, KST) 기준 오늘 날짜(`YYYY-MM-DD`)가 자동으로 채워집니다.
* **조작 안전장치**: 시뮬레이션이 시작되면 실행 도중 날짜가 변경되어 시계열 일관성이 깨지는 것을 방지하기 위해 입력 필드가 자동으로 비활성화(disabled)되며, 리셋 또는 정지 시 다시 활성화됩니다.
* **유효성 검사**: 빈 값이나 잘못된 날짜가 선택된 상태에서 시작 버튼을 누르면 친절한 경고 알림(`alert`)이 발생하며 시뮬레이션이 시작되지 않습니다.

#### 2) `simulation_date` API 계약

##### A. 시뮬레이션 시작 (`POST /api/start`)
* **요청 헤더**: `Content-Type: application/json`

* **방법 1. 다중 가구 동시 실행 요청 (신규)**:
  ```json
  {
    "households": [
      { "house": "H001", "scenario": "normal_routine" },
      { "house": "H003", "scenario": "random" },
      { "house": "H004", "scenario": "manual" }
    ],
    "simulation_date": "2026-09-15"
  }
  ```
  * `households`: 1개 이상 10개 이하의 가구 설정 배열 (중복 가구 ID 불허).
  * 각 원소는 `house`(`H001`~`H010`)와 `scenario`(`normal_routine`, `peak`, `routine_missed`, `sensor_fault`, `random`, `manual`)를 포함해야 합니다.

* **방법 2. 단일 가구 하위 호환 요청 (기존 규격)**:
  ```json
  {
    "scenario": "sensor_fault",
    "house": "H001",
    "simulation_date": "2026-09-15"
  }
  ```
  * 기존 단일 가구 요청은 내부적으로 `[{"house": house, "scenario": scenario}]`로 정규화되어 동작하므로 100% 하위 호환됩니다.
  * 단일 지정 필드(`house`/`scenario`)와 다중 지정 필드(`households`)를 동시에 전달하면 충돌로 간주하여 HTTP `400 Bad Request`로 거절됩니다.

* **방법 3. 웹 API curl 실행 예시 (`sensor_fault` 및 복합 시나리오)**:
  ```bash
  # 1) H001 단일 가구 기본 120초 센서 결측 시연 시작
  curl -X POST http://127.0.0.1:8085/api/start \
    -H "Content-Type: application/json" \
    -d '{"scenario": "sensor_fault", "house": "H001"}'

  # 2) H001 단일 가구 30초 결측 시간 지정 시연 시작
  curl -X POST http://127.0.0.1:8085/api/start \
    -H "Content-Type: application/json" \
    -d '{"scenario": "sensor_fault", "house": "H001", "fault_duration_sec": 30}'

  # 3) 다중 가구 동시 실행 (H001 센서 결측 180초 + H002 피크 검증)
  curl -X POST http://127.0.0.1:8085/api/start \
    -H "Content-Type: application/json" \
    -d '{
      "households": [
        { "house": "H001", "scenario": "sensor_fault" },
        { "house": "H002", "scenario": "peak" }
      ],
      "fault_duration_sec": 180,
      "simulation_date": "2026-09-17"
    }'
  ```

* **다중 가구 공통 가상 시작 시각(`base_dt`) 결정 규칙 및 충돌 방어**:
  * **normal_routine의 H001 전용 제한 (HTTP 400 방어)**:
    `normal_routine` 시나리오는 `H001` 가구에서만 실행 가능합니다. 단일 가구 요청(`{"house": "H002", "scenario": "normal_routine"}`)이나 다중 가구 요청(`households` 배열 내 `{"house": "H002", "scenario": "normal_routine"}`) 모두 HTTP `400 Bad Request` (`normal_routine 시나리오는 H001 가구에서만 실행할 수 있습니다.`)로 거절됩니다.
    웹 UI의 고급 가구 설정 테이블에서도 `H001`의 드롭다운 목록에만 `normal_routine`이 제공되며, `H002`~`H010`에서는 선택할 수 없습니다.
  * **normal_routine과 routine_missed 동시 실행 불가 (HTTP 400 방어)**:
    `normal_routine`(기본 08:04:58 KST)과 `routine_missed`(기본 08:10:01 KST)는 시작 시각이 서로 상이하여 단일 타임라인을 공유할 수 없습니다. 따라서 `households` 목록에 두 시나리오가 동시에 포함되면 HTTP `400 Bad Request` (`normal_routine과 routine_missed는 동일한 다중 실행에서 함께 사용할 수 없습니다.`)로 즉시 거절됩니다.
  * **normal_routine만 포함된 경우**:
    모든 가구의 공통 `base_dt` = 선택한 `simulation_date`의 **08:04:58 KST** (날짜 미지정 시 오늘 날짜의 08:04:58 KST).
  * **routine_missed만 포함된 경우**:
    모든 가구의 공통 `base_dt` = 선택한 `simulation_date`의 **08:10:01 KST** (날짜 미지정 시 오늘 날짜의 08:10:01 KST).
  * **둘 다 포함되지 않은 경우**:
    모든 가구의 공통 `base_dt` = 선택한 `simulation_date` + **실행 시작 시점 현재 KST 시각** (날짜 미지정 시 현재 KST 시각).
  * 동일 tick에서 모든 가구의 MQTT `measured_at`, `ts` 및 SSE `now_iso`는 **완전히 동일한 타임스탬프를 공유**합니다.

* **입력 검증 규칙**:
  * `simulation_date` 필드는 선택(optional)입니다.
  * 전달된 경우 문자열이어야 하며, 엄격한 `YYYY-MM-DD` 형식 및 실제 존재하는 유효한 날짜(윤년 `2024-02-29` 허용, `2026-02-30` 또는 `2026-9-1` 불허)여야 합니다.
  * 유효하지 않은 값이 전달되면 HTTP `400 Bad Request` 에러 응답을 반환합니다.

* **성공 응답 (HTTP 200 OK)**:
  ```json
  {
    "status": "started",
    "scenario": "multi",
    "house": "H001",
    "households": [
      { "house": "H001", "scenario": "peak" },
      { "house": "H002", "scenario": "routine_missed" }
    ],
    "simulation_date": "2026-09-10",
    "resolved_start_time": "2026-09-09T23:10:01.000Z"
  }
  ```

##### B. 가전 수동 제어 (`PUT /api/device`)
* **요청 본문 (JSON)**:
  ```json
  {
    "house": "H004",
    "device": "kettle",
    "enabled": true
  }
  ```
  * `house`: 제어 대상 가구 ID (`H001`~`H010`)
  * `device`: 6대 타겟 가전 (`kettle`, `induction`, `iron`, `microwave`, `hair_dryer`, `vacuum_cleaner`)
  * `enabled`: `true` (ON/기동) 또는 `false` (OFF/정지)
  * **검증 및 동시성 제어**: HTTP 핸들러는 형식 유효성을 검증하고, `SimulatorManager.set_device()`가 라이프사이클 및 시뮬레이션 락 안에서 원자적으로 가구의 `running` 상태 및 `manual` 시나리오 여부를 검증하므로 경쟁 상태가 발생하지 않습니다. 비 manual 가구 제어 시 HTTP `409 Conflict`를 반환합니다.

##### C. 시뮬레이터 상태 조회 (`GET /api/status`)
현재 시뮬레이터의 실행 상태, 전체/가구별 진행 상황 및 최신 메트릭을 조회합니다.
* **응답 본문 (HTTP 200 OK)**:
  ```json
  {
    "is_running": true,
    "is_paused": false,
    "current_mode": "multi",
    "cycle_count": 15,
    "global_cycle_count": 15,
    "active_households": {
      "H001": { "scenario": "peak", "status": "completed", "cycle_count": 60 },
      "H002": { "scenario": "routine_missed", "status": "running", "cycle_count": 15 }
    },
    "last_metrics_by_house": {
      "H001": { ... },
      "H002": { ... }
    },
    "simulation_date": "2026-09-10",
    "resolved_start_time": "2026-09-09T23:10:01.000Z"
  }
  ```
  * `is_running`: `running` 상태인 가구가 1개라도 존재하면 `true`, 전체 완료 또는 정지 시 `false`
  * `is_paused`: 일시정지 상태 여부 (`true` / `false`)
  * `current_mode`: 단일 가구는 해당 시나리오 이름(`peak` 등), 다중 가구는 `"multi"`
  * `active_households`: 가구별 독립 라이프사이클 상태 (`running`, `completed`, `stopped`)
  * `last_metrics_by_house`: 가구별 최신 메트릭의 신뢰할 수 있는(authoritative) 상태 (완료 후에도 조회 가능)

##### D. 시뮬레이션 일시정지 / 재개 (`POST /api/pause`, `POST /api/resume`)
* **일시정지 (`POST /api/pause`)**: 실행 중인 시뮬레이터를 일시정지합니다. Manager 락 및 `tick_lock`으로 보호되어 응답 반환 이후에는 추가적인 MQTT/SSE 발행과 cycle 및 가상 시각 증가가 일절 발생하지 않습니다.
  * 성공 응답 (HTTP 200 OK): `{"status": "paused"}`
* **재개 (`POST /api/resume`)**: 일시정지된 시뮬레이터를 기존 상태(cycle, 가전 상태, 가상 시각)에서 정확히 다음 1초 tick으로 재개합니다.
  * 성공 응답 (HTTP 200 OK): `{"status": "resumed"}`

##### E. 시뮬레이션 중지 및 초기화 (`POST /api/stop`, `POST /api/reset`)
* **중지 (`POST /api/stop`)**: 실행 중이거나 일시정지 상태인 워커를 안전하게 중지합니다. 완료된 가구는 `completed` 상태를 보존하고, 실행 중이던 가구만 `stopped`로 마킹됩니다.
  * 성공 응답 (HTTP 200 OK): `{"status": "stopped"}`
* **초기화 (`POST /api/reset`)**: 시뮬레이터를 중지하고 모든 가구 상태, 메트릭, 일시정지 플래그, 시뮬레이션 날짜를 초기화합니다.
  * **reset 성공 (HTTP 200 OK)**:
    * 워커 종료 완료
    * `active_households`, `last_metrics_by_house`, `last_metrics`, `simulation_date`, `resolved_start_time`, `cycle_count` 완전 초기화
    * 응답: `{"status": "reset"}`
  * **reset 실패 (HTTP 503 Service Unavailable)**:
    * 워커 종료 대기(join 타임아웃 10초) 후에도 워커가 여전히 살아있는 경우 발생
    * reset 초기화는 수행되지 않으며 가구·메트릭·날짜·cycle 데이터는 보존됩니다.
    * 단, 종료 요청(`stop_event.set()`)은 이미 워커에 전달된 상태이므로 워커는 이후 종료될 수 있으며, `is_paused`는 `false`로 전이될 수 있습니다.
    * 응답: `{"error": "RESET_FAILED", "code": "RESET_FAILED", "message": "시뮬레이터 워커 종료에 실패하여 리셋할 수 없습니다."}`
    * **클라이언트는 HTTP 503 수신 후 `GET /api/status`로 최종 상태를 확인해야 합니다.**

#### 3) 가상 시작 시각 결정 우선순위 및 규칙
시뮬레이터는 `--date`(`simulation_date`)와 `--start-time` 입력에 대해 다음의 명확한 우선순위 규칙을 적용합니다:

| 우선순위 규칙 | 조합 조건 | 시작 시각 결정 규칙 | 예시 |
| :--- | :--- | :--- | :--- |
| **규칙 A** | `--date` + 시간 형식 `--start-time` (`HH:MM:SS`) | 선택한 날짜 + 지정 시간 결합 (KST) | `--date 2026-09-10 --start-time 09:00:00`<br>➡️ `2026-09-10 09:00:00+09:00` |
| **규칙 B** | 시간 형식 `--start-time` (`HH:MM:SS`) 단독 | KST 오늘 날짜 + 지정 시간 결합 (KST) | `--start-time 09:00:00`<br>➡️ `(오늘 날짜) 09:00:00+09:00` |
| **규칙 C** | 완전한 ISO datetime `--start-time` 단독 | 지정된 ISO datetime 유지 (tz 없으면 KST) | `--start-time 2026-10-01T15:30:00`<br>➡️ `2026-10-01 15:30:00+09:00` |
| **규칙 D** | `--date` + 완전한 ISO datetime 동시 지정 | **충돌로 간주하여 거절** (`ValueError` / 종료) | `--date와 완전한 ISO --start-time을 함께 사용할 수 없습니다.` |
| **규칙 E** | `simulation_date`만 단독 지정 | • `normal_routine` 포함: 선택 날짜 + `08:04:58` KST<br>• `routine_missed` 포함: 선택 날짜 + `08:10:01` KST<br>• 기타 시나리오만: 선택 날짜 + 시작 시점 현재 KST 시각 | `--scenario normal_routine --date 2026-09-15`<br>➡️ `2026-09-15 08:04:58+09:00` |
| **규칙 F** | 아무 값도 지정하지 않음 | • `normal_routine` 포함: 오늘 날짜 + `08:04:58` KST<br>• `routine_missed` 포함: 오늘 날짜 + `08:10:01` KST<br>• 기타 시나리오만: 현재 KST 시각 스트리밍 | `--scenario normal_routine`<br>➡️ `(오늘 날짜) 08:04:58+09:00` |
| **규칙 G** | 다중 가구에서 `normal_routine`과 `routine_missed` 동시 요청 | **충돌로 간주하여 HTTP 400 거절** | `normal_routine과 routine_missed는 동일한 다중 실행에서 함께 사용할 수 없습니다.` |

> [!CAUTION]
> **날짜 및 ISO 시간 충돌 방지 (규칙 D)**:
> `--date` 옵션과 날짜가 포함된 완전한 ISO 형식의 `--start-time`(예: `2026-09-10T09:00:00`)을 동시에 지정하면 날짜 정보가 충돌하므로 허용되지 않으며 명확한 에러 메시지와 함께 실행이 거절됩니다. 특정 날짜에 특정 시각을 지정하려면 반드시 시간 전용 형식(`HH:MM:SS` 또는 `HH:MM`)의 `--start-time`을 조합하십시오.

#### 4) KST 입력의 UTC 변환 및 타임스탬프 일관성
* **타임존 변환**:
  * KST는 UTC보다 9시간 빠릅니다 (`UTC+09:00`).
  * 예: KST `2026-09-10 14:30:15` ➡️ UTC `2026-09-10T05:30:15.000Z`
  * 예: KST `2026-09-10 08:10:01` ➡️ UTC `2026-09-09T23:10:01.000Z`
* **페이로드 타임스탬프 일치 보장**:
  * MQTT 메시지의 `measured_at`, `ts`와 웹 SSE 실시간 스트림의 `now_iso`는 **밀리초 단위까지 완전히 동일한 UTC ISO 8601 문자열**로 발행됩니다.
* **시간 누적 및 자정 넘김**:
  * 각 시뮬레이션 사이클마다 가상 시각이 정확히 1초(`+ timedelta(seconds=1)`)씩 증가합니다.
  * 벽시계(wall clock)를 매번 재조회하지 않으므로 네트워크 지연이나 슬립 오차에 의해 날짜와 시각이 흔들리지 않습니다.
  * 23:59:59에서 1초가 지나면 다음 날짜의 00:00:00으로 자연스럽게 롤오버됩니다.

#### 5) CLI에서 날짜 및 시작 시각 지정 예시
CLI(`simulator.py`)에서도 `--date` (`-d`) 및 `--start-time` 옵션을 조합하여 동일한 가상 시계 제어가 가능합니다.

```bash
# 1. 특정 날짜를 지정하여 피크 시연 실행 (시작 시점의 KST 시:분:초 결합)
python simulator.py --scenario peak --date 2026-09-10

# 2. 특정 날짜와 임의의 시작 가상 시각을 함께 지정 (2026-09-10 09:00:00 KST 시작)
python simulator.py --scenario random --date 2026-09-10 --start-time 09:00:00

# 3. 날짜 생략 후 --start-time만 지정 (오늘 날짜의 08:15:00 KST 시작)
python simulator.py --scenario routine_missed --start-time 08:15:00

# 4. 날짜 생략 시 CLI 기본값 사용 (routine_missed: 오늘 08:15:00 KST)
python simulator.py --scenario routine_missed
```

#### 6) 배속(Speed Multiplier) 기능 및 API 규격 (`/api/start`, `/api/speed`)

웹 시뮬레이터는 발표 및 시연 시 긴 대기 시간을 단축할 수 있도록 1x, 2x, 5x, 10x 배속 제어를 지원합니다.

##### A. 배속별 주기 및 예상 소요 시간
* **1x 배속 (1.0초 간격)**:
  * H001 정상 일상 (`normal_routine`, 308사이클): 약 5분 8초 (308.0초)
  * H001 이상 감지 (`routine_missed`, 300사이클): 약 5분 (300.0초)
* **2x 배속 (0.5초 간격)**:
  * H001 정상 일상: 약 2분 34초 (154.0초)
  * H001 이상 감지: 약 2분 30초 (150.0초)
* **5x 배속 (0.2초 간격)**:
  * H001 정상 일상: 약 1분 2초 (61.6초)
  * H001 이상 감지: 약 1분 (60.0초)
* **10x 배속 (0.1초 간격)**:
  * H001 정상 일상: 약 30.8초
  * H001 이상 감지: 약 30.0초

##### B. 가상 시각 불변성 (+1초 사이클 보존 원칙)
* 배속이 변경되더라도 시뮬레이션의 가상 시각은 실제 경과 시간이 아니라 **사이클을 기준으로 매 tick 정확히 +1초씩 증가**합니다.
* 다음 필드는 배속 설정과 무관하게 매 사이클 정확히 +1초 연속성을 엄격히 유지합니다:
  * MQTT 페이로드: `measured_at`, `ts`
  * 웹 SSE 스트림: `now_iso`, `simTimeKst` 및 가상 시각 관련 필드
* 10배속에서는 308개의 1초 가상 계측 데이터가 실제 약 30.8초 동안 고속으로 발행됩니다.

##### C. 시작 요청 시 interval 지정 (`POST /api/start`)
시뮬레이션 시작 시 `interval` 필드를 선택적으로 지정할 수 있습니다:
```json
{
  "households": [
    { "house": "H001", "scenario": "normal_routine" }
  ],
  "simulation_date": "2026-09-15",
  "interval": 0.1
}
```
* `interval` 미전달 시: 기본값 `1.0`초로 시작됩니다.
* 허용 범위: `0.1`초 이상 `10.0`초 이하 (숫자 float/int).
* 잘못된 값(`null`, 문자열, `bool`, 0, 음수, `NaN`, `Infinity`, 0.1 미만, 10.0 초과) 전달 시 HTTP `400 Bad Request`로 거절되며, 기존 실행 중인 워커가 있다면 중지되지 않고 보호됩니다.
* 단일 가구 하위 호환 요청(`{"scenario": "peak", "house": "H001", "interval": 0.5}`)에서도 동일하게 지원됩니다.
* **시작 성공 응답 예시 (HTTP 200 OK - H001 단일 실행)**:
  ```json
  {
    "status": "started",
    "scenario": "normal_routine",
    "house": "H001",
    "households": [
      { "house": "H001", "scenario": "normal_routine" }
    ],
    "simulation_date": "2026-09-15",
    "resolved_start_time": "2026-09-14T23:04:58.000Z",
    "interval": 0.1,
    "speed": 10.0
  }
  ```
  *(참고: `scenario` 필드는 단일 가구 실행 시 해당 가구의 시나리오명(`normal_routine`, `peak` 등)이 반환되며, 2개 이상의 가구를 동시에 실행할 때만 `"multi"`로 반환됩니다.)*

##### D. 실행 중 동적 배속 변경 (`POST /api/speed`)
시뮬레이션 실행 중(또는 일시정지 중) 발행 주기를 동적으로 변경할 수 있습니다. 다음 두 가지 JSON 형식 중 정확히 하나를 사용해야 합니다:

* **방법 1. interval(초) 직접 전달**:
  ```json
  {
    "interval": 0.2
  }
  ```

* **방법 2. speed(배속) 전달 (`interval = 1.0 / speed`로 자동 환산)**:
  ```json
  {
    "speed": 5
  }
  ```

* **요청 검증 규칙**:
  * `interval`과 `speed`를 동시에 전달하거나 둘 다 전달하지 않으면 HTTP `400 Bad Request`
  * 정의되지 않은 알 수 없는 필드가 포함되어 있으면 HTTP `400 Bad Request`
  * `bool`, 문자열, `null`, 0, 음수, `NaN`, `Infinity` 거절
  * 최종 환산된 `interval`은 반드시 `0.1`초 이상 `10.0`초 이하 범위여야 함
  * 시뮬레이터가 실행 중이 아닐 때 호출하면 HTTP `409 Conflict` (`INVALID_MODE`)
  * 일시정지(Pause) 상태에서도 배속 변경이 허용되며, 재개(Resume) 시 변경된 주기가 즉시 적용됨
* **성공 응답 (HTTP 200 OK)**:
  ```json
  {
    "status": "speed_updated",
    "interval": 0.2,
    "speed": 5.0
  }
  ```

##### E. 리셋(Reset) 및 초기 상태 보장
* `/api/reset` 성공 시 서버의 내부 interval은 기본값인 `1.0`초로 초기화됩니다.
* 이전 실행에서 10배속을 사용했더라도 `interval`을 생략한 새 시작 요청은 반드시 1배속(`1.0`초)으로 시작됩니다.

##### F. AI 서비스 연동 경계 및 안내
* 본 시뮬레이터는 향후 AI 모델 정상/이상 판정에 필요한 결정론적 전력 입력 패턴을 고속 공급하는 역할을 담당합니다.
* AI 실시간 추론 서비스 연결 전 단계이므로 실제 AI 알림이나 Kafka 이벤트 발생을 보장하거나 단정하지 않습니다.

---

## 7. 정적 파형 시각화 도구 (`visualize_waveform.py`)

MQTT 브로커나 웹 서버 없이, CLI에서 파형 데이터를 생성하여 정적 HTML 대시보드 리포트(`waveform_report.html`) 및 CSV 파일로 추출할 수 있습니다.

```bash
# 1. 루틴 누락(ROUTINE_MISSED) 300초 대기전력 파형 생성 및 브라우저 확인
python visualize_waveform.py --scenario routine_missed --seconds 300 --csv

# 2. 10초 피크(3,000W+) 시연 파형 생성
python visualize_waveform.py --scenario peak --seconds 60

# 3. 6대 가전 대표 기동 데모 파형 (기본값)
python visualize_waveform.py --scenario demo

# 4. 순수 확률 기반 랜덤 시뮬레이션
python visualize_waveform.py --random --seconds 600
```

---

## 8. 전체 데이터 파이프라인 연동 흐름

```text
+------------------------------------+
|  스마트홈 전력 시뮬레이터          |
|  - simulator.py / waveform_viewer  |
|  - H001~H1000 전력 데이터 실시간 생성 |
+------------------------------------+
                  │
                  │ MQTT Publish (v1/power/sim/{house}/main, QoS 1)
                  ▼
+------------------------------------+
|  Mosquitto MQTT Broker (Port 1883) |
|  - max_connections 2000            |
|  - max_inflight_messages 200       |
|  - max_queued_messages 10000       |
+------------------------------------+
                  │
                  │ MQTT Subscribe (v1/power/sim/+/main)
                  ▼
+------------------------------------+
|  MQTT-Kafka Bridge (bridge.py)     |
|  - household_id 기반 파티셔닝 키   |
+------------------------------------+
                  │
                  │ Kafka Produce (Key: household_id)
                  ▼
+------------------------------------+
|  Apache Kafka Broker (Port 9092)   |
|  - Topic: power.raw.v1 (24개 파티션) |
|  - 24개 파티션 균등 해시 분산      |
+------------------------------------+
                  │
                  ▼
     다운스트림 서비스 (BE-2 Flink 분석, BE-3 모니터링 SSE, HDFS 로더)
```

---

## 9. E2E 통합 시나리오(일일 활동·20일 기준선·28일 루틴 변화) 시뮬레이터 원천 MQTT 발행 검증 (`tools/verify_activity_normal_mqtt.py`)

외부 서비스(Kafka, PostgreSQL, AI 추론)에 연결하지 않고, Mosquitto MQTT 브로커와 시뮬레이터 HTTP API만을 활용하여 H001 가구의 통합 카탈로그 10개 시나리오(1일 활동 6종, 20일 기준선 1종, 28일 루틴 변화 3종) 전체 타임라인 원천 데이터가 QoS 1로 계획 결측을 제외하고 결측 없이 정확히 발행되는지 독립적으로 검증하는 스트리밍 검증 도구입니다.

### 9.1 모듈 실행 방법
시뮬레이터 루트 디렉터리(`infrastructure/mqtt/simulator`)에서 모듈로 실행합니다:

```bash
# 기본 실행 (기본 브로커 localhost:1883, API http://127.0.0.1:8085, 기본 시나리오 ACTIVITY_NORMAL, 기준일자 2026-09-16)
python -m tools.verify_activity_normal_mqtt \
  --api-url http://127.0.0.1:8085

# 특정 시나리오 및 기준 일자 지정 실행 예시 (결측 시나리오 검증)
python -m tools.verify_activity_normal_mqtt \
  --scenario ACTIVITY_INSUFFICIENT \
  --reference-date 2026-09-16 \
  --api-url http://127.0.0.1:8085

# 28일 루틴 변화 시나리오 실행 예시 (기준일자 2026-09-16은 마지막 날을 의미하며, 2026-08-20~2026-09-16 28일간 검증)
python -m tools.verify_activity_normal_mqtt \
  --scenario ROUTINE_CHANGED_LATER \
  --reference-date 2026-09-16 \
  --api-url http://127.0.0.1:8085
```

### 환경변수 지정 및 타임아웃 상세 설정 예시

#### 1) Linux/macOS Bash (비밀번호 입력 시 화면 미출력)
```bash
read -rsp "MQTT Password: " MQTT_PASS
export MQTT_PASS
echo
python -m tools.verify_activity_normal_mqtt \
  --scenario ACTIVITY_NORMAL \
  --broker-host localhost \
  --broker-port 1883 \
  --broker-user simulator_user \
  --api-url http://127.0.0.1:8085 \
  --idle-timeout 30.0 \
  --overall-timeout 600.0
```

#### 2) Windows PowerShell (SecureString 기반 안전한 환경변수 등록)
```powershell
$env:MQTT_PASS = [System.Net.NetworkCredential]::new(
    '',
    (Read-Host -Prompt "MQTT Password" -AsSecureString)
).Password
python -m tools.verify_activity_normal_mqtt `
  --scenario ACTIVITY_NORMAL `
  --broker-host localhost `
  --broker-port 1883 `
  --broker-user simulator_user `
  --api-url http://127.0.0.1:8085 `
  --idle-timeout 30.0 `
  --overall-timeout 600.0
```

> **비밀번호 보안 주의**:
> CLI에 `--broker-pass` 옵션은 제공되지 않습니다. 브로커 인증 비밀번호는 `MQTT_PASS` 환경변수를 통해 전달하며, 로그·오류 메시지·JSON 결과 어디에도 비밀번호가 노출되지 않습니다.

> **시작 API 응답 유실 시 제한사항**:
> 시작 API 응답 자체가 유실되어 run_id를 확보하지 못한 경우 자동 stop을 보장할 수 없다. 이 경우 시뮬레이터 관리 화면 또는 서버 로그에서 활성 실행을 확인해야 한다.

### 9.2 지원 옵션 및 타임아웃 기본값
| CLI 옵션 | 기본값 | 설명 |
|---|---|---|
| `--scenario` | `ACTIVITY_NORMAL` | 검증 대상 시나리오 ID (통합 카탈로그 10종 허용)<br>• **단일 일자(1일, 86,400 슬롯)** 6종: `ACTIVITY_NORMAL`, `ACTIVITY_LOW`, `ACTIVITY_NONE`, `ACTIVITY_INSUFFICIENT`, `ACTIVITY_SESSION_MERGE`, `ACTIVITY_DURATION_CAP`<br>• **20일 기준선(1,728,000 슬롯)** 1종: `BASELINE_MICROWAVE_20D`<br>• **28일 루틴 변화(2,419,200 슬롯)** 3종: `ROUTINE_CHANGED_LATER`, `ROUTINE_CHANGED_EARLIER`, `ROUTINE_CHANGED_WITHIN_THRESHOLD` |
| `--reference-date` | `2026-09-16` | 시뮬레이션 기준 일자 (YYYY-MM-DD). 단일 일자 시나리오에서는 해당 일자를, 다일(20일, 28일) 시나리오에서는 전체 일정의 마지막 날(종료일)을 의미하며 시작일은 `reference_date - (total_days - 1)`로 자동 역산됩니다. |
| `--broker-host` | `localhost` / `MQTT_HOST` | MQTT 브로커 호스트명 |
| `--broker-port` | `1883` (평문) / `8883` (TLS) | MQTT 브로커 포트 번호 (`resolve_mqtt_port` 규칙) |
| `--broker-user` | `MQTT_USER` | MQTT 브로커 인증 계정 |
| `--tls-enabled` | `false` / `MQTT_TLS_ENABLED` | TLS 암호화 사용 여부 (`parse_tls_enabled` 해석) |
| `--ca-file` | `MQTT_CA_FILE` | TLS CA 인증서 파일 경로 |
| `--api-url` | `http://127.0.0.1:8085` | 시뮬레이터 Web API URL (기본 포트: 8085) |
| `--connect-timeout` | `10.0`초 | 브로커 소켓 연결 제한시간 |
| `--subscribe-timeout`| `10.0`초 | `v1/power/sim/H001/main` SUBACK 수신 대기시간 |
| `--http-timeout` | `15.0`초 | 시뮬레이터 API (`POST /runs`, `GET /runs/{id}`) 요청 제한시간 |
| `--idle-timeout` | `30.0`초 | 타겟 신규 메시지 미수신 시 유휴 타임아웃 |
| `--overall-timeout` | `600.0`초 | 전체 검증 최대 허용 시간 (다일 시나리오의 경우 슬롯 수에 비례하여 자동 상향됨) |
| `--poll-interval` | `0.5`초 | 시뮬레이터 완료 상태 폴링 주기 |

> **다일 시나리오 제한시간 자동 상향 및 예상 소요 시간**:
> - 다일 시나리오 검증 시 전체 슬롯 수에 비례하여 `overall-timeout`이 자동으로 상향 계산됩니다:
>   $$\text{effective\_overall\_timeout} = \max\left(\text{CLI overall\_timeout},\; \max\left(600.0,\; \frac{\text{total\_virtual\_slots}}{500.0} + 300.0\right)\right)$$
> - **예상 소요 시간**:
>   - 1일 활동 시나리오(86,400 슬롯): BURST 약 60~90초 (타임아웃 기본 600초 유지)
>   - 20일 기준선 시나리오(1,728,000 슬롯): BURST 약 20분 (타임아웃 약 3,756초로 자동 상향)
>   - 28일 루틴 시나리오(2,419,200 슬롯): BURST 약 30분 (타임아웃 약 5,138.4초로 자동 상향)

### 9.3 메모리 복잡도 및 비트맵 검증
- **메모리 복잡도**: $O(\text{total\_virtual\_slots})$
- **메모리 점유**:
  - 1일(86,400 슬롯): `bytearray(86,400)` 약 **86KB**
  - 20일(1,728,000 슬롯): `bytearray(1,728,000)` 약 **1.7MB**
  - 28일(2,419,200 슬롯): `bytearray(2,419,200)` 약 **2.4MB**
- 수백만 개의 JSON payload 전체 목록을 메모리에 저장하지 않고, 실시간으로 타임스탬프의 전체 타임라인 상대 초 `relative_second`를 인덱스로 비트맵에 마킹합니다.
- `run_id` 기반 실시간 기대 UUID5와 대조하여 `target_unique_messages`, `duplicate_deliveries`, `foreign_run_messages`를 엄격히 분리 집계합니다.

### 9.4 다운스트림 AI 팀 연동 명세 (Handoff Specification)
시뮬레이터 원천 검증 완료 후, downstream AI 서비스(`realtime-analysis-service`) 팀에 공식 인계하는 계약 명세입니다:

```yaml
scenario_id: "ACTIVITY_NORMAL"
household_id: "H001"
reference_date: "2026-09-16"
timezone: "Asia/Seoul"
virtual_interval: "1s"
total_planned_slots: 86400
total_published_samples: 86400
omitted_samples: 0
mqtt_topic: "v1/power/sim/H001/main"
mqtt_qos: 1
payload_fields_count: 14
first_sample_measured_at: "2026-09-16T00:00:00+09:00"
last_sample_measured_at: "2026-09-16T23:59:59+09:00"
simulator_completion_status: "COMPLETED"
appliance_schedule_summary:
  - appliance: "kettle"
    start_time: "07:00:00"
    duration_seconds: 120
    second_of_day_range: [25200, 25319]
    absolute_cycle_range: [25201, 25320]
  - appliance: "microwave"
    start_time: "09:00:00"
    duration_seconds: 300
    second_of_day_range: [32400, 32699]
    absolute_cycle_range: [32401, 32700]
  - appliance: "kettle"
    start_time: "12:00:00"
    duration_seconds: 120
    second_of_day_range: [43200, 43319]
    absolute_cycle_range: [43201, 43320]
  - appliance: "vacuum_cleaner"
    start_time: "15:00:00"
    duration_seconds: 1200
    second_of_day_range: [54000, 55199]
    absolute_cycle_range: [54001, 55200]
  - appliance: "microwave"
    start_time: "18:00:00"
    duration_seconds: 300
    second_of_day_range: [64800, 65099]
    absolute_cycle_range: [64801, 65100]
  - appliance: "kettle"
    start_time: "21:00:00"
    duration_seconds: 120
    second_of_day_range: [75600, 75719]
    absolute_cycle_range: [75601, 75720]
expected_physical_on_seconds: 2160
```

## 10. 결정적 시나리오(E2E) 웹 대시보드 제어 패널 (`waveform_viewer.html`)

웹 인터페이스(`waveform_viewer.html`) 내에 결정적 시나리오 실행 및 모니터링을 위한 전용 제어 패널이 추가되었습니다.

### 10.1 주요 기능 및 인터페이스 구성
- **시나리오 및 가구 선택**: 백엔드 REST API(`GET /api/e2e/scenarios`)로부터 10종 시나리오 메타데이터(일수, 발행 슬롯 수)를 동적으로 로드하여 드롭다운 구성.
- **실행 모드 제어**:
  - `BURST`: 지연 없는 최대 속도 배치 발행 (`speed` 파라미터 제외)
  - `REALTIME`: 1.0초 가상 시간 동기화
  - `ACCELERATED`: 사용자 지정 배속(0 초과 양수) 적용. 패널에서는 배속 입력이 필수이며, `POST /api/e2e/runs`에서 `speed`를 생략하면 가구 수 기반 안전 배속이 자동 산출됩니다 ([E2E_GUIDE.md](E2E_GUIDE.md) 6절)
- **기존 실행 연결 (`e2eBtnAttach`)**: 명령줄 검증 도구 등으로 이미 시작된 실행이 있을 때 `run_id`를 직접 입력하여 실시간 모니터링에 연결.
- **409 충돌 안내**: 이미 실행 중인 E2E 세션이 있을 경우 명확한 안내 문구 노출.
- **가구별 상태 기반 제어**: 개별 가구의 상태(`RUNNING`, `PAUSED`, `PAUSING`, `STOPPING` 등)에 따라 일시정지, 재개, 중지 버튼을 정밀 제어.
- **정산 슬롯 기준 진행률 표기**: 진행률 분모를 `planned_virtual_slots`로 고정하여 결측 시나리오(`ACTIVITY_INSUFFICIENT`)에서도 100% 한도를 엄격히 준수하며, 발행 수(`published_samples / planned_publish_samples`)를 별도 열로 명확히 분리 표기.

### 10.2 결정적 시나리오(E2E) 파형 실시간 스트리밍 및 결측 렌더링 계약
- **실시간 파형 렌더링**: `REALTIME` 및 `ACCELERATED` 모드에서 실제 물리 계측값(`totalP`, `apparentS`, `totalQ`, `currentA`, `voltage`, `pf`, `devices`)을 `manager.broadcast_external()` 전송 전용 경로를 통해 SSE로 수신하여 대시보드 차트(`chartMain`, `chartSecondary`)에 표시합니다.
- **BURST 모드 완전 제외**: `execution_mode == BURST`일 때는 화면 전송 콜백을 단 1회도 호출하지 않아 전속력 배치 발행 성능을 100% 보존합니다.
- **초당 20회(20Hz) 전송 상한**: `ACCELERATED` 등 고배속 실행 시에도 화면 전송은 초당 최대 20회로 제한되며, 상한을 초과하는 틱은 화면 전송만 스킵하고 MQTT 발행 및 `published_samples`/`omitted_samples` 카운터는 전량 유지됩니다.
- **레거시 상태 오염 방지 (`source="E2E"`)**:
  - `manager.broadcast_external()`은 구독자 SSE 큐에만 데이터를 전달하고 레거시 호환용 `last_metrics`와 `last_metrics_by_house`를 절대 수정하지 않습니다.
  - 웹 화면에서 `source === "E2E"`로 분기하여 레거시 상태 배지(`badgeStatus`), 안내 문구(`timelineNotice`), 레거시 가구 테이블 행을 일절 변경하지 않습니다.
- **계획 결측 슬롯 렌더링 계약**:
  - 결측 구간은 `measurementAvailable: false`, `sensorFault: false`, 계측치 `null`로 발행됩니다.
  - 파형 차트에 `null`을 넣어 선이 자연스럽게 끊기도록 렌더링하며(0으로 채우지 않음), 전력 메트릭 카드는 "—"로 표시하여 이전 값으로 인한 오해를 방지합니다. 정상 틱 재개 시 차트는 다시 정상 연결됩니다.

---

## DATA_GAP 감지: 실제 수신 중단과 측정 시각 공백

분석 서비스(`realtime-analysis-service`)는 두 가지 경로로 전력 데이터 공백(DATA_GAP)을 감지합니다.

### 1. 수신 중단 감지 (watchdog, `source: "watchdog"`)

| 항목 | 내용 |
|------|------|
| 감지 주체 | `DataQualityWatchdog` 스레드 (주기적 `detect_gaps()` 호출) |
| 기준 시각 | 마지막 Kafka 메시지의 **실제 수신 시각** (`received_at`) |
| 감지 조건 | 현재 실시간 시각 − 마지막 수신 시각 ≥ 임계값 (기본 120초) |
| 발생 시점 | 메시지가 전혀 오지 않아도 watchdog 폴링 주기마다 검사 |
| 용도 | 시뮬레이터가 멈추거나 네트워크가 단절된 경우 |

### 2. 측정 시각 공백 감지 (`source: "measured_at"`)

| 항목 | 내용 |
|------|------|
| 감지 주체 | `DataQualityMonitor.observe()` (Kafka 메시지 수신 시 호출) |
| 기준 시각 | 연속된 두 유효 **측정 시각** (`measured_at`)의 차이 |
| 감지 조건 | (`새 measured_at` − `직전 measured_at` − 1초) ≥ 임계값 |
| 발생 시점 | 고장 후 **첫 복구 샘플이 도착했을 때** GAP을 발견 |
| 용도 | 배속 실행 시 실제 수신 간격은 짧지만 가상 시각에 공백이 있는 경우 |

### 배속 실행 시 이벤트 발생 시점

`sensor_fault` 시나리오를 10×배속으로 실행하면:

1. **고장 구간 중**: MQTT 메시지 0건 발행. 실제 12초만 대기하므로 watchdog(120초 임계값)은 감지 불가.
2. **복구 첫 샘플 도착 시**: `observe()`가 `measured_at` 간격(121초)에서 예상 간격(1초)을 뺀 120초가 임계값 이상임을 발견 → `DATA_GAP(source="measured_at")` 발행.
3. **복구 확인 완료 시**: 설정된 `recovery_confirmation_samples` 충족 후 → `DATA_RECOVERED(source="measured_at")` 발행.

> **참고**: CLI `sensor_fault`는 `--date`·`--start-time` 미지정 시 루프 시작 전 UTC 시각을 한 번 고정하여 배속과 무관하게 `measured_at`이 가상 1초씩 증가합니다. 웹 시뮬레이터는 기존과 동일하게 `base_dt + cycle` 방식을 사용합니다.
