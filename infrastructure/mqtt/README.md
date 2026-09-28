# MQTT Broker와 시뮬레이터 (MQTT & Simulator)

스마트홈 메인 분전반(Smart Meter)의 초 단위 전력 계측 환경을 모사하여 Mosquitto MQTT 브로커로 실시간 스트리밍하고, MQTT-Kafka Bridge를 통해 Kafka 및 실시간 분석 서비스로 데이터를 공급하는 인프라 패키지입니다.

Mosquitto 브로커 실행은 [local Compose](../local/compose.yaml) 또는 [EC2-A Compose](../ec2-a/compose.yaml)에서 관리하며, 이 폴더에서는 브로커 설정 파일과 전력 시뮬레이터를 관리합니다.

---

## 1. 파일 구성

```text
infrastructure/mqtt/
├── config/
│   ├── mosquitto.local.conf       # 로컬 1883 평문 리스너 및 계정 인증 설정
│   ├── mosquitto.production.conf  # 운영 8883 TLS 리스너 및 보안 인증 설정
│   ├── passwd                     # mosquitto_passwd 해시 계정 파일 (Git 제외)
│   └── mosquitto.conf             # 레거시 호환용 설정
│
├── simulator/
│   ├── simulator.py               # 전력 시뮬레이터 호환 Facade & CLI 진입점
│   ├── engine/                    # 핵심 엔진 패키지 (config, tls, profiles, state, power_model, publisher)
│   ├── web_server.py              # 인터랙티브 웹 서버 진입점 (CLI 인자 처리 및 초기화)
│   ├── server/                    # 웹 서버 모듈 패키지 (config, manager, request_handler)
│   ├── scenarios.py               # 대기전력 물리 모델, 루틴 누락/피크 시나리오 모듈
│   ├── waveform_viewer.html       # 브라우저 기반 실시간 인터랙티브 시각화 대시보드
│   ├── tests/                     # 단위 및 실시간 라이브 통합 테스트 스위트
│   │   ├── fixtures/              # 회귀 검증용 기준선 및 테스트 CA 인증서
│   │   ├── test_mqtt_tls.py       # [신규] TLS 설정/포트우선순위/SSLContext 고속 단위 테스트
│   │   ├── test_simulator_compatibility.py # 비네트워크 초고속 물리엔진 단위/호환성 테스트
│   │   └── test_manual_live_integration.py # 브로커 연동 실시간 라이브 통합 테스트
│   ├── visualize_waveform.py      # 정적 파형 생성 및 CSV/HTML 리포트 출력 도구
│   ├── requirements.txt           # 시뮬레이터 실행 의존성 (aiomqtt>=2.0.0, Python 3.10+)
│   └── README.md                  # 시뮬레이터 상세 매뉴얼
│
└── README.md                      # [현재 파일] MQTT 인프라 및 시뮬레이터 종합 가이드
```

---

## 2. 브로커 계정 및 보안 설정

### (1) `config/passwd` 사용자 계정

`mosquitto_passwd` 유틸리티에 의해 해시 암호화되어 관리되는 브로커 인증 계정입니다.

| 사용자명 (`Username`)   | 기본 비밀번호     | 역할 / 용도                                                      |
| :---------------------- | :---------------- | :--------------------------------------------------------------- |
| **`simulator_user`**    | `test1234`        | 스마트홈 전력 시뮬레이터의 발행(Publish) 전용 계정                |
| **`kafka_bridge_user`** | `test1234`        | MQTT 토픽을 구독하여 Kafka로 넘겨주는 브릿지(Bridge) 전용 계정   |

### (2) 브로커 주요 설정 (`config/mosquitto.local.conf`)

* **익명 접속 차단**: `allow_anonymous false` (반드시 계정 인증 필요)
* **포트**: `1883` (로컬 바인딩 `127.0.0.1:1883`)
* **리소스 제어**: `max_connections 2000`, `message_size_limit 262144` (256KB)
* **영속성**: `persistence false` (브로커 자체 디스크 저장은 끄고 Kafka가 파이프라인 버퍼 역할 담당)

---

## 3. 최초 환경 구축 및 실행 방법

### Step 1. 로컬 환경변수 및 비밀번호 준비

저장소 루트에서 [infrastructure/local](../local) 설정을 준비합니다.

```powershell
# infrastructure/local 폴더로 이동
cd infrastructure/local

# 최초 1회: local/.env 생성 및 MQTT passwd 초기화
.\Setup-Local.ps1 -InitializeMqtt
```

### Step 2. Mosquitto 컨테이너 구동

```powershell
docker compose up -d mosquitto
```

### Step 3. 브로커 정상 가동 및 익명 접속 차단 검증

```powershell
# 1) 컨테이너 실행 상태 확인
docker ps -f name=mosquitto-broker

# 2) 익명 접속 차단 검증 (Connection Refused 정상 발생 확인)
docker exec -it mosquitto-broker mosquitto_sub -t "v1/power/sim/#"

# 3) 정상 계정 구독 테스트 (메시지 대기 상태 진입 확인)
docker exec -it mosquitto-broker mosquitto_sub -t "v1/power/sim/#" -u "kafka_bridge_user" -P "test1234" -v
```

---

## 4. 스마트홈 전력 시뮬레이터 (`simulator.py`)

스마트홈 가구의 메인 분전반(`main`)에서 계측되는 초 단위 다변량 교류 전력 데이터(유효전력, 무효전력, 역률, 전류, 전압)를 물리 법칙에 맞게 계산하여 브로커로 실시간 스트리밍합니다.

### (1) 모델링 사양

* **대상 가구**: `H001` ~ `H010` (기본 10개 가구, 최대 1,000가구 부하 테스트 지원)
* **물리 엔진 연동**:
  * **상시 기저 대기전력**: 가구별 40~65W 상시 대기전력 (Random Walk 완만 변동)
  * **냉장고 컴프레서 주기**: 15~25분 가동(55~85W, 역률 0.78), 20~35분 정지 주기의 자동 주기성
  * **전압 AR-1 드리프트**: 220V 기준 212V ~ 228V 사이를 완만하게 변동
  * **6대 수동 가전 FSM**: 전기포트(1,700W), 인덕션(1,600W 서모스탯), 다리미(1,400W), 전자레인지(950W 마그네트론 돌입), 드라이기(950W), 청소기(820W 직권모터)
* **모듈 분리 구조**:
  * [simulator.py](file:///c:/Users/SSAFY/Desktop/S15P21D201/infrastructure/mqtt/simulator/simulator.py): CLI 인자 처리 및 하위 호환 Facade
  * [engine/](file:///c:/Users/SSAFY/Desktop/S15P21D201/infrastructure/mqtt/simulator/engine): 시뮬레이터 핵심 엔진 (`profiles`, `state`, `power_model`, `publisher`, `config`)
  * [scenarios.py](file:///c:/Users/SSAFY/Desktop/S15P21D201/infrastructure/mqtt/simulator/scenarios.py): 대기전력 물리 계산 및 이상치/피크 시나리오 스케줄 전담
  * [server/](file:///c:/Users/SSAFY/Desktop/S15P21D201/infrastructure/mqtt/simulator/server): 인터랙티브 웹 서버 비즈니스 로직, REST API, SSE 스트리밍, 동시성 락
  * [web_server.py](file:///c:/Users/SSAFY/Desktop/S15P21D201/infrastructure/mqtt/simulator/web_server.py): 경량 웹 서버 실행 진입점

### (2) 커맨드라인 옵션 상세 (`python simulator.py --help`)

| 옵션 | 단축키 | 기본값 | 설명 |
| :--- | :--- | :--- | :--- |
| `--scenario` | `-s` | `random` | 시나리오 모드 (`random`: 연속 확률, `peak`: 10초 피크, `routine_missed`: 08:10 루틴 누락 이상치) |
| `--houses` | `-n` | `10` | 대상 가구 수 (`H001` ~ `H{n:03d}`) |
| `--interval` | `-i` | `1.0` | 데이터 발행 주기 (초 단위) |
| `--hz` | | `None` | 가구당 초당 전송 횟수 (지정 시 `interval = 1/hz` 자동 환산) |
| `--count` | `-c` | `0` | 전송 사이클 횟수 (`0`: 무한, `N > 0`: N회 전송 후 종료, peak: 60, routine_missed: 300) |
| `--start-time` | | `None` | 가상 시작 시각 (예: `08:15:00`). routine_missed 모드는 기본값으로 오늘 아침 08:15:00 KST 적용 |
| `--host` | | `None` | MQTT 브로커 호스트 주소 (**TLS 시 인증서 SAN과 일치 필수. EC2-A 내부 실행 시에도 A 사설 IP 사용**) |
| `--port` | `-p` | `None` | MQTT 브로커 포트 번호 (미지정 시 `MQTT_PORT` 환경변수 또는 TLS 여부에 따라 `8883`/`1883` 자동 결정) |
| `--user` | `-u` | `None` | MQTT 인증 사용자명 (환경변수: `MQTT_USER`) |
| `--password` | | `None` | MQTT 인증 비밀번호 (**보안을 위해 환경변수 `MQTT_PASS` 사용 권장**, 미지정 시 `MQTT_PASS` 또는 `test1234`) |
| `--tls` / `--no-tls` | | `None` | MQTT TLS 암호화 연결 활성화 여부 (환경변수: `MQTT_TLS_ENABLED`,미지정 시 `MQTT_TLS_ENABLED` 또는 `False`) |
| `--ca-file` | | `None` | 브로커 검증에 사용할 CA 인증서 파일 경로 (PEM 형식, 환경변수: `MQTT_CA_FILE`) |
| `--qos` | | `1` | 발행 QoS 레벨 (`0` 또는 `1`) |
| `--quiet` | `-q` | `False` | 요약 모드 (매초 로그 생략, 5초 주기 통계 출력) |

> [!IMPORTANT]
> **포트 결정 우선순위**:
> `--port` CLI 명시 > `MQTT_PORT` 환경변수 > TLS 활성화 시 `8883` > 평문 연결 시 `1883`

### (3) 주요 실행 예시

#### 예시 A. 로컬 평문 실행 (localhost:1883)

로컬에 구동된 Mosquitto 브로커(`localhost:1883`)로 평문 통신합니다.

```bash
# 1. 루틴 누락(ROUTINE_MISSED) 300초 이상치 검증 모드
python infrastructure/mqtt/simulator/simulator.py --scenario routine_missed

# 2. 10초 피크(3,000W+) 시연 모드
python infrastructure/mqtt/simulator/simulator.py --scenario peak

# 3. 1,000가구 대규모 스트리밍 부하 테스트 (초당 1,000건)
python infrastructure/mqtt/simulator/simulator.py -n 1000 -q
```

#### 예시 B. 운영 EC2 TLS 실행 (8883 포트 보안 연결)

운영 Mosquitto 브로커(EC2-A)는 8883 TLS 리스너만 지원하며, 서버 인증서 SAN에 EC2-A 사설 IP가 등록되어 있습니다.

> [!WARNING]
>
> * **인증서 SAN 일치**: EC2-A 내부 실행 시에도 `localhost` 대신 인증서 SAN에 등록된 사설 IP `<A_PRIVATE_IP>`를 지정해야 합니다.
> * **비밀번호 보안 및 쉘 히스토리 방지**:
>   * 커맨드라인에 `--password`를 직접 사용하면 프로세스 목록(`ps aux`)에 노출됩니다.
>   * 쉘에 `export MQTT_PASS=...`를 직접 타이핑하면 `~/.bash_history`에 평문 저장되므로, `read -rsp "MQTT Password: " MQTT_PASS && export MQTT_PASS` 또는 `chmod 600` 설정된 환경파일을 사용하십시오.
> * **CA 인증서 경로 예시**: EC2-A는 `~/mqtt-ca/ca.crt`, EC2-B는 `/opt/nilm/mqtt/certs/ca.crt` (서버 권한 필요할 수 있음).

```bash
# 환경변수 기반 운영 TLS 실행 (권장)
export MQTT_HOST=<A_PRIVATE_IP>
export MQTT_PORT=8883
export MQTT_USER=<MQTT_USER>
read -rsp "MQTT Password: " MQTT_PASS && export MQTT_PASS
export MQTT_TLS_ENABLED=true
export MQTT_CA_FILE=<CA_FILE_PATH>

python infrastructure/mqtt/simulator/simulator.py --scenario peak
```

---

## 5. 실시간 웹 시각화 대시보드 (`waveform_viewer.html` & `web_server.py`)

브라우저에서 직접 버튼을 클릭하여 시뮬레이터를 제어하고, 실시간 전력 파형 시각화와 **실제 Mosquitto MQTT 발행(Kafka 연동)**을 동시에 수행할 수 있습니다.

웹 컨트롤러 HTTP 서버는 보안 강화를 위해 **기본적으로 `127.0.0.1`에만 바인딩**됩니다.

> [!CAUTION]
>
> * **0.0.0.0 바인딩 및 미인증 제어 API 노출 위험**:
>   웹 컨트롤러의 제어 API(`/api/start`, `/api/stop` 등)는 별도의 인증이 없으므로, `--bind-host 0.0.0.0`으로 개방 시 인가되지 않은 외부 사용자가 전력 데이터 발행을 조작할 수 있습니다.
> * **권장 원격 접속 (SSH 터널링)**:
>   `127.0.0.1` 기본값을 유지하고 로컬 PC에서 SSH 포트 포워딩(`ssh -i <KEY.pem> -L 8085:127.0.0.1:8085 ubuntu@<EC2_IP>`)을 통해 접속하십시오.

```bash
# 1. 로컬 평문 실행 (기본 127.0.0.1:8085 바인딩, localhost:1883 브로커)
python infrastructure/mqtt/simulator/web_server.py

# 2. 운영 TLS 실행 (환경변수 설정 후 실행)
export MQTT_HOST=<A_PRIVATE_IP>
export MQTT_PORT=8883
export MQTT_USER=<MQTT_USER>
read -rsp "MQTT Password: " MQTT_PASS && export MQTT_PASS
export MQTT_TLS_ENABLED=true
export MQTT_CA_FILE=<CA_FILE_PATH>
python infrastructure/mqtt/simulator/web_server.py
```

* **[10초 피크 시연 시작 (3,000W+)] 버튼 (빨간색)**: 클릭 즉시 실제 MQTT 발행 시작, 10초 정각에 3,400~3,500W 치솟으며 피크 경보 배지 점멸
* **[루틴 누락 시연 시작 (08:10+)] 버튼 (주황색)**: 08:10 아침 루틴 미가동 대기전력 데이터를 300초간 실시간 발행하여 AI 이상치 감지 윈도우 검증
* **[연속 실시간 시뮬레이션] 버튼 (파란색)**: 6대 가전 무한 연속 동작 및 실시간 MQTT 발행
* **실시간 6대 수동 가전 제어 패널**: 화면 스위치를 클릭해 전기포트, 인덕션, 다리미, 전자레인지 등을 실시간으로 켜고 끄며 실제 MQTT 발행 및 파형 변화 동기화
* **실시간 제어 바**: 일시정지, 재생, 리셋, 재생 속도 조절(1x, 2x, 5x)
* **CSV 내보내기**: 시뮬레이션 시계열 데이터를 엑셀 호환 UTF-8 BOM CSV 파일로 즉시 저장

---

## 6. 테스트 스위트 (`infrastructure/mqtt/simulator/tests`)

시뮬레이터의 물리 엔진과 웹 서버의 신뢰성을 보장하기 위해 단위 테스트와 통합 테스트를 제공합니다.

| 테스트 스크립트 | 유형 | 소요 시간 | 주요 검증 내용 |
| :--- | :--- | :--- | :--- |
| **`test_mqtt_tls.py`** | 단위 테스트 (비네트워크) | **~0.03초** | TLS 환경변수 파서(오타 방어), 포트 결정 우선순위, 유효 CA SSLContext 생성, 동기 오류 반환 |
| **`test_simulator_compatibility.py`** | 단위 테스트 (비네트워크) | **~0.01초** | 물리 계측 수식, 가전 FSM 전이, bcc5bdd 커밋 기준선 데이터 48스텝 100% 일치 검증 |
| **`test_manual_live_integration.py`** | 라이브 E2E 통합 테스트 | **~85초** | 실제 Mosquitto 브로커 연동, MQTT ↔ SSE 6개 물리량 일치, 인덕션 전체 듀티사이클 실시간 검증 (미가동 시 자동 skip) |

```powershell
# 1. TLS 설정 단위 테스트 실행
python -u infrastructure/mqtt/simulator/tests/test_mqtt_tls.py

# 2. 물리 엔진 단위 테스트 실행
python -u infrastructure/mqtt/simulator/tests/test_simulator_compatibility.py

# 3. 브로커 연동 라이브 통합 테스트 실행 (브로커 가동 시)
python -u infrastructure/mqtt/simulator/tests/test_manual_live_integration.py
```

---

## 7. 전체 데이터 파이프라인 연동 구조 (E2E Data Flow)

스마트홈 분전반부터 실시간 분석 서비스 및 대시보드까지의 전체 전달 흐름입니다.

```text
+-------------------------------------------------------------+
|  1. 전력 시뮬레이터 (infrastructure/mqtt/simulator)         |
|  - simulator.py (scenarios.py 기반 물리 엔진)               |
|  - H001~H1000 가구 전력 데이터 실시간 생성                  |
+-------------------------------------------------------------+
                               │
                               │ MQTT Publish (v1/power/sim/{house}/main, QoS 1)
                               ▼
+-------------------------------------------------------------+
|  2. Mosquitto MQTT 브로커 (Port 1883)                       |
|  - max_connections 2000, 익명 접속 차단                     |
|  - 계정 기반 인메모리 고속 토픽 라우팅                       |
+-------------------------------------------------------------+
                               │
                               │ MQTT Subscribe (v1/power/sim/+/main)
                               ▼
+-------------------------------------------------------------+
|  3. MQTT-Kafka Bridge (infrastructure/mqtt-kafka-bridge)    |
|  - household_id 기반 Kafka 파티셔닝 키 할당                 |
|  - Kafka 전달 확인 후 MQTT PUBACK 응답 (신뢰성 보장)        |
+-------------------------------------------------------------+
                               │
                               │ Kafka Produce (Topic: power.raw.v1, 24 파티션)
                               ▼
+-------------------------------------------------------------+
|  4. Apache Kafka 브로커 (Port 9092)                         |
|  - 토픽: power.raw.v1                                       |
|  - Kafka UI (Port 8091)로 실시간 메시지 관제 지원           |
+-------------------------------------------------------------+
                               │
                               │ Kafka Consume (Consumer Group)
                               ▼
+-------------------------------------------------------------+
|  5. AI 실시간 분석 서비스 (ai/realtime-analysis-service)    |
|  - 가구별 299개 슬라이딩 윈도우 버퍼 적재                   |
|  - 08:10 루틴 누락(ROUTINE_MISSED) 이상치 판정             |
+-------------------------------------------------------------+
                               │
                               │ 이상 감지 시 Event Produce
                               ▼
+-------------------------------------------------------------+
|  6. 이상 이벤트 토픽 (Kafka Topic: analysis.event.v1)       |
|  - 점수(Score 86) 및 근거(Reason) 이벤트 발행               |
|  - 다운스트림 알림/사고 대응 서비스로 전달                  |
+-------------------------------------------------------------+
```
