# Mosquitto MQTT Broker 설정 및 실행 가이드

본 문서는 Docker Compose 환경에서 Eclipse Mosquitto v2 기반의 MQTT 브로커를 구축하고, 계정 기반 보안 인증을 적용하여 실행하는 전체 과정 및 설정 내용을 정리한 가이드입니다.

---

## 1. 디렉터리 구조

```text
MQTT_test/
├── docker-compose.yml          # Mosquitto 컨테이너 오케스트레이션 정의
├── config/
│   ├── mosquitto.conf          # 브로커 환경 및 보안 정책 설정 파일
│   └── passwd                  # 해시 암호화된 사용자 인증 계정 파일
├── data/                       # (영구 저장 비활성화 상태)
├── log/                        # 로그 보관용 디렉터리
└── README.md                   # 본 문서
```

---

## 2. 주요 설정 파일 상세

### (1) `docker-compose.yml`
Mosquitto 브로커 컨테이너를 구동하고, 호스트의 설정 및 비밀번호 파일을 컨테이너 내부로 바인드 마운트합니다.

```yaml
version: '3.8'

services:
  mosquitto:
    image: eclipse-mosquitto:2
    container_name: mosquitto-broker
    ports:
      - "1883:1883"              # MQTT 기본 평문 포트 포워딩
    volumes:
      - ./config/mosquitto.conf:/mosquitto/config/mosquitto.conf
      - ./config/passwd:/mosquitto/config/passwd  # 인증 비밀번호 파일 매핑
    restart: unless-stopped
```

---

### (2) `config/mosquitto.conf`
Mosquitto v2부터 필수가 된 인증/리스너 보안 정책 및 성능 제어 설정입니다.

```conf
# =================================================================
# 1. Listeners (포트 및 통신 환경)
# =================================================================
# 로컬 파이프라인(Kafka Connect, 시뮬레이터) 내부 통신용 평문 포트
listener 1883

# 추후 외부 ESP32 단말기 접속을 위한 TLS 통신 포트 (현재는 주석 처리)
# listener 8883
# certfile /etc/mosquitto/certs/server.crt
# keyfile /etc/mosquitto/certs/server.key

# =================================================================
# 2. Security (보안 및 인증)
# =================================================================
# 익명 접속 원천 차단 (반드시 false로 설정)
allow_anonymous false

# 인증에 사용할 비밀번호 파일 경로 지정
password_file /mosquitto/config/passwd

# =================================================================
# 3. Performance & Resource Control (성능 및 리소스 제어)
# =================================================================
# 동시 접속 클라이언트 수 제한 (OOM 방지, 가구 시뮬레이터 고려)
max_connections 500

# 브로커 단의 디스크 영구 저장 비활성화 (Kafka가 데이터 파이프라인 역할 담당)
persistence false

# 과도한 크기의 단일 메시지 차단 (네트워크 병목 방지, 256KB 제한)
message_size_limit 262144
```

---

### (3) `config/passwd` (인증 계정)
`mosquitto_passwd` 유틸리티에 의해 해시 암호화되어 관리되는 사용자 목록입니다.

| 사용자명 (`Username`)   | 역할 / 용도                                                      |
| :---------------------- | :--------------------------------------------------------------- |
| **`simulator_user`**    | 스마트홈/IoT 센서 데이터 시뮬레이터 발행(Publish)용 계정         |
| **`kafka_bridge_user`** | MQTT 메시지를 구독(Subscribe)하여 Kafka로 전달하는 브릿지용 계정 |

---

## 3. 최초 구축 및 실행 순서

터미널 환경(**Git Bash** 또는 **Windows CMD / PowerShell**)에 맞는 명령어를 확인하여 실행하세요.

### Step 1. 비밀번호 파일 초기화 & 컨테이너 실행

- **Windows CMD (명령 프롬프트)**
  ```cmd
  type nul > config\passwd
  docker-compose up -d
  ```

- **Git Bash (MINGW64)**
  ```bash
  touch config/passwd
  docker-compose up -d
  ```

- **PowerShell**
  ```powershell
  New-Item -ItemType File -Force -Path config\passwd
  docker-compose up -d
  ```

---

### Step 2. 사용자 계정 생성

> [!NOTE]
> - **CMD / PowerShell**: 컨테이너 내부 경로(`/mosquitto/config/passwd`)를 그대로 사용합니다.
> - **Git Bash**: Git Bash의 자동 경로 변환을 피하기 위해 슬래시 2개(`//mosquitto/config/passwd`)를 사용해야 합니다.

#### [Windows CMD / PowerShell 기준]
```cmd
:: 1. simulator_user 계정 등록 (비밀번호 대화형 입력)
docker exec -it mosquitto-broker mosquitto_passwd /mosquitto/config/passwd simulator_user

:: 2. kafka_bridge_user 계정 등록
docker exec -it mosquitto-broker mosquitto_passwd /mosquitto/config/passwd kafka_bridge_user
```

*또는 비밀번호를 한 번에 인자로 전달하는 방식 (`-b` 옵션):*
```cmd
docker exec mosquitto-broker mosquitto_passwd -b /mosquitto/config/passwd simulator_user <비밀번호>
docker exec mosquitto-broker mosquitto_passwd -b /mosquitto/config/passwd kafka_bridge_user <비밀번호>
```

#### [Git Bash 기준]
```bash
# 1. simulator_user 계정 등록
docker exec -it mosquitto-broker mosquitto_passwd //mosquitto/config/passwd simulator_user

# 2. kafka_bridge_user 계정 등록
docker exec -it mosquitto-broker mosquitto_passwd //mosquitto/config/passwd kafka_bridge_user
```

---

### Step 3. 브로커 재시작 (비밀번호 반영)
비밀번호 파일이 갱신되었으므로 Mosquitto 프로세스가 이를 인식하도록 재시작합니다.

```cmd
docker restart mosquitto-broker
```

---

## 4. 동작 테스트 및 검증 방법

### 방법 A. Docker 컨테이너 내부 CLI 도구로 송수신 테스트

#### 1) 터미널 1 : 구독 (Subscribe) - 메시지 대기
```cmd
docker exec -it mosquitto-broker mosquitto_sub -t "home/sensor/#" -u "kafka_bridge_user" -P "<비밀번호>" -v
```

#### 2) 터미널 2 : 발행 (Publish) - 메시지 전송

- **CMD (명령 프롬프트)**
  ```cmd
  docker exec -it mosquitto-broker mosquitto_pub -t "home/sensor/livingroom" -u "simulator_user" -P "<비밀번호>" -m "{\"temp\": 24.5, \"humidity\": 50}"
  ```

- **Git Bash**
  ```bash
  docker exec -it mosquitto-broker mosquitto_pub -t "home/sensor/livingroom" -u "simulator_user" -P "<비밀번호>" -m '{"temp": 24.5, "humidity": 50}'
  ```

*정상 동작 시 터미널 1에 `home/sensor/livingroom {"temp": 24.5, "humidity": 50}`이 출력됩니다.*

---

### 방법 B. 익명 접속 차단 검증
비밀번호 없이 접속을 시도하여 인증 실패(`Connection Refused`)가 발생하는지 확인합니다.

```cmd
:: 인증 없이 구독 시도 -> Connection error: Connection Refused: not authorised 발생 확인
docker exec -it mosquitto-broker mosquitto_sub -t "home/#"
```

---

## 5. 컨테이너 관리 명령어 모음

| 동작                 | Windows CMD / PowerShell             | Git Bash                             |
| :------------------- | :----------------------------------- | :----------------------------------- |
| **상태 확인**        | `docker ps -f name=mosquitto-broker` | `docker ps -f name=mosquitto-broker` |
| **로그 실시간 확인** | `docker logs -f mosquitto-broker`    | `docker logs -f mosquitto-broker`    |
| **컨테이너 재시작**  | `docker restart mosquitto-broker`    | `docker restart mosquitto-broker`    |
| **컨테이너 정지**    | `docker-compose down`                | `docker-compose down`                |
| **컨테이너 시작**    | `docker-compose up -d`               | `docker-compose up -d`               |

---

## 6. 터미널별 차이점 요약

| 구분                   | Windows CMD / PowerShell                       | Git Bash (MINGW64)                          |
| :--------------------- | :--------------------------------------------- | :------------------------------------------ |
| **빈 파일 생성**       | `type nul > config\passwd`                     | `touch config/passwd`                       |
| **컨테이너 내부 경로** | `/mosquitto/config/passwd` (그대로 사용)       | `//mosquitto/config/passwd` (`//` 2개 필수) |
| **JSON 인자 따옴표**   | `"{\"key\": \"value\"}"` (큰따옴표 이스케이프) | `'{"key": "value"}'` (작은따옴표)           |

---

## 7. 스마트홈 전력 데이터 시뮬레이터 (`simulator.py`)

스마트홈 내 각 가구 및 가전기기의 실시간 소비 전력(W)을 모사하여 Mosquitto MQTT 브로커로 실시간 발행(Publish)하는 Python 프로그램입니다.

### (1) 시뮬레이션 사양
- **대상 가구**: `H001` ~ `H010` (총 10개 가구)
- **계측 지점**: 가구별 스마트 미터 / **메인 분전반 (`main`)**
- **내부 모델링 (NILM 대상 6종 가전 + 기저 부하)**:
  - 냉장고 (`fridge`), TV (`tv`), 에어컨 (`aircon`), 세탁기 (`washing_machine`), 전자레인지 (`microwave`), 조명 (`lights`)
  - 상시 기저 대기전력(25~45W) 및 센서 노이즈
- **특징**:
  - 가구 내 각 가전들의 상태 전이 및 소비 전력을 합산하여 **가구 전체 메인 분전반 총 전력(W)**을 산출 및 발행
- **발행 토픽 규칙**: `v1/power/sim/{house}/main` (예: `v1/power/sim/H001/main`)
- **전송 메시지 스키마 (JSON)**:
  ```json
  {
    "house": "H001",
    "device": "main",
    "ts": "2026-09-02T05:05:38.042Z",
    "power_w": 1245.35
  }
  ```

### (2) 시뮬레이터 환경 구축 및 실행 방법

1. **Python 가상환경 생성 (최초 1회)**
   ```bash
   python -m venv venv
   ```

2. **가상환경 활성화**
   - **Git Bash**:
     ```bash
     source venv/Scripts/activate
     ```
   - **PowerShell**:
     ```powershell
     .\venv\Scripts\Activate.ps1
     ```
   - **Windows CMD**:
     ```cmd
     venv\Scripts\activate.bat
     ```

3. **필수 라이브러리(`paho-mqtt`) 설치**
   ```bash
   pip install -r requirements.txt
   ```
   *(또는 `pip install paho-mqtt`)*

4. **시뮬레이터 구동**
   ```bash
   python simulator.py
   ```
   *1초 주기로 3개 가구 × 6개 기기(총 18건)의 전력 데이터가 브로커로 연속 발행됩니다. (종료: `Ctrl + C`)*

### (3) 시뮬레이터 데이터 실시간 수신 검증 (구독 테스트)
별도의 터미널 창에서 아래 명령어를 실행하여 시뮬레이터가 보낸 데이터가 실시간으로 들어오는지 확인합니다:
```cmd
docker exec -it mosquitto-broker mosquitto_sub -t "v1/power/sim/#" -u "kafka_bridge_user" -P "test1234" -v
```

---

## 8. 현재 구축된 데이터 흐름 (Data Flow)

현재 단계에서 구현 및 검증이 완료된 IoT 데이터 수집 흐름입니다.

```text
+-------------------------------------------------------------+
|  1. 데이터 생성 (Edge / Simulator)                          |
|  - Python 시뮬레이터 (simulator.py)                         |
|  - 가구(H001~H003) 6종 가전 전력 데이터 JSON 생성           |
+-------------------------------------------------------------+
                              │
                              │ MQTT Publish (QoS 1, Port 1883)
                              │ Auth: simulator_user
                              ▼
+-------------------------------------------------------------+
|  2. 메시지 수집 및 중계 (Broker Ingestion)                  |
|  - Mosquitto MQTT v2 Broker (Docker)                        |
|  - Topic: v1/power/sim/{house}/{device}                     |
|  - 익명 차단 및 계정 기반 인증 라우팅                       |
+-------------------------------------------------------------+
                              │
                              │ MQTT Subscribe (QoS 1)
                              │ Auth: kafka_bridge_user
                              ▼
+-------------------------------------------------------------+
|  3. 데이터 수신 및 검증 (Consumer / Pipeline Endpoint)      |
|  - 토픽(v1/power/sim/#) 구독자                              |
|  - 실시간 전력 데이터 수신 확인 완료                        |
+-------------------------------------------------------------+
```

### 단계별 상세 명세

| 단계 | 구분 | 구성 요소 | 상세 내용 |
| :--- | :--- | :--- | :--- |
| **Step 1** | **발행 (Publish)** | `simulator.py` (Python 3) | 가구별 6종 가전의 전력값(W)을 1초 주기로 JSON 직렬화하여 발행 |
| **Step 2** | **수집 (Broker)** | `mosquitto-broker` (Docker) | `v1/power/sim/{house}/{device}` 토픽으로 수신 및 인메모리 라우팅 |
| **Step 3** | **구독 (Subscribe)** | CLI / 파이프라인 수신단 | `v1/power/sim/#` 토픽을 구독하여 실시간 데이터 수신 및 검증 |



