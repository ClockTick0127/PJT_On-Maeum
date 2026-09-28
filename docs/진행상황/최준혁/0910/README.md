# 2026-09-10 진행 상황 공유

> **작성자**: 최준혁 (Data / Pipeline)<br>
> **주요 내용**: 시뮬레이터 구조 고도화 및 웹 제어 기능 확장, 운영 EC2 Mosquitto TLS 연결 지원, 자동 테스트와 운영 E2E 가이드 작성

---

## 1. 금일 작업 요약

기존 단일 파일 중심의 시뮬레이터를 엔진과 웹 서버 패키지로 분리하고, 기존 동작을 보존하는 회귀 테스트와 실제 MQTT·SSE 통합 테스트를 추가했다. 이후 운영 환경에서 시뮬레이터 데이터가 MQTT 브로커에 전달되지 않는 원인을 분석하고, 시뮬레이터가 운영 Mosquitto의 `8883/TLS` 리스너에 안전하게 연결할 수 있도록 코드를 수정했다.

주요 작업은 다음과 같다.

- `simulator.py`의 전력 모델, 상태, 발행 책임을 `engine/` 패키지로 분리
- `web_server.py`의 실행 관리와 HTTP 요청 처리를 `server/` 패키지로 분리
- 기존 공개 심볼과 고정 시드 출력이 유지되는 호환 Facade 및 회귀 테스트 구축
- 웹 뷰어에 6대 가전 실시간 수동 제어와 MQTT·SSE 동기화 검증 추가
- CLI 및 웹 시뮬레이터에 공통 MQTT TLS 설정 모듈 추가
- CA 인증서와 서버 인증서 SAN을 검증하는 `SSLContext` 적용
- CLI 인자, 환경변수, 기본값의 설정 우선순위 정리
- TLS 활성화 여부에 따라 `8883` 또는 `1883` 포트를 자동 선택하도록 개선
- 비밀번호가 도움말, 로그, 상태 API에 노출되지 않도록 보안 강화
- 웹 제어 서버의 기본 바인딩을 `127.0.0.1`로 제한
- TLS 설정 및 기존 시뮬레이터 동작에 대한 자동 테스트 추가
- 운영 EC2에서 MQTT부터 Kafka, 분석 이벤트, 사용자 알림까지 확인할 수 있는 E2E 절차 문서화

운영 계정 발급, CA 파일 권한 설정, 서버 실행 환경 배치는 인프라 준비가 필요한 항목이므로 실제 운영 E2E 테스트는 후속 작업으로 남겨 두었다.

---

## 2. 문제 상황 및 원인 분석

목표 데이터 흐름은 다음과 같다.

```text
MQTT Simulator
  -> EC2-A Mosquitto
  -> EC2-B MQTT-Kafka Bridge
  -> Kafka power.raw.v1
  -> Realtime Analysis Service
  -> Kafka analysis.event.v1
  -> Monitoring Service
  -> Incident / Notification
  -> SSE 또는 Web Push 알림
```

기존에는 첫 구간인 `Simulator -> EC2-A Mosquitto` 연결부터 실패하고 있었다. 확인된 원인은 다음과 같다.

| 구분 | 기존 상태 | 운영 환경 요구사항 | 영향 |
| :--- | :--- | :--- | :--- |
| 포트 및 프로토콜 | `localhost:1883` 평문 연결 | `8883/TLS` 전용 리스너 | TCP 또는 TLS 연결 실패 |
| TLS 클라이언트 설정 | `aiomqtt.Client`에 TLS Context 없음 | 운영 CA를 이용한 인증서 검증 필요 | 8883 포트를 사용해도 평문 핸드셰이크 시도 |
| MQTT 계정 | `simulator_user / test1234` 기본값 | 운영 Mosquitto에 등록된 계정 필요 | 사용자 인증 실패 |
| 서버 실행 환경 | 운영 서버에 시뮬레이터 소스와 의존성 없음 | Python 3.10 이상 및 `aiomqtt` 필요 | 서버에서 시뮬레이터 실행 불가 |
| 인증서 SAN | 기본 접속 주소가 `localhost` | EC2-A 사설 IP가 SAN에 등록됨 | CA 검증 시 hostname 불일치 |

따라서 Kafka, 분석 서비스, 알림 서비스의 문제가 아니라 MQTT 발행 이전 단계에서 연결 조건이 맞지 않는 것이 최초 장애 원인이었다.

---

## 3. 시뮬레이터 고도화

9월 10일 16:03에 반영된 `26710ec` 커밋에서는 TLS 작업에 앞서 시뮬레이터의 구조와 테스트 체계를 전반적으로 개선했다.

### (1) 단일 파일 구조를 책임별 패키지로 분리

기존 `simulator.py`와 `web_server.py`에 집중되어 있던 로직을 다음과 같이 분리했다.

```text
simulator.py                         web_server.py
  └─ CLI 및 호환 Facade               └─ 인자 처리 및 서버 실행
       │                                    │
       ▼                                    ▼
engine/                              server/
  ├─ config.py                         ├─ config.py
  ├─ profiles.py                       ├─ manager.py
  ├─ state.py                          └─ request_handler.py
  ├─ power_model.py
  └─ publisher.py
```

| 모듈 | 분리한 책임 |
| :--- | :--- |
| `engine/config.py` | 시뮬레이터 기본 설정과 MQTT 설정 관리 |
| `engine/profiles.py` | 6대 가전의 소비전력, 역률, 기동 특성 및 듀티 사이클 정의 |
| `engine/state.py` | 가구별 환경·가전 상태 초기화와 수동 ON/OFF 상태 관리 |
| `engine/power_model.py` | 피크 시나리오, 환경 변화, 가전 부하 및 메인 분전반 계측값 계산 |
| `engine/publisher.py` | MQTT 토픽과 이중 호환 JSON 페이로드 발행 |
| `server/manager.py` | 워커 생명주기, 모드 전환, 동시성 락, MQTT·SSE 데이터 관리 |
| `server/request_handler.py` | 정적 화면, REST 제어 API 및 SSE 스트림 요청 처리 |

이 구조로 CLI, 물리 엔진, MQTT 발행, 웹 요청 처리 간 결합도를 낮추고 기능별 수정과 테스트가 가능해졌다.

### (2) 기존 호환성을 유지하는 Facade 구성

구조를 분리하면서 기존 코드가 `simulator.py`에서 가져오던 공개 심볼과 공유 상태 객체를 계속 사용할 수 있도록 호환 Facade를 유지했다.

- 기존 import 경로와 공개 함수 유지
- Facade와 엔진 패키지가 동일한 상태 객체를 참조하도록 보장
- 수동 가전 ON/OFF의 멱등성과 상태 정리 검증
- 랜덤 동작 비활성화 시 불변 조건 검증
- 기존 메시지 필드, 자료형 및 물리량 계산 결과 유지
- 고정 시드 기준 48스텝 출력값을 fixture로 저장해 변경 전후 결과 비교

이를 통해 내부 구조를 크게 바꾸면서도 기존 CLI, 웹 뷰어, MQTT 페이로드 동작이 달라지는 회귀 문제를 방지했다.

### (3) 웹 뷰어 및 제어 서버 기능 고도화

웹 뷰어와 서버를 실제 시뮬레이션 제어 및 통합 검증이 가능한 형태로 확장했다.

- 전기포트, 인덕션, 다리미, 전자레인지, 헤어드라이기, 진공청소기 6대의 실시간 수동 ON/OFF 제어
- 화면 조작 결과를 물리 엔진 상태와 실제 MQTT 발행 데이터에 반영
- MQTT에 발행한 한 틱의 데이터와 SSE 차트 데이터가 동일한지 검증 가능한 구조 적용
- 인덕션의 전체 가열·휴지 듀티 사이클과 상태 전이 지원
- `manual`, `peak`, `routine_missed` 모드 간 잘못된 가전 제어 요청 차단
- 빠른 재시작 상황에서도 중복 워커가 생성되지 않도록 생명주기와 동시성 제어 강화
- 루틴 누락 시나리오 및 6대 가전 상태를 뷰어에서 식별할 수 있도록 UI 개선

### (4) 시뮬레이터 테스트 체계 구축

고도화 작업과 함께 `tests/` 패키지와 회귀 기준 데이터를 추가했다.

| 테스트 | 검증 범위 |
| :--- | :--- |
| `test_simulator_compatibility.py` | 공개 심볼, 공유 상태, 수동 제어, 물리 계측 스키마, 인덕션 듀티 사이클, 고정 시드 기준선 등 9개 비네트워크 회귀 테스트 |
| `simulator_seed42_baseline.json` | 구조 개편 이전과 동일한지 비교하기 위한 48스텝 결정론적 출력 fixture |
| `test_manual_live_integration.py` | 실제 Mosquitto와 웹 서버를 사용한 MQTT·SSE 동일성, 인덕션 상태 전이, 모드 충돌, 빠른 재시작 등 4개 라이브 통합 테스트 |

실제 MQTT 브로커가 필요한 라이브 테스트는 기존 서비스를 임의로 재시작하지 않고, 브로커에 접속할 수 없는 환경에서는 안전하게 건너뛰도록 구성했다.

---

## 4. 운영 MQTT TLS 상세 구현 내용

### (1) 공통 MQTT TLS 모듈 추가

`infrastructure/mqtt/simulator/engine/tls.py`를 추가하여 CLI와 웹 시뮬레이터가 동일한 연결 설정을 사용하도록 통합했다.

구현 항목:

- `MQTT_TLS_ENABLED`의 `true/false`, `1/0`, `yes/no`, `on/off` 파싱
- 알 수 없는 TLS 설정값을 묵인하지 않고 `ValueError`로 거부
- `MQTT_CA_FILE` 경로 및 파일 형식 사전 검증
- `ssl.CERT_REQUIRED`와 `check_hostname=True`를 적용한 `SSLContext` 생성
- CA 파일 누락, 빈 파일, 디렉터리 지정, 손상된 PEM 인증서 오류 처리
- MQTT 연결 설정을 하나의 설정 객체로 통합

인증서 검증을 우회하는 `ssl.CERT_NONE` 또는 `check_hostname=False` 방식은 적용하지 않았다.

### (2) 설정 우선순위 및 포트 자동 결정

MQTT 접속 설정은 다음 순서로 결정된다.

```text
명시적인 CLI 인자
  -> MQTT 환경변수
  -> TLS/평문 기본값
```

포트는 다음 우선순위를 적용했다.

```text
--port
  -> MQTT_PORT
  -> TLS 활성화 시 8883
  -> 평문 연결 시 1883
```

덕분에 운영 환경에서는 TLS를 활성화하면 기본적으로 `8883`을 사용하고, TLS 설정이 없는 기존 로컬 환경에서는 `localhost:1883` 동작을 유지한다.

### (3) CLI 시뮬레이터 TLS 지원

`simulator.py`에 다음 옵션과 환경변수 연동을 추가했다.

| CLI 옵션 | 환경변수 | 용도 |
| :--- | :--- | :--- |
| `--host` | `MQTT_HOST` | MQTT 브로커 주소 |
| `--port`, `-p` | `MQTT_PORT` | MQTT 브로커 포트 |
| `--user`, `-u` | `MQTT_USER` | MQTT 인증 계정 |
| `--password` | `MQTT_PASS` | MQTT 인증 비밀번호 |
| `--tls` / `--no-tls` | `MQTT_TLS_ENABLED` | TLS 연결 활성화 여부 |
| `--ca-file` | `MQTT_CA_FILE` | CA 인증서 파일 경로 |

`aiomqtt.Client` 생성 시 검증된 TLS Context를 전달하도록 수정하여 운영 Mosquitto와 실제 TLS 핸드셰이크를 수행할 수 있게 했다.

### (4) 웹 시뮬레이터 TLS 및 오류 처리

`web_server.py`와 `server/manager.py`, `server/request_handler.py`에도 동일한 MQTT 설정을 적용했다.

- 웹 서버 기동 시 TLS 및 CA 설정 사전 검증
- `/api/start` 요청 시에도 TLS 설정을 동기 검증
- 잘못된 CA 경로 또는 TLS 설정이면 HTTP 오류 응답으로 즉시 반환
- 웹 시뮬레이터의 MQTT 클라이언트에도 TLS Context 전달
- `/api/status`에서 브로커 주소와 TLS 활성화 상태 확인 지원
- 비밀번호는 상태 응답과 로그에서 제외

웹 제어 API에는 별도 사용자 인증이 없으므로 기본 HTTP 바인딩 주소를 `0.0.0.0`에서 `127.0.0.1`로 변경했다. EC2에서 웹 UI를 확인할 때는 기본 바인딩을 유지하고 SSH 터널을 사용하는 방식을 권장하도록 문서화했다.

### (5) 인증정보 보호

운영 비밀번호 노출을 방지하기 위해 다음 사항을 반영했다.

- `--password`의 CLI 파서 기본값을 `None`으로 변경
- 실제 `MQTT_PASS` 값이 `--help` 출력에 포함되지 않도록 처리
- 로그와 상태 API에서 비밀번호 출력 금지
- 운영 실행 시 `--password` 직접 입력 대신 `read -rsp`와 환경변수 사용 권장
- 저장소 문서에는 실제 IP, 계정, 비밀번호, 운영 인증서를 기록하지 않고 placeholder 사용

### (6) 웹 서버 접근 범위 보안 강화

웹 제어 API(`/api/start`, `/api/stop`, `/api/device`)는 인증 기능이 없기 때문에 외부에 그대로 노출하면 제3자가 임의로 시뮬레이터를 실행할 수 있다.

이에 따라:

- 기본 바인딩을 `127.0.0.1`로 제한
- 외부 접속은 SSH 포트 포워딩 사용 권장
- `--bind-host 0.0.0.0` 사용 시 EC2 보안 그룹에서 관리자 공인 IP `/32`만 허용하도록 안내

---

## 5. 테스트 및 검증

### 자동 테스트

`test_mqtt_tls.py`와 테스트용 공개 CA 인증서를 추가했다.

주요 검증 항목:

- TLS 환경변수 정상값 및 잘못된 값 파싱
- CLI 포트와 `MQTT_PORT`의 우선순위
- TLS/평문 기본 포트 선택
- 엄격한 인증서 검증을 사용하는 SSL Context 생성
- CA 파일 누락, 손상 및 잘못된 경로 처리
- CLI와 웹 MQTT 클라이언트에 TLS Context 전달
- 웹 시뮬레이터 시작 전 동기 오류 반환
- 비밀번호의 도움말, 로그 및 상태 응답 비노출
- 웹 서버 바인딩 옵션 검증
- 기존 시뮬레이터 물리 엔진 및 고정 시드 결과 회귀 검증

비네트워크 TLS 및 회귀 테스트 28개를 통과했다. 실제 MQTT 브로커가 필요한 라이브 통합 테스트는 브로커가 없는 환경에서 자동으로 skip되도록 정리했다.

### 현재 검증 범위

```text
[완료] TLS 설정 파싱 및 포트 결정 단위 테스트
[완료] CA 파일과 SSLContext 검증 테스트
[완료] CLI·웹 MQTT 클라이언트 TLS 전달 테스트
[완료] 기존 로컬 평문 동작에 대한 회귀 검증
[완료] 문서 및 Git whitespace 검사
[대기] 운영 CA를 사용한 EC2-A TLS 핸드셰이크
[대기] 운영 MQTT 계정 인증 및 실제 메시지 발행
[대기] MQTT -> Kafka -> 분석 -> 알림 전체 E2E 검증
```

코드가 TLS 연결을 지원하도록 구현된 것과 실제 운영 인프라에서 연결이 성공한 것은 별개의 완료 조건이다. 운영 E2E 완료로 오해되지 않도록 두 상태를 구분했다.

---

## 6. 변경 파일 요약

| 파일 경로 | 구분 | 작업 내용 |
| :--- | :--- | :--- |
| `infrastructure/mqtt/simulator/engine/profiles.py` | 신규 | 6대 가전 전력 특성과 듀티 사이클 프로필 분리 |
| `infrastructure/mqtt/simulator/engine/state.py` | 신규 | 가구별 시뮬레이션 상태 및 수동 제어 관리 |
| `infrastructure/mqtt/simulator/engine/power_model.py` | 신규 | 환경, 가전 부하 및 메인 분전반 물리량 계산 분리 |
| `infrastructure/mqtt/simulator/engine/publisher.py` | 신규 | MQTT 토픽과 이중 호환 페이로드 발행 분리 |
| `infrastructure/mqtt/simulator/engine/tls.py` | 신규 | 공통 TLS 파서, 포트 결정, 설정 해석, SSL Context 생성 |
| `infrastructure/mqtt/simulator/engine/__init__.py` | 신규·수정 | 엔진 공개 API와 TLS 공통 기능 제공 |
| `infrastructure/mqtt/simulator/engine/config.py` | 신규·수정 | 기본 설정과 환경변수 기반 MQTT 설정 관리 |
| `infrastructure/mqtt/simulator/server/config.py` | 신규 | 웹 서버 기본 포트와 설정 분리 |
| `infrastructure/mqtt/simulator/server/manager.py` | 신규·수정 | 워커·모드·동시성·SSE 상태 관리 및 TLS 연결 적용 |
| `infrastructure/mqtt/simulator/server/request_handler.py` | 신규·수정 | REST/SSE 요청 처리 및 TLS 설정 오류 응답 |
| `infrastructure/mqtt/simulator/simulator.py` | 수정 | 호환 Facade, CLI 진입점, TLS 옵션과 `aiomqtt` 연결 적용 |
| `infrastructure/mqtt/simulator/web_server.py` | 수정 | 경량 실행 진입점, TLS 사전 검증, 바인딩 보안 강화 |
| `infrastructure/mqtt/simulator/waveform_viewer.html` | 수정 | 6대 가전 수동 제어와 실시간 상태 UI 개선 |
| `infrastructure/mqtt/simulator/tests/test_simulator_compatibility.py` | 신규 | 공개 API, 상태, FSM, 물리량 및 기준선 회귀 테스트 |
| `infrastructure/mqtt/simulator/tests/fixtures/simulator_seed42_baseline.json` | 신규 | 고정 시드 48스텝 회귀 검증 기준 데이터 |
| `infrastructure/mqtt/simulator/tests/test_mqtt_tls.py` | 신규 | TLS, 포트, 보안 및 웹 처리 단위 테스트 |
| `infrastructure/mqtt/simulator/tests/fixtures/test_ca.crt` | 신규 | 단위 테스트용 공개 CA 인증서 |
| `infrastructure/mqtt/simulator/tests/test_manual_live_integration.py` | 신규·수정 | MQTT·SSE·FSM·재시작 라이브 검증 및 브로커 미가동 시 안전한 skip 처리 |
| `infrastructure/mqtt/README.md` | 수정 | 운영 TLS 실행 방법과 보안 주의사항 반영 |
| `infrastructure/mqtt/simulator/README.md` | 수정 | CLI·웹 환경별 TLS 실행 가이드 반영 |
| `infrastructure/mqtt/README_TLS_E2E.md` | 신규 | 운영 MQTT부터 최종 알림까지 단계별 E2E 가이드 |

---

## 7. 담당 범위 정리

이번 작업에서 시뮬레이터 담당 범위와 인프라 담당 범위를 다음과 같이 구분했다.

| 담당 | 책임 범위 |
| :--- | :--- |
| 시뮬레이터 담당 | TLS 연결 코드, CLI·웹 설정, 자동 테스트, 시뮬레이터 실행, MQTT 발행 확인, 페이로드 계약 확인 |
| 인프라 담당 | 운영 Mosquitto 계정 추가, 인증서와 CA 배치, 파일 권한, 보안 그룹, 서버 실행 공간, Mosquitto 및 Bridge 상태 확인 |
| 분석 서비스 담당 | `power.raw.v1` 소비, 분석 윈도우 처리, `analysis.event.v1` 발행 |
| 모니터링 담당 | 분석 이벤트 소비, Incident 및 Notification 생성, SSE/Web Push 처리 |

시뮬레이터 담당자는 운영 계정과 인증서를 직접 생성하거나 운영 서버 권한을 변경하는 대신, 필요한 조건을 인프라 담당자에게 요청하고 제공받은 접속정보로 MQTT 발행까지 검증한다.

---

## 8. 다음 진행 예정 사항

### (1) 인프라 준비 요청

- EC2-A Mosquitto에 시뮬레이터 전용 계정 추가
- 기존 `kafka_bridge_user`가 삭제되지 않도록 `mosquitto_passwd -c` 사용 금지
- 서버 인증서 SAN에 등록된 EC2-A 사설 IP 확인
- 시뮬레이터 실행 계정이 읽을 수 있는 CA 파일 경로 제공
- Python 3.10 이상, 소스 코드 및 `aiomqtt` 실행 환경 준비

### (2) 운영 MQTT 단독 검증

- `openssl s_client`로 CA와 SAN 검증
- 시뮬레이터를 짧은 횟수(`--count 3`)로 실행
- Mosquitto 로그에서 TLS 접속, 계정 인증, MQTT 발행 확인

### (3) 단계별 E2E 검증

1. MQTT-Kafka Bridge가 `v1/power/sim/+/main` 메시지를 수신하는지 확인
2. Kafka `power.raw.v1`에 시뮬레이터 데이터가 적재되는지 확인
3. `routine_missed --hz 50`으로 분석 윈도우를 빠르게 충족
4. Kafka `analysis.event.v1`에서 이상 이벤트 확인
5. Monitoring Service의 Incident 및 Notification 생성 확인
6. SSE 또는 Web Push를 통한 사용자 알림 확인

---

## 9. 형상 관리 결과

- 시뮬레이터 고도화 커밋: `26710ec` (`Feature/data/simulator`)
- 1차 `develop` 머지 커밋: `73b71ff` (2026-09-10 16:03 KST)
- 운영 MQTT TLS 지원 커밋: `cfa8f0e` (`feat(data) : 운영 mqtt tls 연결 지원`)
- Merge Request: `!25`
- TLS 작업 `develop` 머지 커밋: `4a46eb3`
- TLS 작업이 자정을 넘겨 커밋 및 머지는 2026-09-11 00:30~00:32 KST에 완료됨

---

## 10. 최종 상태

```text
[완료] 시뮬레이터 엔진 및 웹 서버 책임별 모듈 분리
[완료] 기존 공개 API와 고정 시드 출력 호환성 유지
[완료] 6대 가전 웹 수동 제어 및 MQTT·SSE 동기화 구조 개선
[완료] 비네트워크 회귀 테스트 및 실브로커 통합 테스트 체계 구축
[완료] 운영 MQTT TLS 연결을 위한 시뮬레이터 코드 구현
[완료] CLI 및 웹 시뮬레이터 공통 설정 적용
[완료] CA 및 SAN 검증 강제
[완료] 비밀번호 및 웹 제어 API 보안 강화
[완료] 자동 테스트와 운영 E2E 문서 작성
[완료] feature/data/simulator 작업 develop 머지
[대기] 운영 시뮬레이터 계정 및 CA 접근 권한 준비
[대기] EC2-A에서 실제 MQTT TLS 발행 검증
[대기] Kafka, 분석 서비스, 모니터링 및 최종 알림 E2E 검증
```
