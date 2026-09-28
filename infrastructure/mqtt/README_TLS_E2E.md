# MQTT 시뮬레이터 운영 TLS 및 E2E 연동 가이드

이 문서는 스마트홈 전력 시뮬레이터에서 생성한 데이터를 운영 환경의 MQTT, Kafka, 실시간 분석 서비스, 모니터링 서비스로 전달하여 최종 알림까지 확인하는 절차를 정리한다.

실제 사설 IP, 계정 비밀번호, 운영 인증서와 개인키는 저장소에 커밋하지 않는다. 문서의 `<A_PRIVATE_IP>`, `<MQTT_USER>` 같은 값은 실제 운영값으로 교체해서 사용한다.

## 1. 목표 데이터 흐름

```text
MQTT Simulator
  -> EC2-A Mosquitto (TCP 8883, TLS)
  -> EC2-B MQTT-Kafka Bridge
  -> Kafka power.raw.v1
  -> Realtime Analysis Service
  -> Kafka analysis.event.v1
  -> Monitoring Service Consumer
  -> Incident / Notification 생성
  -> SSE 또는 Web Push 알림
```

시뮬레이터는 다음 토픽으로 전력 데이터를 발행한다.

```text
v1/power/sim/{household_id}/main
```

MQTT-Kafka Bridge는 다음 패턴을 구독한다.

```text
v1/power/sim/+/main
```

## 2. 기존 장애 원인

운영 연동이 되지 않았던 최초 원인은 `Simulator -> Mosquitto` 구간의 운영 설정 불일치였다.

1. 운영 Mosquitto는 `8883/TLS` 리스너만 사용하지만 시뮬레이터는 기본적으로 `localhost:1883` 평문 연결을 사용했다.
2. 시뮬레이터의 `aiomqtt.Client`에 TLS Context가 전달되지 않았다.
3. 시뮬레이터 기본 계정인 `simulator_user`가 운영 Mosquitto의 `passwd`에 없었다.
4. 운영 서버 `/opt/nilm`에는 Compose와 운영 설정만 배치되며 시뮬레이터 소스와 Python 실행 환경은 배치되지 않았다.
5. 서버 인증서 SAN에는 EC2-A 사설 IP가 등록되어 있으므로 `localhost`로 접속하면 hostname 검증에 실패한다.

따라서 초기 상태에서는 MQTT에 메시지가 들어가지 않았고, Kafka 이후 구간은 검증할 수 없었다.

## 3. 현재까지 구현된 내용

### 공통 MQTT TLS 설정

`simulator/engine/tls.py`에 CLI와 웹 시뮬레이터가 함께 사용하는 TLS 설정 로직이 추가되었다.

- `MQTT_TLS_ENABLED` 파싱
- `MQTT_CA_FILE` CA 인증서 로딩
- `ssl.CERT_REQUIRED` 적용
- `check_hostname=True` 적용
- 인증서 SAN과 접속 호스트 검증
- TLS 사용 시 기본 포트 `8883`
- 평문 사용 시 기본 포트 `1883`
- 잘못된 TLS 불리언 값 거부
- CA 파일 누락, 손상, 경로 오류 사전 검증

인증서 검증을 끄는 `CERT_NONE`, `check_hostname=False` 방식은 사용하지 않는다.

### CLI 시뮬레이터

`simulator/simulator.py`는 다음 옵션을 지원한다.

```text
--host
--port, -p
--user, -u
--password
--tls / --no-tls
--ca-file
```

설정 우선순위는 다음과 같다.

```text
명시적인 CLI 인자
  -> MQTT 환경변수
  -> TLS/평문 기본값
```

별도의 TLS 설정이 없는 로컬 환경에서는 기존과 같이 `localhost:1883` 평문 연결을 사용한다.

### 웹 시뮬레이터

`simulator/web_server.py`와 `simulator/server/`에도 동일한 TLS 설정이 적용되었다.

- 웹 서버 기동 시 TLS 설정 사전 검증
- 시뮬레이션 시작 시 TLS 설정 동기 검증
- 설정 오류 발생 시 `/api/start` 오류 응답
- `/api/status`에 브로커 주소와 TLS 상태 표시
- 기본 HTTP 바인딩을 `127.0.0.1`로 제한
- `--bind-host`로 명시적인 외부 바인딩 지원

웹 제어 API에는 별도 사용자 인증이 없다. 운영 서버에서 `--bind-host 0.0.0.0`을 사용할 경우 반드시 보안 그룹에서 접근 IP를 제한해야 한다. 가능하면 기본 바인딩과 SSH 터널을 사용한다.

### 비밀번호 보호

- `MQTT_PASS` 실제 값이 `--help` 기본값으로 출력되지 않는다.
- MQTT 연결 로그와 상태 API에 비밀번호를 출력하지 않는다.
- `--password`는 프로세스 목록에 노출될 수 있으므로 운영에서는 사용하지 않는 것을 권장한다.

## 4. 현재 검증 상태

단위 테스트와 기존 시뮬레이터 회귀 테스트 결과는 다음과 같다.

```text
총 28개 테스트
27개 통과
1개 skip
```

skip된 항목은 실제 MQTT 브로커가 필요한 라이브 통합 테스트다. 따라서 아래 항목은 아직 운영 서버에서 검증해야 한다.

- 운영 CA를 사용한 TLS 핸드셰이크
- 인증서 SAN과 EC2-A 사설 IP 일치
- 운영 `simulator_user` 인증
- MQTT 메시지 발행
- MQTT-Kafka Bridge 전달
- Kafka 및 분석 서비스 처리
- Monitoring Service 이벤트 소비
- SSE 또는 Web Push 알림

## 5. 커밋 전 확인 사항

```bash
git diff --check
python -m unittest discover -s infrastructure/mqtt/simulator/tests -p "test_*.py"
git status
git diff
```

다음 파일이나 값이 Git 변경사항에 포함되지 않았는지 확인한다.

- 운영 `ca.crt`
- CA 개인키 `ca.key`
- Mosquitto `server.key`
- 운영 `.env`
- Mosquitto `passwd`
- 실제 사설 IP
- 실제 계정 비밀번호

`simulator/tests/fixtures/test_ca.crt`는 단위 테스트를 위한 공개 테스트 인증서이며 운영 CA가 아니다.

## 6. 인프라 준비 요청 사항

운영 테스트 전에 인프라 담당자가 다음 항목을 준비해야 한다.

### Mosquitto 계정

EC2-A의 `/opt/nilm/mqtt/passwd`에 시뮬레이터 전용 계정을 추가한다.

기존 `kafka_bridge_user`를 유지해야 하므로 계정을 추가할 때 `mosquitto_passwd -c`를 사용하면 안 된다. `-c`는 기존 파일을 새로 생성하여 기존 계정을 제거할 수 있다.

계정 추가 후 Mosquitto 재시작 또는 설정 재적용이 필요하다.

### 인증서와 CA

- 서버 인증서 SAN에 등록된 EC2-A 사설 IP를 실행 담당자에게 전달한다.
- 시뮬레이터 실행 계정이 읽을 수 있는 CA 파일 경로를 제공한다.
- CA, 계정 비밀번호는 저장소에 넣지 않는다.

예상 CA 위치는 다음과 같다.

| 실행 위치 | CA 파일 예시 |
|---|---|
| EC2-A | `~/mqtt-ca/ca.crt` |
| EC2-B | `/opt/nilm/mqtt/certs/ca.crt` |

EC2-B의 `/opt/nilm`은 권한이 제한될 수 있으므로 실행 계정의 읽기 권한을 별도로 확인한다.

### 실행 환경

현재 운영 배포는 시뮬레이터 소스를 `/opt/nilm`에 복사하지 않는다. 다음 중 하나를 선택해야 한다.

1. EC2-A에 저장소를 별도 checkout하고 Python venv로 실행
2. 시뮬레이터 디렉터리만 별도 배치
3. 시뮬레이터 전용 Docker 이미지를 생성하여 실행

단기 E2E 검증에는 EC2-A의 별도 checkout과 venv 방식이 가장 단순하다. 반복 실행과 자동화가 필요하면 전용 컨테이너 사용을 검토한다.

## 7. EC2-A 실행 준비

### Python 버전 및 의존성

Python 3.10 이상이 필요하다.

```bash
python3 --version
python3 -m venv .venv
source .venv/bin/activate
pip install -r infrastructure/mqtt/simulator/requirements.txt
```

### 운영 환경변수

```bash
export MQTT_HOST=<A_PRIVATE_IP>
export MQTT_PORT=8883
export MQTT_USER=<MQTT_USER>
export MQTT_TLS_ENABLED=true
export MQTT_CA_FILE="$HOME/mqtt-ca/ca.crt"

read -rsp "MQTT Password: " MQTT_PASS
printf '\n'
export MQTT_PASS
```

비밀번호를 `--password` 인자로 전달하거나 `export MQTT_PASS=실제비밀번호` 형태로 직접 입력하지 않는다. 두 방식 모두 각각 프로세스 목록 또는 shell history에 비밀번호를 남길 수 있다.

인증서 SAN에는 `<A_PRIVATE_IP>`가 등록되어 있어야 한다. 시뮬레이터를 EC2-A에서 실행하더라도 `MQTT_HOST=localhost`를 사용하지 않는다.

## 8. TLS 사전 확인

시뮬레이터 실행 전에 운영 인증서와 포트를 확인한다.

```bash
openssl s_client \
  -connect <A_PRIVATE_IP>:8883 \
  -CAfile "$MQTT_CA_FILE" \
  -verify_ip <A_PRIVATE_IP>
```

정상이라면 인증서 체인과 IP 검증이 성공해야 한다.

| 증상 | 확인할 항목 | 주 담당 |
|---|---|---|
| `Connection refused` | Mosquitto 기동, 8883 리스너, 방화벽 | 인프라 |
| 연결 timeout | EC2 보안 그룹, 라우팅, 접속 IP | 인프라 |
| `certificate verify failed` | CA 파일과 서버 인증서 발급 CA 일치 여부 | 인프라 |
| `IP address mismatch` | `MQTT_HOST`와 인증서 SAN 일치 여부 | 인프라/실행 담당 |
| 인증 실패 | `simulator_user`, 비밀번호, passwd 반영 | 인프라 |
| CA 파일을 읽을 수 없음 | 파일 경로와 실행 사용자 권한 | 인프라/실행 담당 |

## 9. 단계별 운영 검증

전체 파이프라인을 한 번에 확인하지 말고 경계를 나눠서 검증한다.

### 1단계: 짧은 MQTT 발행

```bash
python infrastructure/mqtt/simulator/simulator.py \
  --scenario peak \
  --count 3
```

확인 항목:

- TLS 설정 오류가 없는가
- MQTT 연결에 성공하는가
- 메시지 발행이 완료되는가
- Mosquitto 로그에 시뮬레이터 계정 접속이 표시되는가

이 단계가 실패하면 Kafka와 백엔드를 확인하기 전에 MQTT 문제부터 해결한다.

### 2단계: MQTT-Kafka Bridge

EC2-B의 Bridge 로그에서 다음 흐름을 확인한다.

```text
v1/power/sim/+/main 구독
  -> MQTT 메시지 수신
  -> Kafka power.raw.v1 발행
```

MQTT 발행은 성공하지만 `power.raw.v1`에 데이터가 없다면 Bridge의 MQTT 구독, Kafka 연결 및 메시지 검증 로그를 확인한다.

### 3단계: 분석 이벤트 발생

빠른 검증을 위해 루틴 누락 시나리오를 실행한다.

```bash
python infrastructure/mqtt/simulator/simulator.py \
  --scenario routine_missed \
  --hz 50
```

이 시나리오는 299개 분석 윈도우를 채운 뒤 이상 이벤트 발생 조건을 만든다.

확인 흐름:

```text
Kafka power.raw.v1 소비
  -> 299개 분석 윈도우 적재
  -> ROUTINE_MISSED 조건 충족
  -> Kafka analysis.event.v1 발행
```

### 4단계: 모니터링 이벤트 소비

Monitoring Service에서 다음 항목을 확인한다.

```text
analysis.event.v1 소비
  -> AnalysisEvent 저장
  -> Incident 생성
  -> NotificationDelivery 생성
```

### 5단계: 사용자 알림

마지막으로 다음을 구분해서 확인한다.

1. Incident가 생성됐는가
2. SSE 스트림으로 알림이 전달되는가
3. Push Subscription이 등록됐는가
4. Web Push가 활성화되어 있는가
5. 알림 응답 API가 동작하는가

`WEB_PUSH_ENABLED=false`이면 브라우저 푸시는 발생하지 않을 수 있다. 이 경우 Incident와 SSE 생성부터 먼저 검증한다.

## 10. 웹 시뮬레이터 원격 접속

웹 서버는 기본적으로 `127.0.0.1:8085`에만 바인딩된다.

EC2에서 실행한 웹 시뮬레이터를 로컬 브라우저로 확인하려면 SSH 터널을 사용한다.

```bash
ssh -i <EC2_KEY.pem> \
  -L 8085:127.0.0.1:8085 \
  ubuntu@<EC2_PUBLIC_IP>
```

EC2-A에서 웹 서버를 실행한다.

```bash
python infrastructure/mqtt/simulator/web_server.py
```

로컬 브라우저에서 다음 주소에 접속한다.

```text
http://127.0.0.1:8085
```

외부 바인딩이 반드시 필요하면 다음 옵션을 사용할 수 있다.

```bash
python infrastructure/mqtt/simulator/web_server.py --bind-host 0.0.0.0
```

이 경우 EC2 보안 그룹의 8085 인바운드를 관리자 공인 IP `/32`로 제한한다.

## 11. 담당 범위

| 구분 | 담당 업무 |
|---|---|
| 시뮬레이터 담당 | TLS 연결 코드, CLI/웹 설정, 테스트, 실행, MQTT 발행 확인, 메시지 계약 확인 |
| 인프라 담당 | Mosquitto 계정, 인증서, CA 배치, 파일 권한, 보안 그룹, 실행 공간, Mosquitto/Bridge 운영 상태 |
| 분석 서비스 담당 | `power.raw.v1` 소비, 분석 윈도우, `analysis.event.v1` 발행 |
| 모니터링 담당 | 이벤트 소비, Incident 및 Notification 생성, SSE/Web Push 처리 |

문제가 발생하면 처음 실패한 경계의 담당자가 우선 확인하고, 이전 단계의 성공 로그와 메시지 수를 다음 담당자에게 전달한다.

## 12. 완료 기준

다음 조건을 모두 충족해야 운영 E2E 연동이 완료된 것으로 판단한다.

- [ ] 시뮬레이터가 EC2-A Mosquitto의 `8883/TLS`에 연결된다.
- [ ] CA 인증서 검증에 성공한다.
- [ ] MQTT 접속 주소와 인증서 SAN 검증에 성공한다.
- [ ] 시뮬레이터 계정 인증에 성공한다.
- [ ] `v1/power/sim/{household_id}/main` 메시지가 발행된다.
- [ ] MQTT-Kafka Bridge가 메시지를 수신한다.
- [ ] Kafka `power.raw.v1`에서 데이터가 확인된다.
- [ ] Realtime Analysis Service가 데이터를 소비한다.
- [ ] Kafka `analysis.event.v1`에서 이상 이벤트가 확인된다.
- [ ] Monitoring Service가 이벤트를 소비한다.
- [ ] Incident와 NotificationDelivery가 생성된다.
- [ ] SSE 또는 Web Push로 사용자 알림을 확인한다.

## 13. 현재 상태 요약

```text
[완료] 시뮬레이터 TLS 코드 구현
[완료] CLI 및 웹 TLS 연결 적용
[완료] CA 및 SAN 검증 강제
[완료] 비밀번호 help/log 노출 방지
[완료] 웹 서버 기본 바인딩 보안 강화
[완료] 단위 및 회귀 테스트
[대기] 운영 시뮬레이터 계정 준비
[대기] 운영 CA 접근 권한 확인
[대기] 서버에 시뮬레이터 실행 환경 준비
[대기] 운영 MQTT 라이브 연결 테스트
[대기] MQTT -> Kafka -> 분석 -> 알림 E2E 검증
```
