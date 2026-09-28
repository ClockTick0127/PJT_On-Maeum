# MQTT 로컬 시뮬레이터 운영 브로커 발행

로컬 PC에서 시뮬레이터를 실행해 EC2-A의 운영 Mosquitto(8883, TLS)로 직접 발행하는 절차다. 운영 브로커는 현재 EC2-B Bridge만 접속할 수 있게 구성되어 있어, 로컬 발행에는 서버 쪽 변경 3가지와 로컬 준비가 필요하다.

| 위치 | 변경 | 이유 |
| --- | --- | --- |
| EC2-A | `simulator_user` 계정 추가 | 운영 `passwd`에 Bridge 계정만 있다 |
| EC2-A | 서버 인증서 SAN에 외부 접속 주소 추가 재발급 | 현재 SAN이 A 사설 IP만이라 hostname 검증에 실패한다 |
| EC2-A + AWS | 8883을 로컬 PC 공인 IP로 개방 | 현재 B 사설 IP만 허용한다 |
| 로컬 PC | `ca.crt` 복사, 환경변수 설정, 실행 | 시뮬레이터가 CA 검증과 hostname 검증을 강제한다 |

기준: [MQTT 인증서 및 계정 준비](MQTT_인증서_및_계정_준비.md), [mosquitto.production.conf](../../infrastructure/mqtt/config/mosquitto.production.conf), [ec2-a/compose.yaml](../../infrastructure/ec2-a/compose.yaml) `mosquitto`, [ec2-b/compose.yaml](../../infrastructure/ec2-b/compose.yaml) `mqtt-kafka-bridge`, [simulator/engine/tls.py](../../infrastructure/mqtt/simulator/engine/tls.py), [시뮬레이터 README](../../infrastructure/mqtt/simulator/README.md), [운영 TLS 및 E2E 가이드](../../infrastructure/mqtt/README_TLS_E2E.md).

실제 도메인, IP, 비밀번호는 문서와 저장소에 적지 않는다. 아래 자리표시자는 꺾쇠를 포함해 전체를 실제 값으로 치환해서 실행한다.

| 자리표시자 | 값 |
| --- | --- |
| `<APP_DOMAIN>` | EC2-A 공인 IP를 가리키는 기관 제공 호스트. A `.env`의 `APP_DOMAIN`과 같다 |
| `<A_ELASTIC_IP>` | EC2-A Elastic IP |
| `<A_PRIVATE_IP>` | EC2-A 사설 IP. 기존 서버 인증서 SAN과 B `.env`의 `MQTT_HOST` 값 |
| `<MY_PUBLIC_IP>` | 로컬 PC의 공인 IP |
| `<EC2_KEY.pem>` | EC2 접속 키 파일 경로 |

## 사전 결정

- **접속 주소** — 로컬 시뮬레이터의 `MQTT_HOST`는 `<APP_DOMAIN>`을 쓴다. MQTT 전용 도메인은 없고, 기관 제공 호스트가 이미 EC2-A를 가리킨다. 도메인은 IP로만 풀리므로 nginx(80/443)와 Mosquitto(8883)가 같은 도메인을 써도 충돌하지 않는다. 인증용 DuckDNS 도메인은 쓰지 않는다.
- **SAN 구성** — 서버 인증서 SAN에는 `IP:<A_PRIVATE_IP>`, `DNS:<APP_DOMAIN>`, `IP:<A_ELASTIC_IP>` 세 항목을 넣는다. 사설 IP는 Bridge 접속 유지에 필수다. Elastic IP를 함께 넣어두면 도메인이 바뀌어도 접속할 수 있다.
- **시뮬레이터 계정명** — `simulator_user`. 시뮬레이터 기본값이라 `MQTT_USER`를 생략해도 된다.
- **방화벽** — 8883은 `<MY_PUBLIC_IP>/32`로만 연다. `0.0.0.0/0` 개방은 TLS와 비밀번호 인증이 있어도 브로커를 인터넷 전체에 노출하므로 하지 않는다.

## 1. EC2-A: 시뮬레이터 계정 추가

`-c`를 붙이면 파일을 새로 만들어 기존 `kafka_bridge_user`가 지워진다. 붙이지 않는다. 비밀번호는 프롬프트에 두 번 입력한다.

```bash
sudo docker run --rm -it -v /opt/nilm/mqtt:/work eclipse-mosquitto:2 mosquitto_passwd /work/passwd simulator_user
```

```bash
sudo chown 1883:1883 /opt/nilm/mqtt/passwd && sudo chmod 640 /opt/nilm/mqtt/passwd
```

여기서 입력한 비밀번호가 로컬 PC의 `MQTT_PASS`다. 반영은 3단계의 Mosquitto 재시작에서 함께 처리한다.

## 2. EC2-A: 서버 인증서 재발급

기존 CA로 서명하므로 EC2-B의 `ca.crt`는 그대로 쓴다. 먼저 CA 키와 인증서가 있는지 확인한다. `ca.key`가 없으면 CA부터 다시 만들고 B의 `ca.crt`도 교체해야 하므로 [MQTT 인증서 및 계정 준비](MQTT_인증서_및_계정_준비.md) 2단계부터 다시 진행한다.

```bash
ls -l ~/mqtt-ca/ca.key ~/mqtt-ca/ca.crt
```

새 서버 키와 CSR:

```bash
cd ~/mqtt-ca && openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr -subj "/CN=nilm-mqtt"
```

SAN 확장 파일. **기존 `IP:<A_PRIVATE_IP>`를 반드시 남긴다.** 빠지면 Bridge가 접속하지 못한다.

```bash
printf "subjectAltName=IP:<A_PRIVATE_IP>,DNS:<APP_DOMAIN>,IP:<A_ELASTIC_IP>\nextendedKeyUsage=serverAuth\n" > ~/mqtt-ca/san.cnf
```

CA로 서명:

```bash
cd ~/mqtt-ca && openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 3650 -out server.crt -extfile san.cnf
```

SAN 확인. `IP Address:<A_PRIVATE_IP>`, `DNS:<APP_DOMAIN>`, `IP Address:<A_ELASTIC_IP>` 세 항목이 모두 나와야 한다.

```bash
openssl x509 -in ~/mqtt-ca/server.crt -noout -ext subjectAltName
```

## 3. EC2-A: 배치와 재시작

Mosquitto 컨테이너는 uid 1883으로 실행되므로 개인키는 1883만 읽게 한다.

```bash
sudo install -m 644 ~/mqtt-ca/server.crt /opt/nilm/mqtt/certs/server.crt && sudo install -m 640 -o 1883 -g 1883 ~/mqtt-ca/server.key /opt/nilm/mqtt/certs/server.key
```

인증서 교체는 HUP로 확실히 반영되지 않으므로 restart를 쓴다. 이 재시작이 1단계의 `passwd`도 함께 반영한다.

```bash
sudo docker compose --project-directory /opt/nilm restart mosquitto
```

EC2-B에서 Bridge가 재접속했는지 확인한다. Bridge 서비스에는 healthcheck가 없으므로 `ps`의 상태가 아니라 로그로 본다. Bridge는 1~30초 백오프로 자동 재접속한다. 1분 안에 재접속 로그가 없으면 SAN에 사설 IP가 빠진 것이다.

```bash
sudo docker compose --project-directory /opt/nilm logs --since 2m mqtt-kafka-bridge
```

배치가 끝난 임시 파일은 지운다. `ca.key`는 지우지 않는다.

```bash
shred -u ~/mqtt-ca/server.key ~/mqtt-ca/server.csr
```

Jenkins `prepare.sh`는 `passwd`와 `server.crt`, `server.key`의 존재만 검사하고 덮어쓰지 않으므로 이후 재배포에도 이 변경은 유지된다.

## 4. 네트워크 개방

로컬 PC에서 공인 IP를 확인한다.

```powershell
Invoke-RestMethod https://ifconfig.me/ip
```

EC2-A ufw에 추가한다. 기존 B 사설 IP 규칙은 그대로 둔다.

```bash
sudo ufw allow from <MY_PUBLIC_IP> to any port 8883 proto tcp && sudo ufw status numbered
```

AWS 콘솔에서 EC2-A 보안 그룹에 인바운드 규칙을 추가한다.

| 항목 | 값 |
| --- | --- |
| Type | Custom TCP |
| Port | 8883 |
| Source | `<MY_PUBLIC_IP>/32` |

Docker가 publish한 포트는 ufw를 우회하므로 실제 접근 차단은 보안 그룹이 담당한다. ufw 규칙은 기존 문서와의 일관성을 위해 함께 추가한다. 캠퍼스와 집처럼 접속 장소가 바뀌면 공인 IP가 달라지므로 ufw와 보안 그룹 두 곳을 모두 갱신한다.

## 5. 로컬 PC: CA 인증서 복사

저장소 경로에는 한글이 있어 OpenSSL이 CA 파일을 열지 못할 수 있다. 한글과 공백이 없는 경로에 둔다. `infrastructure/**/certs/`는 gitignore 대상이지만 저장소 밖이 더 안전하다.

```powershell
New-Item -ItemType Directory -Force C:\nilm
scp -i <EC2_KEY.pem> ubuntu@<A_ELASTIC_IP>:~/mqtt-ca/ca.crt C:\nilm\ca.crt
```

A의 원본과 지문이 같은지 대조한다. A에서:

```bash
openssl x509 -in ~/mqtt-ca/ca.crt -noout -fingerprint -sha256
```

로컬 PC에서. Git 동봉 OpenSSL이 PATH에 있다.

```powershell
openssl x509 -in C:\nilm\ca.crt -noout -subject -fingerprint -sha256
```

`subject=CN = nilm-mqtt-ca`가 나오고 두 Fingerprint가 같으면 완료다.

## 6. 로컬 PC: 의존성과 환경변수

Python 3.10 이상이 필요하다. 저장소의 시뮬레이터 폴더에서 설치한다.

```powershell
cd "C:\dev\특화 프로젝트 - 분산\infrastructure\mqtt\simulator"
pip install -r requirements.txt
```

PowerShell 세션마다 설정한다. `MQTT_HOST`는 2단계 SAN에 넣은 `DNS:` 문자열과 정확히 같아야 한다. 비밀번호는 화면과 히스토리에 남지 않게 SecureString으로 받는다. `--password` 인자로 넘기지 않는다.

```powershell
$env:MQTT_HOST = "<APP_DOMAIN>"
$env:MQTT_PORT = "8883"
$env:MQTT_USER = "simulator_user"
$env:MQTT_TLS_ENABLED = "true"
$env:MQTT_CA_FILE = "C:\nilm\ca.crt"
$sec = Read-Host "MQTT Password" -AsSecureString
$env:MQTT_PASS = [System.Net.NetworkCredential]::new("", $sec).Password
```

## 7. 로컬 PC: 단계별 확인 후 실행

전체를 한 번에 실행하지 않고 포트, TLS, 인증 순서로 경계를 나눠 확인한다.

포트 도달. `TcpTestSucceeded : True`가 나와야 한다.

```powershell
Test-NetConnection $env:MQTT_HOST -Port 8883
```

TLS 검증. 시뮬레이터와 같은 조건인 CA 검증과 hostname 검증을 그대로 수행한다. 성공하면 `TLS OK`와 SAN 목록이 출력된다.

```powershell
python -c "import os,ssl,socket; h=os.environ['MQTT_HOST']; c=ssl.create_default_context(cafile=os.environ['MQTT_CA_FILE']); s=c.wrap_socket(socket.create_connection((h,8883),timeout=5),server_hostname=h); print('TLS OK',s.version(),s.getpeercert()['subjectAltName'])"
```

3건만 발행해 계정 인증과 발행까지 확인한다.

```powershell
python simulator.py --scenario peak --count 3
```

성공하면 이후는 시나리오만 바꾼다. 웹 대시보드는 로컬에서 실행되므로 SSH 터널이 필요 없다.

| 목적 | 명령 |
| --- | --- |
| 10초 피크 시연 (60초) | `python simulator.py --scenario peak` |
| 루틴 누락 빠른 검증 | `python simulator.py --scenario routine_missed --hz 50` |
| 연속 부하 100가구 | `python simulator.py -n 100 -q` |
| 웹 대시보드 | `python web_server.py` 후 브라우저 `http://127.0.0.1:8085` |

## 8. EC2-B: 도달 확인

Bridge 로그에서 MQTT 수신과 Kafka 발행을 확인한다.

```bash
sudo docker compose --project-directory /opt/nilm logs -f mqtt-kafka-bridge
```

Kafka 토픽에서 직접 확인한다.

```bash
sudo docker compose --project-directory /opt/nilm exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic power.raw.v1 --max-messages 5
```

Kafka 이후의 분석 서비스, 모니터링, 알림 확인은 [운영 TLS 및 E2E 가이드](../../infrastructure/mqtt/README_TLS_E2E.md) 9단계를 따른다.

## 트러블슈팅

| 증상 | 원인 | 조치 |
| --- | --- | --- |
| `TcpTestSucceeded : False` | 보안 그룹 미개방 또는 내 공인 IP 변경 | 4단계 보안 그룹과 ufw 재확인 |
| `certificate verify failed` | 로컬 `ca.crt`가 원본과 다름 또는 잘림 | 5단계 지문 대조 후 재복사 |
| `Hostname mismatch` 또는 `IP address mismatch` | `MQTT_HOST`가 SAN에 없음 | 2단계 SAN 출력과 `MQTT_HOST` 문자열 비교 |
| 연결은 되나 `Not authorized` | `simulator_user` 미반영 또는 비밀번호 오류 | 1단계 후 3단계 restart 여부 확인 |
| `MQTT CA 인증서 파일을 열 수 없습니다` | 경로에 한글이나 공백 | `C:\nilm\ca.crt`처럼 ASCII 경로 사용 |
| 재발급 뒤 Bridge 재접속 실패 | 새 SAN에서 `IP:<A_PRIVATE_IP>` 누락 | 사설 IP 포함해 2단계 재발급 후 restart |
| 발행은 성공하나 Kafka에 데이터 없음 | Bridge 구독 또는 Kafka 연결 문제 | 8단계 Bridge 로그 확인 |

## 마무리

- `~/mqtt-ca/ca.key`는 `chmod 600`으로 두고 잃지 않는다. 이후 추가 SAN 변경이나 기기 인증서 발급에 필요하다.
- 로컬 `C:\nilm\ca.crt`는 공개 인증서라 유출 위험은 없지만 저장소에 커밋하지 않는다.
- 접속 장소가 바뀌면 4단계의 공인 IP 규칙만 갱신한다. 인증서와 계정은 그대로다.
- 커밋 전에 `git status`로 `ca.crt`, `.env`, 실제 IP·도메인·비밀번호가 변경사항에 포함되지 않았는지 확인한다.
