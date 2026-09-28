# MQTT 인증서 및 계정 준비

EC2-A의 Mosquitto(8883, TLS)와 EC2-B의 Bridge가 통신하려면 배포 전에 사람이 직접 만들어야 하는 파일이 4개 있다. Jenkins `prepare.sh`는 이 파일들의 존재만 검사하고 생성하지 않는다. 없으면 Prepare 단계에서 배포가 중단된다.

| 서버 | 경로 | 용도 |
| --- | --- | --- |
| EC2-A | `/opt/nilm/mqtt/certs/server.crt` | Mosquitto 서버 인증서 |
| EC2-A | `/opt/nilm/mqtt/certs/server.key` | Mosquitto 서버 개인키 |
| EC2-A | `/opt/nilm/mqtt/passwd` | Bridge 로그인 계정 파일 |
| EC2-B | `/opt/nilm/mqtt/certs/ca.crt` | Bridge가 서버 인증서를 검증할 CA |

nginx용 Let's Encrypt 인증서(`/opt/nilm/certs/`)와는 별개다. MQTT는 자체 CA로 발급하며 공인 인증서가 필요 없다. Bridge는 `ca.crt`로 서버를 검증하고, `MQTT_HOST`와 인증서 SAN이 일치해야 연결된다.

기준: [ec2-a/compose.yaml](../../infrastructure/ec2-a/compose.yaml) `mosquitto`, [ec2-b/compose.yaml](../../infrastructure/ec2-b/compose.yaml) `mqtt-kafka-bridge`, [mosquitto.production.conf](../../infrastructure/mqtt/config/mosquitto.production.conf), [prepare.sh](../../infrastructure/scripts/prepare.sh).

## 사전 결정

- **B가 A에 접속할 주소** — 이 값이 서버 인증서 SAN과 B `.env`의 `MQTT_HOST`에 똑같이 들어간다. 같은 VPC면 A의 사설 IP를 쓴다. 이 문서의 `<A_PRIVATE_IP>`는 모두 이 값으로 바꿔 실행한다(꺾쇠 포함 전체를 치환). 실제 IP를 문서나 저장소에 적지 않는다. A에서 확인:

```bash
hostname -I | awk '{print $1}'
```

- **Bridge 계정명** — 예시는 `kafka_bridge_user`. B `.env`의 `MQTT_USER`와 같아야 한다.

## 1. EC2-A: 디렉터리 준비

`prepare.sh`가 `jenkins-agent`로 `/opt/nilm/mqtt/mosquitto.conf`를 쓰므로 `mqtt/`는 Agent 소유여야 한다.

```bash
sudo install -d -m 755 -o jenkins-agent -g jenkins-agent /opt/nilm/mqtt && sudo install -d -m 755 /opt/nilm/mqtt/certs
```

## 2. EC2-A: 자체 CA 생성

작업 폴더는 홈에 둔다. `ca.key`는 기기 인증서 발급·재발급에 필요하므로 보관한다.

```bash
mkdir -p ~/mqtt-ca && chmod 700 ~/mqtt-ca && cd ~/mqtt-ca
```

```bash
openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -keyout ca.key -out ca.crt -subj "/CN=nilm-mqtt-ca"
```

## 3. EC2-A: 서버 인증서 발급

서버 키와 CSR:

```bash
openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr -subj "/CN=nilm-mqtt"
```

SAN 확장 파일. `IP:`에는 사전 결정한 접속 주소를 넣는다. 도메인으로도 붙일 계획이면 `IP:<A_PRIVATE_IP>,DNS:<MQTT_DNS_NAME>`처럼 콤마로 추가한다.

```bash
printf "subjectAltName=IP:<A_PRIVATE_IP>\nextendedKeyUsage=serverAuth\n" > san.cnf
```

CA로 서명:

```bash
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 3650 -out server.crt -extfile san.cnf
```

SAN 확인. `IP Address:<A_PRIVATE_IP>`가 나와야 한다.

```bash
openssl x509 -in server.crt -noout -ext subjectAltName
```

## 4. EC2-A: 인증서 배치

Mosquitto 컨테이너는 uid 1883으로 실행되므로 개인키는 1883만 읽게 한다.

```bash
sudo install -m 644 ~/mqtt-ca/server.crt /opt/nilm/mqtt/certs/server.crt && sudo install -m 640 -o 1883 -g 1883 ~/mqtt-ca/server.key /opt/nilm/mqtt/certs/server.key
```

## 5. EC2-A: 계정 파일 생성

`mosquitto_passwd`를 별도 설치하지 않고 `eclipse-mosquitto:2` 이미지 안의 도구를 1회 실행한다. 프로젝트 배포와 무관하며 Docker만 있으면 된다. 비밀번호는 프롬프트에 두 번 입력한다(명령줄에 남기지 않는다).

```bash
sudo docker run --rm -it -v /opt/nilm/mqtt:/work eclipse-mosquitto:2 mosquitto_passwd -c /work/passwd kafka_bridge_user
```

```bash
sudo chown 1883:1883 /opt/nilm/mqtt/passwd && sudo chmod 640 /opt/nilm/mqtt/passwd
```

여기서 입력한 비밀번호가 B `.env`의 `MQTT_PASS`다.

계정을 추가할 때는 `-c`를 빼고 실행한다(`-c`는 파일을 새로 만들어 기존 계정을 지운다).

```bash
sudo docker run --rm -it -v /opt/nilm/mqtt:/work eclipse-mosquitto:2 mosquitto_passwd /work/passwd <추가할_계정>
```

## 6. EC2-A: 최종 확인

```bash
sudo ls -laR /opt/nilm/mqtt
```

```text
/opt/nilm/mqtt/              jenkins-agent  755
/opt/nilm/mqtt/passwd        1883:1883      640
/opt/nilm/mqtt/certs/        root           755
/opt/nilm/mqtt/certs/server.crt  root       644
/opt/nilm/mqtt/certs/server.key  1883:1883  640
```

`jenkins-agent`는 `test -s`로 존재만 확인하므로 개인키 읽기 권한이 없어도 Prepare는 통과한다. `/opt/nilm` 아래는 `ubuntu`로 볼 수 없으니 확인 명령에는 항상 `sudo`를 붙인다.

## 7. EC2-B: CA 인증서 복사

`ca.crt`는 공개 인증서라 내용을 그대로 붙여넣어도 된다.

A에서 출력:

```bash
cat ~/mqtt-ca/ca.crt
```

B에서 디렉터리 준비 후 붙여넣기. 전체(`-----BEGIN CERTIFICATE-----`부터 `-----END CERTIFICATE-----`까지) 붙인 뒤 `Ctrl+D`.

```bash
sudo install -d -m 755 -o jenkins-agent -g jenkins-agent /opt/nilm/mqtt && sudo install -d -m 755 /opt/nilm/mqtt/certs && sudo tee /opt/nilm/mqtt/certs/ca.crt > /dev/null
```

```bash
sudo chmod 644 /opt/nilm/mqtt/certs/ca.crt
```

B의 `/opt/nilm` 자체가 root 소유로 만들어졌다면 Agent 소유로 바꾼다. 그렇지 않으면 B의 Prepare가 `test -w /opt/nilm`에서 실패한다.

```bash
ls -ld /opt/nilm
```

```bash
sudo chown jenkins-agent:jenkins-agent /opt/nilm && sudo chmod 750 /opt/nilm
```

## 8. EC2-B: 복사 검증

파일이 정상 인증서인지, A의 원본과 같은지 확인한다.

```bash
sudo openssl x509 -in /opt/nilm/mqtt/certs/ca.crt -noout -subject -fingerprint -sha256
```

A에서 같은 명령을 원본에 실행한다.

```bash
openssl x509 -in ~/mqtt-ca/ca.crt -noout -fingerprint -sha256
```

`subject=CN = nilm-mqtt-ca`가 나오고 두 Fingerprint가 같으면 완료다. `Unable to load certificate`면 붙여넣기가 잘린 것이니 7단계의 `tee` 명령을 다시 실행한다.

## 9. B `.env` 반영 → Jenkins 재업로드

```dotenv
MQTT_HOST=<A_PRIVATE_IP>
MQTT_PORT=8883
MQTT_USER=kafka_bridge_user
MQTT_PASS=<5단계에서 입력한 비밀번호>
MQTT_CA_FILE=/etc/mqtt/ca.crt
```

`MQTT_HOST`는 3단계 SAN과 문자열이 정확히 같아야 한다. Bridge는 `CERT_REQUIRED`로 호스트를 검증하므로 다르면 연결이 거부된다. 수정 후 Jenkins Credentials `ec2-b-runtime-env`를 **Update → Replace 체크 → 파일 선택**으로 교체한다.

## 10. 네트워크

같은 포트가 두 곳 모두 열려야 통신된다.

| 서버 | ufw | AWS 보안 그룹 |
| --- | --- | --- |
| EC2-A | `sudo ufw allow from <B사설IP> to any port 8883 proto tcp` | 8883 인바운드, Source = B 보안 그룹 |

Bridge가 A에 접속하는 방향이므로 A의 8883만 열면 된다. B 쪽 5432·9092는 DB/Kafka용으로 별도다.

## 마무리

- `~/mqtt-ca/ca.key`는 `chmod 600`으로 두고 잃지 않는다. ESP32 등 기기 인증서를 이 CA로 발급하게 된다.
- 배치가 끝난 `~/mqtt-ca/server.key`, `server.csr`, `san.cnf`는 지워도 된다: `shred -u ~/mqtt-ca/server.key ~/mqtt-ca/server.csr`
- 인증서 유효기간은 CA·서버 모두 10년(3650일)이다.
- 인증서를 재발급하거나 SAN을 바꾸면 4단계 배치 후 A에서 `docker compose --project-directory /opt/nilm restart mosquitto`, B `.env`의 `MQTT_HOST`가 바뀌었다면 Jenkins 재배포가 필요하다.

## 배포 후 확인

B에서 A의 8883 포트 도달 확인:

```bash
nc -zv <A_PRIVATE_IP> 8883
```

Bridge가 healthy가 된 뒤 Jenkins의 `Deploy Bridge and verify pipeline` 스테이지가 `smoke.py`로 MQTT→Kafka 전달을 검증한다. 수동으로는 B에서:

```bash
docker compose --project-directory /opt/nilm exec -T mqtt-kafka-bridge python smoke.py
```
