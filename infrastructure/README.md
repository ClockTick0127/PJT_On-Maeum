# 실행 및 배포

## 폴더별 역할

- `local/compose.yaml`: 로컬 전체 서비스. PostgreSQL·Kafka의 기존 프로젝트/볼륨 이름을 보존한다.
- `ec2-a/compose.yaml`: Backend, Keycloak, Redis, Mosquitto, Frontend/Nginx.
- `ec2-b/compose.yaml`: PostgreSQL, Kafka, 토픽 초기화, Bridge.
- 서비스 코드와 SQL은 기존 `postgres/`, `mqtt/`, `mqtt-kafka-bridge/`, `kafka/` 및 저장소 `backend/`에 둔다.
- HDFS는 별도 실험용으로 유지하며 `nilm-net`에 접속한다.
- [전체 구조](../docs/배포설정/로컬_EC2_Compose_Jenkins_구조.md), [Jenkins 설정](../docs/배포설정/Jenkins_실행_및_검증.md)

## 최초 로컬 설정 (Windows PowerShell)

저장소 루트에서:

```powershell
cd infrastructure/local
.\Setup-Local.ps1
```

기존 `infrastructure/.env`의 DB 설정과 `backend/.env`의 인증 주소 설정을 새 `.env`로 복사한다. 새 `.env`가 이미 있으면 덮어쓰지 않는다. 비밀번호는 출력하지 않는다.

`.env`의 `MQTT_USER`, `MQTT_PASS`는 기존 `../mqtt/config/passwd` 계정과 일치해야 한다. 받은 passwd의 해시에서 원래 비밀번호를 알아낼 수는 없다. 발급자에게 비밀번호를 받거나 별도 계정을 등록한 뒤 설정한다.

MQTT 계정 파일이 없는 새 환경에서는 `.env`의 로컬 비밀번호를 설정한 후 다음을 실행한다. 비어 있지 않은 기존 passwd는 덮어쓰지 않는다.

```powershell
.\Setup-Local.ps1 -InitializeMqtt
```

Linux에서 새 환경을 준비할 경우 `.env.example`을 `.env`로 복사하고 값을 설정한다. MQTT 계정 파일은 `mosquitto_passwd`로 준비한다.

## 기존 환경 전환 — 처음 한 번

**기존 설정의 Kafka 데이터는 실제로 `/tmp/kafka-logs`에 저장되고 있었다. 새 Compose는 `/var/lib/kafka/data` 볼륨을 사용한다. 기존 Kafka를 곧바로 재생성하거나 삭제하면 기존 로그를 잃을 수 있다.**

기존 로컬 구성이 실행 중이라면 새 `.env`를 준비하고 MQTT 발행을 멈춘 뒤 다음 순서로 전환한다.

```powershell
# infrastructure/local에서 실행. 첫 명령은 계획만 출력한다.
.\Migrate-Legacy.ps1
.\Migrate-Legacy.ps1 -Apply
docker compose up -d --build
```

전환 스크립트는:

1. `nilm-local` 프로젝트의 `nilm-kafka`인지 확인한다.
2. 목적지 Kafka 볼륨이 비어 있을 때만 진행한다.
3. Kafka를 정지하고 기존 로그를 저장소 `.deployment/kafka-backup-날짜`에 복사한다.
4. 클러스터 ID를 보존하고 로그를 기존 named volume으로 복사한다.
5. 이전 `nilm-backend` 프로젝트의 Gateway/Device/Monitoring 컨테이너만 정지·제거한다.
6. PostgreSQL 및 모든 기존 볼륨은 삭제하지 않는다.

복사 실패 시 기존 Kafka를 다시 시작한다. 백업은 자동 삭제하지 않는다. 부분 복사로 목적지 볼륨에 데이터가 생겼으면 재실행이 거부되므로 백업·원본을 확인하고 수동 복구한다. 이 스크립트는 이번 구현 검증 중 실제 사용자 데이터에 실행하지 않았다.

이미 새 구성을 사용하는 환경에는 이 스크립트를 다시 실행하지 않는다.

## 평소 실행

```powershell
cd infrastructure/local
docker compose up -d --build
docker compose ps
```

로그:

```powershell
docker compose logs -f mqtt-kafka-bridge
```

프론트는 기본적으로 `frontend` 폴더에서 `npm ci`, `npm run dev`로 실행한다. Vite의 `/api` 프록시는 `localhost:8080`을 사용한다.

프론트 컨테이너 또는 Kafka UI가 필요할 때만 해당 프로필을 활성화한다.

```powershell
docker compose --profile frontend --profile tools up -d --build
```

Frontend 3000, Vite 5173, Keycloak 8090, Gateway 8080, Device 8081, Monitoring 8082, Kafka UI 8091. 로컬 호스트 공개 포트는 127.0.0.1에 바인딩한다.

Keycloak의 새 dashboard client redirect는 `FRONTEND_ORIGIN`을 사용한다. Docker 프론트 3000으로 로그인할 때는 해당 주소에 맞춘 client 설정이 필요하다. 현재 화면에는 로그인 UI가 아직 없다.

Backend를 IDE에서 실행할 때:

```powershell
docker compose up -d postgres keycloak kafka mosquitto redis
```

IDE 환경변수는 별도 설정한다. Compose의 `.env`가 IDE로 자동 전달되지 않는다.

## 데이터 전달 테스트

Bridge가 healthy가 된 뒤:

```powershell
docker compose exec -T mqtt-kafka-bridge python smoke.py
```

고유한 `smoke-...` 가구 ID로 테스트 메시지 1건을 MQTT에 발행한 뒤 Kafka의 동일 레코드를 확인한다. 레코드는 `smoke_test: true`를 포함한다. 테스트 데이터를 사용하면 안 되는 분석에서는 이 표시를 제외한다.

Bridge는 MQTT QoS 1을 Kafka 전달 확인 후 ACK한다. 재전달 시 중복 가능성은 있으므로 exactly-once 처리로 간주하지 않는다. 운영 MQTT 계정에 ACL을 도입하면 이 테스트 계정의 발행 권한도 별도로 맞춰야 한다.

## Keycloak 초기 구성

`../keycloak/nilm-realm.json`을 최초 기동 시 import한다.

- `nilm-dashboard`: 공개 client, Authorization Code + PKCE.
- `nilm-smoke`: 서비스 계정 client, CI/CD의 인증 요청 검증용.
- smoke secret은 `NILM_SMOKE_CLIENT_SECRET`로 전달한다.
- 기존 `nilm` realm이 있으면 import는 건너뛴다. 사용자/realm을 자동 삭제하거나 덮어쓰지 않는다.
- 기존 realm에 client가 없거나 secret이 다르면 Admin Console에서 생성·갱신해야 한다. env 변경만으로 기존 client secret이 갱신되지는 않는다.

## 운영 배포

각 EC2의 `/opt/nilm`에 해당 Compose·env·설정 파일을 준비한다. Jenkins의 `prepare.sh`가 비밀 파일을 제외한 설정을 복사하고 `CONFIG_ROOT=/opt/nilm`을 기록한다.

```bash
cd /opt/nilm
docker compose pull
docker compose up -d
```

최초 배포는 Jenkins가 B 기반 서비스 → A → B Bridge 순서로 준비 상태를 확인하며 실행한다. 운영 백엔드와 Keycloak은 B의 PostgreSQL에 연결한다. B의 5432·9092는 사설 IP에 바인딩하며 보안 그룹에서도 필요한 A 서버 접근만 허용한다.

## 데이터 및 설정 유의 사항

- 일반 배포에서 `down -v`를 사용하지 않는다.
- SQL은 빈 PostgreSQL 데이터 디렉터리를 초기화할 때만 실행된다. 이후 스키마 변경은 Flyway로 관리한다.
- 현재 DB 연결은 공통 PostgreSQL 사용자 하나를 사용한다. 서비스별 사용자를 도입할 때는 사용자/권한 생성 SQL도 함께 구현해야 한다.
- Kafka 초기화는 기존 토픽을 삭제하지 않는다. 기존 토픽의 파티션 수는 자동 변경하지 않으며 신규 raw 토픽은 기본 24파티션이다.
- `mqtt/config/mosquitto.conf`는 기존 실행 중인 컨테이너의 바인드 경로 보존을 위해 남긴 레거시 파일이다. 새 Compose는 local/production 설정을 사용한다.
