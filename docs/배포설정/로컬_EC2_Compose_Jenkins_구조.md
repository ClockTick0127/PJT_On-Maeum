# 로컬·EC2 Docker Compose 및 Jenkins 배포 구조

작성일: 2026-09-06

> 이 문서는 구현된 로컬·EC2-A·EC2-B Compose와 Jenkins 배포 구조를 설명한다. 이미지 빌드와 격리된 로컬 통합 검증을 완료했다. 실제 Jenkins job 설정·EC2 배포·기존 데이터 이전은 별도 작업이다. 구체적인 사용법은 [실행 안내](../../infrastructure/README.md), 검증 범위와 서버 준비는 [Jenkins 설정](Jenkins_실행_및_검증.md)을 참고한다.

## 1. 구성 방침

Docker Compose는 `infrastructure` 아래에서 관리한다. 로컬, EC2-A, EC2-B에 각각 독립된 Compose 파일 하나를 두며 공통 Compose를 여러 개 합치는 방식은 사용하지 않는다.

- 개발자는 `infrastructure/local`에서 로컬 서비스를 실행한다.
- Jenkins는 EC2-A용 Compose와 EC2-B용 Compose를 각 서버에 배포한다.
- Backend와 Bridge 소스, Dockerfile, 초기화 SQL은 기존 파트 폴더에서 관리한다.
- 로컬 Compose는 소스를 빌드하고, 운영 Compose는 Jenkins가 만들어 Registry에 올린 이미지를 실행한다.
- 환경별 YAML에 일부 중복을 허용한다. 공통 환경변수나 헬스체크를 바꿀 때는 해당 서비스의 로컬·운영 정의를 함께 검토한다.

env를 읽는 시점이 다르다는 이유만으로 파일 분리가 기술적으로 강제되지는 않는다. 이 설계에서는 Jenkins의 전달 대상과 관리 편의를 기준으로 프론트 빌드용, EC2-A 실행용, EC2-B 실행용 설정을 구분한다.

## 2. 목표 서버 배치

| 실행 대상 | 서비스 |
| --- | --- |
| 로컬 | 개발에 필요한 PostgreSQL, Kafka, Mosquitto, Bridge, Keycloak, Redis, Backend. Frontend는 Vite 개발 서버 또는 별도 컨테이너로 실행 |
| EC2-A | Frontend/Nginx, API Gateway, IoT Device Service, Monitoring Service, Keycloak, Mosquitto, Redis |
| EC2-B | PostgreSQL, Kafka, MQTT–Kafka Bridge |

첫 스켈레톤 배포에서는 Spark, Flink, S3 Archive Sink, AI 학습·추론 서비스는 포함하지 않는다. 기존 HDFS 작업은 별도 실험용으로 보존한다.

## 3. 저장소 폴더 구조

아래는 주요 구조이며, 실행·마이그레이션 스크립트는 `infrastructure/local`, 배포 스크립트는 `infrastructure/scripts`에 추가되어 있다.

```text
프로젝트/
├─ Jenkinsfile                           # 빌드·검증·배포 파이프라인
│
├─ backend/
│  ├─ api-gateway/
│  │  ├─ Dockerfile                      # 기존 유지
│  │  └─ src/
│  ├─ iot-device-service/
│  │  ├─ Dockerfile                      # 기존 유지
│  │  └─ src/
│  └─ monitoring-service/
│     ├─ Dockerfile                      # 기존 유지
│     └─ src/
│
├─ frontend/
│  ├─ Dockerfile                         # 정적 프론트 이미지 빌드
│  └─ src/
│
└─ infrastructure/
   ├─ local/                             # 로컬 실행 진입점
   │  ├─ compose.yaml
   │  ├─ .env                            # 개발자별 값, Git 제외
   │  └─ .env.example
   │
   ├─ ec2-a/                             # A 서버 전용 정의
   │  ├─ compose.yaml
   │  └─ .env.example
   │
   ├─ ec2-b/                             # B 서버 전용 정의
   │  ├─ compose.yaml
   │  └─ .env.example
   │
   ├─ nginx/                             # 정적 파일·API·인증 프록시 설정
   │  └─ default.conf
   │
   ├─ postgres/
   │  └─ 01-create-databases.sql          # 기존 유지
   │
   ├─ mqtt/
   │  ├─ config/
   │  │  ├─ mosquitto.local.conf          # 현재 설정을 로컬용으로 분리
   │  │  ├─ mosquitto.production.conf     # 운영 TLS 설정
   │  │  └─ passwd                        # 로컬 인증 파일, Git 제외
   │  ├─ simulator/
   │  │  ├─ simulator.py
   │  │  └─ requirements.txt
   │  └─ README.md                       # 새 경로와 실행 방법 반영
   │
   ├─ mqtt-kafka-bridge/
   │  ├─ bridge.py                       # 기존 소스 유지
   │  ├─ requirements.txt
   │  └─ Dockerfile                      # Bridge 컨테이너 이미지
   │
   ├─ kafka/
   │  └─ replay/
   │     ├─ producer.py
   │     ├─ consumer_check.py
   │     └─ requirements.txt
   │
   └─ hdfs/                              # 별도 실험용으로 유지
      ├─ docker-compose.yml
      └─ loader/
         ├─ Dockerfile
         ├─ loader.py
         └─ requirements.txt
```

Frontend/Nginx는 첫 배포에서는 프론트 정적 파일을 포함한 Nginx 이미지 하나로 구성할 수 있다. 도메인·인증서·프록시 설정은 운영 배치에 맞춰 추가한다.

## 4. 기존 파트 폴더의 관리 방법

| 폴더 | 역할 | 새 구성에서 사용하는 방법 |
| --- | --- | --- |
| `postgres/` | DB 초기 생성 SQL | 로컬 및 EC2-B PostgreSQL 컨테이너에 마운트 |
| `mqtt/config/` | Mosquitto 리스너·인증·TLS 설정 | 로컬 설정은 로컬 Compose가, 운영 설정은 EC2-A Compose가 사용 |
| `mqtt/simulator/` | 센서를 대신하는 MQTT 발행 도구 | 개발·시연 시 명시적으로 실행, 운영 상시 실행 대상에서 제외 |
| `mqtt-kafka-bridge/` | MQTT 수신 후 Kafka에 전달하는 코드 | 로컬은 소스 빌드, 운영은 Jenkins 이미지 사용 |
| `kafka/replay/` | Kafka에 데이터 재생 및 수신 확인 | 검증 도구로 유지. Kafka 서버 자체의 실행 정의가 아님 |
| `hdfs/` | HDFS와 Kafka→HDFS 적재 실험 | 기본 로컬·운영 배포에서 제외하고 필요할 때 별도 실행 |
| `backend/*/` | Spring 서비스 코드와 Dockerfile | 기존 위치에서 개발. 새 Compose가 해당 소스 또는 이미지를 참조 |

폴더가 존재한다고 자동으로 실행되지는 않는다. Compose의 `services`에 등록한 서비스가 실행 대상이다. 코드나 SQL을 로컬용·운영용으로 복제하지 않는다.

현재 HDFS Compose는 외부 네트워크 `nilm-net`에 의존한다. 새 로컬 네트워크를 도입할 때 HDFS 연결 설정도 함께 맞춰야 한다. HDFS Loader를 그대로 S3 Archive Sink로 사용할 수는 없다.

## 5. 로컬 Compose와 운영 Compose의 차이

| 항목 | 로컬 | 운영 |
| --- | --- | --- |
| 애플리케이션 | `build:`로 소스 빌드 | `image:`로 Registry 이미지 실행 |
| 환경변수 파일 | `infrastructure/local/.env` | 각 서버 `/opt/nilm/.env` |
| Backend→DB | 같은 Compose의 `postgres:5432` | EC2-B 사설 주소의 PostgreSQL |
| Bridge→MQTT | 같은 Compose의 `mosquitto` | EC2-A의 내부 DNS 또는 사설 주소 |
| Bridge→Kafka | `kafka:19092` | 같은 EC2-B Compose의 `kafka:19092` |
| 외부 공개 포트 | IDE·디버깅에 필요한 포트 | 사용자 진입 포트와 서버 간 통신 포트만 |
| Mosquitto 설정 | 로컬 설정·로컬 계정 | 운영 TLS 설정·운영 계정·인증서 |

### 로컬 경로 예시

아래 YAML은 경로 설명용 일부 설정이다. 이미지, 환경변수, 포트, 헬스체크 등을 포함한 완성본이 아니다.

```yaml
# infrastructure/local/compose.yaml
services:
  postgres:
    image: postgres:18-alpine
    volumes:
      - postgres_data:/var/lib/postgresql
      - ../postgres/01-create-databases.sql:/docker-entrypoint-initdb.d/01-create-databases.sql:ro

  mqtt-kafka-bridge:
    build:
      context: ../mqtt-kafka-bridge

  iot-device-service:
    build:
      context: ../../backend/iot-device-service
    environment:
      POSTGRES_URL: jdbc:postgresql://postgres:5432/device_db
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}

volumes:
  postgres_data:
```

### 운영 이미지 예시

```yaml
# infrastructure/ec2-a/compose.yaml 일부
services:
  iot-device-service:
    image: ${REGISTRY}/nilm-iot-device-service:${IMAGE_TAG}
    restart: unless-stopped
    environment:
      POSTGRES_URL: jdbc:postgresql://${POSTGRES_HOST}:5432/device_db
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
```

`IMAGE_TAG`는 Jenkins가 이번 배포의 커밋 SHA로 설정한다. 환경변수 Credential에 오래된 태그를 고정하지 않는다. 독립 배포를 도입할 때는 서비스별 이미지 태그로 확장한다.

## 6. env와 Jenkins Credentials

| Jenkins Credential | 전달 대상 | 적용 시점 |
| --- | --- | --- |
| `frontend-build-env` | 프론트 이미지 빌드 단계 | Vite 빌드 시점. 실제 필요한 변수가 있을 때 사용 |
| `ec2-a-runtime-env` | EC2-A의 `/opt/nilm/.env` | Compose 설정 해석 및 컨테이너 생성 시점 |
| `ec2-b-runtime-env` | EC2-B의 `/opt/nilm/.env` | Compose 설정 해석 및 컨테이너 생성 시점 |

`frontend-build.env`, `ec2-a-runtime.env`, `ec2-b-runtime.env`처럼 파일 이름을 구분해 준비하더라도 서버에는 각 운영 파일을 `.env`라는 이름으로 저장할 수 있다. 서로 다른 서버이므로 충돌하지 않는다. AI 설정은 실제 배포할 AI 서비스가 생길 때 추가한다.

```text
Jenkins Credential
    → 서버의 .env 파일
    → Compose YAML의 ${변수} 치환
    → environment에 지정한 컨테이너 환경변수
    → Spring 또는 Python 코드가 읽음
```

`.env`의 모든 값이 컨테이너에 자동 주입되지는 않는다. 각 서비스의 `environment`에서 필요한 항목만 연결한다. `env_file: .env`를 모든 서비스에 지정해 전체 운영 비밀값을 통째로 전달하지 않는다.

Spring 설정에는 `.env` 파일을 직접 읽는 `spring.config.import`가 없다. 컨테이너 환경변수 등을 통해 설정값을 받으므로 Spring에 운영 파일 경로를 추가할 필요는 없다.

Vite의 `VITE_*` 값은 브라우저에 전달되는 빌드 결과에 포함된다. 비밀값을 넣지 않는다. API를 상대 경로 `/api`로 요청하고 Nginx에서 Gateway로 전달하면 API 주소용 빌드 변수는 생략할 수 있다. 프론트에는 `VITE_*` 소비 코드가 없으므로 실제 사용 항목을 먼저 정한다.

Mosquitto의 `passwd`, TLS 개인키·인증서는 `.env`와 별개의 배포 파일이다. env에 비밀번호를 넣는 것만으로 Mosquitto 계정이 생성되지 않는다. 실제 env, 인증 파일, 개인키는 Git에 커밋하지 않고 배포 전에 ignore 규칙을 확인한다.

## 7. 운영 서버 파일 배치

운영 Compose는 다음 `/opt/nilm` 배치에서 실행되도록 작성한다. 이 경로는 프로젝트의 배포 규칙이며 Jenkins나 Docker의 강제 경로가 아니다.

```text
EC2-A /opt/nilm/
├─ compose.yaml                   # 저장소 ec2-a/compose.yaml을 배포
├─ .env                           # ec2-a-runtime-env
├─ nginx/
│  └─ default.conf
├─ certs/                         # Nginx HTTPS 인증서·개인키
└─ mqtt/
   ├─ mosquitto.conf              # 운영 설정을 이 이름으로 배포
   ├─ passwd                     # 운영 계정 파일
   └─ certs/
      ├─ server.crt
      └─ server.key

EC2-B /opt/nilm/
├─ compose.yaml                   # 저장소 ec2-b/compose.yaml을 배포
├─ .env                           # ec2-b-runtime-env
├─ postgres/
│  └─ 01-create-databases.sql
└─ mqtt/
   └─ certs/
      └─ ca.crt                  # Bridge의 TLS 신뢰 구성에 필요한 경우
```

운영 Compose의 `./mqtt/...`, `./postgres/...`는 위 서버 배치 기준이다. 저장소의 `ec2-a` 폴더에서 그대로 실행하기 위한 경로가 아니다. Jenkins가 Compose뿐 아니라 마운트할 파일까지 준비해야 한다.

운영 서버에는 Backend·Bridge 소스를 복사하지 않는다. Jenkins가 빌드한 이미지에 코드가 들어간다. PostgreSQL과 Kafka 데이터는 별도 Docker 볼륨에 저장하고, 일반 코드 배포에서 볼륨을 삭제하지 않는다.

## 8. 실행 방법

아래 명령은 env와 MQTT 계정 파일을 준비한 이후 사용한다. 기존 Kafka 사용자는 먼저 실행 안내의 마이그레이션 절차를 확인한다.

### 로컬

최초에 `infrastructure/local/.env.example`을 `.env`로 복사하고 로컬 값을 설정한다. MQTT 계정 파일 등 필요한 마운트 파일도 준비한다.

저장소 루트에서:

```powershell
cd infrastructure/local
docker compose up -d --build
```

상태 및 로그 확인:

```powershell
docker compose ps
docker compose logs -f iot-device-service
```

Backend를 IDE에서 실행할 경우에는 기반 서비스만 선택한다.

```powershell
docker compose up -d postgres keycloak kafka mosquitto redis
```

IDE에서 실행하는 Java 프로세스에는 별도로 환경변수를 설정해야 한다. Compose의 `.env`가 IDE에 자동 전달되지는 않는다.

### 운영 재배포

Jenkins가 각 서버의 파일·이미지 태그를 준비한 뒤 실행한다.

```bash
cd /opt/nilm
docker compose pull
docker compose up -d
```

첫 배포는 아래 의존 순서대로 서비스를 나누어 실행하고 준비 상태를 확인한다. 단순히 두 서버에서 동시에 `up`하는 것을 배포 완료로 간주하지 않는다.

## 9. master 스켈레톤 CI/CD를 위해 반영한 변경

| 영역 | 전환 전 상태 | 반영한 변경 |
| --- | --- | --- |
| Compose | 인프라와 Backend Compose가 별도로 존재 | 독립된 local/ec2-a/ec2-b Compose 구현 |
| Backend 이미지 | Compose에서 `build:` 사용 | 운영은 커밋 SHA 태그 이미지 사용 |
| DB 사용자 | YAML `POSTGRES_USERNAME`, Spring `POSTGRES_USER` | YAML을 `POSTGRES_USER`로 일치 |
| 서버 간 DB 접속 | `postgres:5432` 사용 | A에서 B의 사설 주소로 접속, B 포트와 보안 그룹 구성 |
| Kafka | 외부 광고 주소 `localhost:9092` | A에서도 접속 가능한 B의 사설 주소 광고 |
| Kafka 토픽 | 자동 생성 비활성화 | `power.raw.v1` 등 필요한 토픽을 반복 실행 가능한 초기화 단계로 생성. 목표 파티션 수 24 반영 |
| 네트워크 | 기존 `nilm-net` 및 외부 네트워크 의존 | A/B 각각 자체 네트워크. 서버 간에는 사설 주소 사용 |
| Keycloak | `start-dev`, localhost 기본 주소 | 운영 실행 모드·도메인·프록시·B DB 연결 및 nilm realm/client 초기 구성 |
| Bridge | Python 소스와 requirements만 존재 | Dockerfile·서비스 정의·연결 재시도·종료 처리 추가 |
| MQTT TLS | Mosquitto TLS 주석 처리, Bridge TLS 코드 없음 | 서버 TLS 설정·인증서 마운트·클라이언트 TLS 검증 구현 |
| Frontend | Dockerfile과 Nginx 배포 설정 없음 | 이미지 빌드·정적 파일 제공·API/인증 프록시 구성 |
| Redis | 현재 메인 Compose에 없음 | A 및 로컬 Compose에 추가, 필요한 서비스 연결 |
| 검증 | PostgreSQL 외 주요 Compose 헬스체크 부족 | 이미지에서 실행 가능한 헬스체크와 Jenkins 요청 검증 추가 |
| 영속 데이터 | 기존 프로젝트 이름에 연결된 볼륨 | 운영 프로젝트·볼륨 이름 고정, 기존 데이터 이전 여부 명시 |

### Bridge 환경변수 이름

`bridge.py`가 실제로 읽는 이름을 사용한다.

```yaml
environment:
  MQTT_HOST: ${MQTT_HOST}
  MQTT_PORT: ${MQTT_PORT}
  MQTT_USER: ${MQTT_USER}
  MQTT_PASS: ${MQTT_PASS}
  MQTT_TOPIC: v1/power/sim/+/main
  KAFKA_BOOTSTRAP: kafka:19092
  KAFKA_TOPIC: power.raw.v1
```

`MQTT_USERNAME`, `MQTT_PASSWORD`, `KAFKA_BOOTSTRAP_SERVERS`를 전달해도 현재 코드는 해당 이름을 읽지 않는다. 포트 변경과 함께 TLS 설정이 필요하다. 구현된 Bridge는 `MQTT_TLS_ENABLED=true`, `MQTT_CA_FILE`로 인증서와 호스트명을 검증한다.

### DB 초기화와 스키마 변경

현재 `01-create-databases.sql`은 device/monitoring/analysis/keycloak DB만 생성한다. 서비스별 DB 사용자 생성은 포함하지 않는다. 별도 사용자를 도입하면 사용자·권한 생성도 함께 구현해야 한다.

PostgreSQL 초기화 SQL은 빈 데이터 디렉터리로 최초 초기화할 때 실행된다. 일반 배포마다 재실행하거나 `down -v`로 DB를 지우지 않는다. 이후 애플리케이션 스키마 변경은 Flyway 마이그레이션으로 관리한다.

## 10. Jenkins 파이프라인과 검증 기준

첫 배포 흐름:

```text
코드 체크아웃
  → 서비스별 테스트·빌드 및 Compose 설정 검증
  → 애플리케이션 이미지 빌드·Registry 업로드
  → 서버별 Compose·env·마운트 파일 배치
  → B: PostgreSQL·Kafka 실행 및 준비 확인
  → B: Kafka 토픽 초기화
  → A: Mosquitto·Redis·Keycloak 실행 및 인증 초기 구성 확인
  → A: Backend·Frontend/Nginx 실행 및 요청 확인
  → B: Bridge 실행
  → MQTT → Bridge → Kafka 전달 확인
```

- CI 빌드·테스트는 운영 배포 준비와 병행해서 구성한다.
- master 자동 배포는 초기 배포 검증을 통과한 뒤 활성화한다.
- 같은 배포 대상의 파이프라인이 동시에 실행되지 않게 한다.
- Compose 설정 검증에는 `docker compose config --quiet` 등을 사용한다. 실제 비밀값이 포함된 전체 설정을 Jenkins 로그에 출력하지 않는다.
- 같은 Compose의 준비 대기는 헬스체크를 이용할 수 있다. A/B 서버 사이의 준비 상태는 Jenkins에서 별도로 확인한다.
- 배포 성공 기준은 컨테이너의 `Up` 표시뿐 아니라 Backend health, Gateway 요청, 프론트 응답, 인증 및 MQTT→Kafka 전달 성공이다.
- 테스트 이미지 태그를 재배포해도 DB·Kafka 데이터가 유지되는지 확인한다.
- 이전 배포 이미지 태그를 기록해 애플리케이션 롤백에 사용한다. DB 마이그레이션은 이미지 롤백과 별도로 호환성을 검토한다.

## 11. 기존 파일 전환 및 남은 작업

새 Compose, Dockerfile, Jenkinsfile 및 실행 문서를 추가하고 기존 Compose 두 개를 제거했다. 기존 로컬 컨테이너는 자동으로 재생성하지 않았다. 기존 Kafka는 로그가 `/tmp/kafka-logs`에 있으므로 `Migrate-Legacy.ps1 -Apply`로 정지·백업·볼륨 복사 후 새 구성으로 전환해야 한다. 실제 운영 서버 배포와 Jenkins job 등록은 서버 준비 후 수행한다.

| 기존 파일 | 전환 후 처리 |
| --- | --- |
| `infrastructure/docker-compose.yml` | 새 Compose 3개로 대체 및 제거 |
| `infrastructure/.env`, `.env.example` | 실제 env는 이전 입력으로 보존. 기존 example은 제거하고 서버별 example 제공 |
| `backend/compose.yaml` | 서비스 정의 이전 후 제거 |
| `backend/.env` | Compose 용도면 로컬 `.env`로 통합. IDE 사용 여부는 별도 확인 |
| `infrastructure/hdfs/docker-compose.yml` | 실험용 유지, 로컬 네트워크 변경 반영 |
| Backend Dockerfile, Bridge 소스, SQL | 기존 위치 유지 |

## 참고

- [Docker Compose 환경변수](https://docs.docker.com/compose/how-tos/environment-variables/variable-interpolation/)
- [Docker Compose 운영 배포](https://docs.docker.com/compose/how-tos/production/)
- [Docker Compose 시작 순서와 헬스체크](https://docs.docker.com/compose/how-tos/startup-order/)
- [Keycloak 프록시 설정](https://www.keycloak.org/server/reverseproxy)

## 12. 구현상 보충 사항

- 로컬 프로젝트 이름은 기존 데이터·HDFS 호환성을 위해 `nilm-postgres`, 네트워크는 `nilm-net`이다.
- Kafka UI와 컨테이너 Frontend는 각각 `tools`, `frontend` 프로필로 선택 실행한다.
- 운영 Compose는 `CONFIG_ROOT`로 마운트 기준 경로를 받으며 Jenkins가 `/opt/nilm`을 설정한다.
- Keycloak realm import 파일은 `infrastructure/keycloak/nilm-realm.json`에 있다. 기존 realm은 덮어쓰지 않으므로 기존 환경의 client 설정은 따로 확인한다.
- MQTT는 영속 세션과 디스크 persistence를 사용한다. Bridge는 Kafka 전달 확인 후 MQTT ACK하지만 재시도 중 중복은 가능하다.
- Backend/Bridge/Frontend 이미지 빌드와 로컬 통합 검증은 완료했으며, 운영 Jenkins에서의 실행 및 EC2 검증은 미수행이다.
