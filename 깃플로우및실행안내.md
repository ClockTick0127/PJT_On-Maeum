# 로컬 실행 및 배포 (루트 위치 기준)

Docker Compose는 `infrastructure/local`(로컬), `infrastructure/ec2-a`, `infrastructure/ec2-b`(운영)에서 각각 관리합니다. 아래 명령은 모두 `infrastructure/local`에서 실행합니다.

- [실행 안내 상세](infrastructure/README.md)
- [전체 구조 설명](docs/배포설정/로컬_EC2_Compose_Jenkins_구조.md)
- [Jenkins 설정과 검증 결과](docs/배포설정/Jenkins_실행_및_검증.md)



## 인프라 + 백엔드 + 프론트엔드 한번에 실행
```dash
docker compose -f infrastructure/local/compose.yaml --profile frontend up -d --build
```

## 인프라 + 백엔드 실행
```dash
docker compose -f infrastructure/local/compose.yaml up -d --build
```

## 프론트엔드 실행
```dash
docker compose -f infrastructure/local/compose.yaml --profile frontend up -d --build frontend
```

## HDFS + 원본 적재기 실행
nilm-net을 external로 참조하므로 인프라+백엔드 실행 먼저 해야함
```dash
docker compose -f infrastructure/hdfs/docker-compose.yml up -d --build
```
Namenode UI: http://localhost:9870

## Silver 일일 배치 (power-silver-daily)
Bronze 전력 원본에서 정제 전력과 가구별 관측일을 만드는 Spark 배치입니다. 상시 서비스가
아니라 하루에 한 번 실행합니다. 자세한 내용은 [batch/power_silver_service/README.md](batch/power_silver_service/README.md).
```dash
docker compose -f infrastructure/local/compose.yaml run --rm power-silver power-silver run --date 2026-09-19
docker compose -f infrastructure/local/compose.yaml run --rm power-silver power-silver status --json
```

## Gold 프로필 배치

최근 28일의 활성 `appliance_usage_daily`와 세션 slice 버전을 고정해 shadow
routine baseline과 지표별 통계 프로필을 생성합니다. 기존 serving DB baseline과 위험
점수는 변경하지 않습니다. 자세한 계약은
[batch/gold_profile_service/README.md](batch/gold_profile_service/README.md)를 참고하세요.

```bash
docker compose -f infrastructure/local/compose.yaml run --rm gold-profile gold-profile run --as-of 2026-09-19
docker compose -f infrastructure/local/compose.yaml run --rm gold-profile gold-profile dirty --from 2026-09-01 --to 2026-09-28
```

## 시뮬레이터
1. 의존성 설치(최초 1회)
```dash
pip install -r infrastructure/mqtt/simulator/requirements.txt
```

2-1. 웹 대시보드 실행
```dash
python infrastructure/mqtt/simulator/web_server.py
```
접속: http://127.0.0.1:8085

2-2. CLI 실행
```dash 
python infrastructure/mqtt/simulator/simulator.py --scenario normal_routine
```

2-3. EC2-A에서 Docker로 실행
master 배포 때 `simulator` 이미지가 함께 pull되지만, 자동으로 뜨지는 않습니다. 최초 1회 아래를 준비합니다.
- Jenkins `ec2-a-runtime-env`에 `SIMULATOR_MQTT_HOST`(EC2-A 사설 IP), `SIMULATOR_MQTT_PASS`를 추가합니다.
- 운영 MQTT CA(`CN=nilm-mqtt-ca`)를 `/opt/nilm/mqtt/certs/ca.crt`에 둡니다. 다른 경로를 쓰면 `SIMULATOR_MQTT_CA_FILE`로 지정합니다.
- 접속 비밀번호 파일을 만듭니다(비밀번호는 프롬프트로 입력).
```bash
printf 'demo:%s\n' "$(openssl passwd -apr1)" > /opt/nilm/nginx/auth/simulator.htpasswd && chmod 644 /opt/nilm/nginx/auth/simulator.htpasswd
```
기동·종료:
```bash
cd /opt/nilm && docker compose --profile simulator up -d simulator
```
```bash
cd /opt/nilm && docker compose --profile simulator stop simulator
```
접속: `https://<APP_DOMAIN>/simulator/routine-demo` (아이디 `demo`). 컨테이너가 꺼져 있으면 502가 납니다.

### 운영 Prometheus / Grafana 확인
1. SSH 터널 접속 (창은 닫지 말 것)
```bash
ssh -i <경로>\J15D201T.pem -L 13001:127.0.0.1:13001 -L 19090:127.0.0.1:19090 ubuntu@<EC2-A 퍼블릭 IP>
```

2. 내 PC 브라우저에서 접속

- Prometheus: http://localhost:19090
- Grafana: http://localhost:13001 (admin / 배포 담당자에게 문의)
> - 포트 충돌(`Address already in use`) 시 **`-L`의 앞 숫자만** 변경하고(예: `-L 28090:127.0.0.1:19090`)
    >   브라우저도 그 번호로 접속. 뒤 숫자는 EC2-A 포트이므로 그대로 둘 것.
> - `컨테이너 자원` 대시보드는 운영에서 cAdvisor를 사용하지 않아 비어 있는 것이 정상.
> - SSH 창을 닫으면 터널도 끊김.


## `.env`를 바꿨을 때

```powershell
docker compose up -d
```

`--build`는 필요 없습니다. 환경변수가 바뀐 컨테이너만 재생성됩니다. 단, 아래 값은 **첫 기동 때 데이터에 굳어져서 `.env`만 바꿔도 반영되지 않습니다.**

| 변수 | 반영되지 않는 이유 | 데이터를 유지하며 바꾸는 방법 |
| --- | --- | --- |
| `KEYCLOAK_ADMIN_PASSWORD` | 관리자는 `keycloak_db`가 비었을 때 한 번만 생성 | Admin Console(realm `master` → Users → admin → Credentials) 또는 `kcadm.sh set-password -r master --username admin` |
| `POSTGRES_PASSWORD` | 데이터 디렉터리 초기화 때만 적용 | `ALTER ROLE <user> PASSWORD '...'` 실행 후 `.env` 수정 → `up -d` |
| `POSTGRES_USER` | 같음 | 이름 변경은 비권장. 아래 전체 초기화 사용 |
| `FRONTEND_ORIGIN`, `NILM_SMOKE_CLIENT_SECRET` | realm import는 `nilm` realm이 없을 때만 실행 | Admin Console에서 client 설정 수정 |
| `MQTT_USER`, `MQTT_PASS` | 실제 인증은 `mqtt/config/passwd` 파일 | `mosquitto_passwd`로 passwd 갱신 후 `.env` 수정 → `up -d` |

`JWT_ISSUER_URI`, `CORS_ALLOWED_ORIGINS`, `APP_SECURITY_ENABLED`, `KAFKA_RAW_TOPIC`은 컨테이너 환경변수로만 쓰여 `up -d`만으로 반영됩니다.

## 백엔드·Bridge 코드를 바꿨을 때

```powershell
docker compose up -d --build api-gateway
```

바뀐 서비스만 지정합니다(`api-gateway`, `iot-device-service`, `monitoring-service`, `mqtt-kafka-bridge`, `realtime-analysis-service`, `frontend`). 백엔드 Dockerfile은 이미지 안에서 `./gradlew test bootJar`를 실행하므로 로컬 사전 빌드는 필요 없지만, **테스트가 실패하면 이미지 빌드도 실패**합니다. 빌드 로그는 `docker compose build <service> --progress=plain`으로 확인합니다.

자주 수정할 때는 인프라만 Compose로 띄우고 백엔드는 IDE에서 실행하는 편이 빠릅니다. Compose의 `.env`는 IDE로 전달되지 않으니 환경변수를 별도로 설정합니다.

```powershell
docker compose up -d postgres keycloak kafka mosquitto redis
```

분석 서비스 로그를 보려면 다음 명령을 사용합니다.

```powershell
docker compose logs -f realtime-analysis-service
```

## 전체 초기화

DB init SQL, Flyway, Keycloak realm import를 처음부터 다시 확인하거나 `POSTGRES_USER` 같은 값을 바꿀 때 사용합니다. 로컬 데이터(`device_db`, `monitoring_db`, Kafka, Keycloak 사용자)가 모두 삭제되며 되돌릴 수 없습니다.

```powershell
docker compose down -v
docker compose up -d --build
```

## 동작 확인

```powershell
# PostgreSQL init: 01-create-databases.sql 실행 여부와 DB 4개
docker logs nilm-postgres 2>&1 | Select-String "01-create-databases|CREATE DATABASE"
docker exec nilm-postgres psql -U nilm_admin -d postgres -c "\l"

# MQTT -> Bridge -> Kafka 전달
docker compose exec -T mqtt-kafka-bridge python smoke.py
```

인증 흐름(Keycloak 테스트 사용자 생성, 토큰 발급, Gateway 호출)은 [실행 안내 상세](infrastructure/README.md)의 Keycloak 절을 따릅니다.

# Git Convention

## Git Flow

Git Flow 전략을 기반으로 브랜치를 관리

| Branch      | 역할                       |
| ----------- | -------------------------- |
| `master`    | 배포 가능한 운영 버전      |
| `develop`   | 다음 배포를 위한 개발 통합 |
| `feature/*` | 기능 개발                  |
| `release/*` | 배포 준비 및 최종 수정     |
| `hotfix/*`  | 운영 환경 긴급 수정        |

```text
master
│
├── hotfix/*
│
└── develop
    ├── feature/*
    └── release/*
```

<br />
<br />

## Branch Flow

#### Feature

- develop 에서만 분기
- 하나의 기능이 완성될경우 develop으로 병합

#### Release

- develop에 어느정도 기능이 쌓였을 경우 release로 병합
- release에서 발생한 버그는 release 브랜치 내에서 수정
- release 내에서 버그를 수정한 경우 develop에도 병합
- 버그가 수정되었고 서비스 가능하다면 master와 develop 양쪽으로 PR

#### Hotfix

- master 에서만 분기
- 빠른 버그수정이 필요할때 사용하며 버그 수정이 완료되면 master로 PR

#### main, develop

- Push하지 않고 Pull Request를 통해 반영

<br />
<br />

## Branch Naming

```text
{type}/{scope}/{description}
```

#### Type

```text
feature
release
hotfix
```

#### Scope

| Scope     | 대상                       |
| --------- | -------------------------- |
| `front`   | Frontend                   |
| `back`    | Backend                    |
| `ai`      | AI / ML                    |
| `data`    | 데이터 수집 / 전처리 / ETL |
| `bigdata` | 대용량 데이터 처리         |
| `spark`   | Spark / 분산 처리          |
| `kafka`   | Kafka / 분산 처리          |
| `infra`   | Docker / 서버 / 배포 환경  |
| `common`  | 공통 작업                  |

예시:

```text
feature/front/login
feature/back/auth-api
feature/ai/recommendation

release/1.0.0
hotfix/back/auth-error
```

<br />
<br />

## Commit Convention

```text
type(scope): message
```

| Type       | 설명                              |
| ---------- | --------------------------------- |
| `feat`     | 기능 추가                         |
| `fix`      | 버그 수정                         |
| `refactor` | 코드 구조 개선                    |
| `perf`     | 성능 개선                         |
| `docs`     | 문서 수정                         |
| `test`     | 테스트 추가 / 수정                |
| `style`    | 동작에 영향을 주지 않는 코드 수정 |
| `chore`    | 설정 / 패키지 / 빌드 / 기타 작업  |
| `data`     | 데이터셋 / 전처리 변경            |

| Scope     | 대상                       |
| --------- | -------------------------- |
| `front`   | Frontend                   |
| `back`    | Backend                    |
| `ai`      | AI / ML                    |
| `data`    | 데이터 수집 / 전처리 / ETL |
| `bigdata` | 대용량 데이터 처리         |
| `spark`   | Spark / 분산 처리          |
| `kafka`   | Kafka / 분산 처리          |
| `infra`   | Docker / 서버 / 배포 환경  |
| `common`  | 공통 작업                  |

<br />
<br />

## Pull Request

PR은 하나의 기능 또는 작업 단위로 생성한다.

```text
feature/front/login → develop
feature/recommendation → develop

release/1.0.0 → main
release/1.0.0 → develop

hotfix/back/auth-error → main
hotfix/back/auth-error → develop
```

PR 제목은 Commit Convention과 동일한 형식을 사용한다.

```text
feat(front): 로그인 페이지 구현
feat(ai): 추천 모델 구현
perf(bigdata): 데이터 처리 성능 개선
```
