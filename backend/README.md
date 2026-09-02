# Backend Services

NILM 프로젝트의 Spring Boot 백엔드 서비스 모음.
각 서비스는 **독립적으로 빌드·실행**되는 별도 Gradle 프로젝트다.

## 기술 버전

| 항목 | 버전 |
| --- | --- |
| Java | 21 (toolchain) |
| Spring Boot | 3.5.16 |
| Spring Cloud | 2025.0.3 (api-gateway만 사용) |
| Gradle | 8.14.3 (wrapper 포함) |
| springdoc-openapi | 2.8.9 |

## 서비스 구성

| 서비스 | 포트 | DB | 역할 |
| --- | --- | --- | --- |
| api-gateway | 8080 | 없음 | 외부 진입점, 라우팅, CORS |
| iot-device-service | 8081 | device_db | 기기 등록·설치·상태 관리 |
| monitoring-service | 8082 | monitoring_db | 가구 상태·이벤트·대응 이력 관리 |

라우팅 규칙 (api-gateway):

```text
/api/devices/**    → iot-device-service (IOT_DEVICE_SERVICE_URL, 기본 http://localhost:8081)
/api/monitoring/** → monitoring-service (MONITORING_SERVICE_URL, 기본 http://localhost:8082)
```

## 사전 준비

1. JDK 21 설치 (없으면 Gradle toolchain이 자동 다운로드를 시도한다)
2. PostgreSQL 컨테이너 실행 (저장소 루트에서):

```powershell
docker compose up -d
```

3. DB 계정 비밀번호를 환경변수로 준비한다. 비밀번호는 저장소 어디에도 하드코딩하지 않는다.

## 실행 방법 (서비스별, PowerShell 기준)

각 서비스 폴더에서 실행한다. 설정은 `application.properties` 하나이며, 모든 값은 `${환경변수:기본값}` 형태라 환경변수로 덮어쓸 수 있다.

```powershell
# api-gateway (DB 불필요)
cd backend/api-gateway
.\gradlew.bat bootRun

# iot-device-service
cd backend/iot-device-service
$env:DB_PASSWORD = "<.env의 POSTGRES_PASSWORD>"
.\gradlew.bat bootRun

# monitoring-service
cd backend/monitoring-service
$env:DB_PASSWORD = "<.env의 POSTGRES_PASSWORD>"
.\gradlew.bat bootRun
```

빌드와 테스트:

```powershell
.\gradlew.bat build   # 각 서비스 폴더에서
```

## 환경변수

| 변수 | 대상 | 기본값 (local) |
| --- | --- | --- |
| `SERVER_PORT` | 전체 | 8080 / 8081 / 8082 |
| `IOT_DEVICE_SERVICE_URL` | gateway | http://localhost:8081 |
| `MONITORING_SERVICE_URL` | gateway | http://localhost:8082 |
| `CORS_ALLOWED_ORIGINS` | gateway | http://localhost:5173,http://localhost:3000 |
| `DB_URL` | device/monitoring | jdbc:postgresql://localhost:5432/{각자 DB} |
| `DB_USERNAME` | device/monitoring | nilm_admin |
| `DB_PASSWORD` | device/monitoring | (없음 — 반드시 주입) |
| `APP_SECURITY_ENABLED` | device/monitoring | false (prod 기본 true) |
| `JWT_ISSUER_URI` | device/monitoring | prod에서만 필요 (Keycloak 예정) |
| `REDIS_HOST` / `REDIS_PORT` | monitoring | localhost / 6379 (아직 미사용) |

Docker 내부에서 실행할 때는 `DB_URL`을 `jdbc:postgresql://postgres:5432/...`로 바꾼다.

## 확인 주소 (local)

| 항목 | 주소 |
| --- | --- |
| Gateway 헬스체크 | http://localhost:8080/actuator/health |
| Device 헬스체크 | http://localhost:8081/actuator/health |
| Monitoring 헬스체크 | http://localhost:8082/actuator/health |
| Device Swagger | http://localhost:8081/swagger-ui.html |
| Monitoring Swagger | http://localhost:8082/swagger-ui.html |
| 동작 확인 (gateway 경유) | http://localhost:8080/api/devices/ping |
| 동작 확인 (gateway 경유) | http://localhost:8080/api/monitoring/ping |

검증 확인: `POST /api/devices/echo`에 `{"message": ""}`를 보내면 공통 오류 응답
(`code: VALIDATION_ERROR`, 400)이 반환된다.

## 환경 구분 방식

프로필 파일 분리 없이 `application.properties` 하나를 사용한다.
- 로컬 실행: 기본값 그대로 (localhost DB 등)
- 테스트: `src/test/resources/application.properties`가 자동으로 대신 적용되어 H2 인메모리 DB로 돈다
- 배포(추후): 환경변수 주입으로 값을 덮어쓴다

## 인증 (현재 상태)

- OAuth2 Resource Server 의존성만 추가된 상태. Keycloak은 아직 도입 전.
- `app.security.enabled=false`(local 기본)면 모든 요청 허용 → Keycloak 없이 실행 가능.
- `true`면 JWT 검증 활성화, `JWT_ISSUER_URI` 필요. 헬스체크·Swagger 경로는 항상 허용.

## DB 변경 관리

- `ddl-auto: none` 고정, 스키마 변경은 Flyway 마이그레이션(`src/main/resources/db/migration/`)으로만 한다.
- 현재 V1은 자리표시자(테이블 없음). 테이블 설계는 별도 작업에서 확정 후 V2부터 추가한다.
- `analysis_db`는 이 프로젝트 범위가 아니며 어떤 서비스도 연결하지 않는다.

## 의도적으로 구현하지 않은 것

Keycloak·JWT 발급, Kafka, MQTT, Redis 실사용, AI 연동, 도메인 CRUD, Eureka, 배포 구성.
서비스 간 데이터 접근은 자기 DB만 허용 (MSA 데이터 소유 원칙 — 크로스 DB FK/조인 금지).
