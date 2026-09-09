# Backend Services

API Gateway(8080), IoT Device Service(8081), Monitoring Service(8082)는 독립 Gradle 프로젝트다.

## Docker 실행

Compose는 infrastructure 아래에서 관리한다. backend/compose.yaml은 제거했다.

- [로컬 실행 및 기존 환경 전환](../infrastructure/README.md)
- [Jenkins 설정](../docs/배포설정/Jenkins_실행_및_검증.md)

저장소 루트에서 새 로컬 설정을 준비한 뒤:

```powershell
cd infrastructure/local
docker compose up -d --build
```

## IDE 또는 Gradle로 실행

먼저 local Compose에서 postgres/keycloak/kafka/mosquitto/redis를 실행한다. 같은 포트를 사용하는 백엔드 컨테이너는 정지한다.

각 서비스의 실행 환경에 아래 값을 설정한다. Compose의 .env는 IDE에 자동 전달되지 않는다.

| 변수 | 용도 |
| --- | --- |
| POSTGRES_URL | Device: jdbc:postgresql://localhost:5432/device_db, Monitoring: jdbc:postgresql://localhost:5432/monitoring_db |
| POSTGRES_USER | DB 사용자 |
| POSTGRES_PASSWORD | DB 비밀번호 |
| APP_SECURITY_ENABLED | true면 JWT 검증 |
| JWT_ISSUER_URI | http://localhost:8090/realms/nilm |
| IOT_DEVICE_SERVICE_URL | Gateway에서 http://localhost:8081 |
| MONITORING_SERVICE_URL | Gateway에서 http://localhost:8082 |
| CORS_ALLOWED_ORIGINS | 프론트 origin |
| REDIS_HOST / REDIS_PORT | Monitoring의 Redis 연결 |

Redis는 local Compose 내부에서만 공개한다. Monitoring을 IDE에서 실행할 때 Redis를 쓰려면 별도 로컬 포트 바인딩을 추가한다. 현재 IDE 기본 설정에서는 Redis health가 꺼져 있다.

서비스 디렉터리에서:

```powershell
.\gradlew.bat bootRun
```

테스트:

```powershell
.\gradlew.bat test
```

## 이미지와 헬스체크

Dockerfile은 Java 21로 test와 bootJar를 수행한 후 비루트 사용자로 실행한다.

- /actuator/health: 전체 상태
- /actuator/health/readiness: 준비 상태
- Device와 Monitoring의 readiness는 DB 연결을 포함한다.
- health는 인증 없이 접근 가능하고 API는 APP_SECURITY_ENABLED=true일 때 JWT가 필요하다.

Gateway 경유 API: /api/devices/ping, /api/monitoring/ping.
도메인 CRUD와 실제 Kafka 소비 로직은 아직 스켈레톤 상태다.

## DB 변경

Hibernate ddl-auto=none, Flyway로 스키마 변경을 관리한다. infrastructure/postgres SQL은 최초 DB 생성용이며 배포마다 재실행하지 않는다.
