# IoT Device Service: 계정·가구·기기·MQTT 자격

사람과 기기가 시스템에 들어오는 문을 담당한다. 누가 어느 가구에 접근할 수 있는지,
어떤 센서가 어느 토픽에 발행할 수 있는지를 이 서비스가 결정한다.

## 이번 구현 범위

- **계정** — 가입·로그인·토큰 갱신·내 정보. 신원(계정·비밀번호·토큰)은 Keycloak이 소유하고,
  이 서비스는 프로필과 관계만 가진다
- **가구 멤버십** — 사람과 가구의 관계. 접근 권한과 알림 수신자의 원본
- **초대 코드** — 가구 접근 신청권. 로그인 수단이 아니다
- **기기 생명주기** — 등록부터 폐기까지의 상태 전이와 이력
- **MQTT 자격** — 기기별 계정 발급, ACL 생성, Mosquitto passwd 파일 동기화

하지 않는 것: 전력 데이터 수집·분석·위험 판정. 이 서비스는 데이터가 들어올 자격만 관리하고,
측정치 자체는 MQTT → 브릿지 → Kafka 경로로 흐른다.

## 실행 설정

### 사전 조건

| 구성 요소 | 용도 | 없으면 |
| --- | --- | --- |
| PostgreSQL `device_db` | 프로필·가구·기기·자격 저장 | 기동 실패 |
| Keycloak (realm `nilm`) | 가입·로그인 위임 | 인증 API만 실패, 나머지는 동작 |
| Kafka | 담당자 가입 이벤트 발행 | 릴레이만 재시도, API는 정상 |
| Mosquitto | passwd 동기화 대상 | 동기화 API만 실패 |

`infrastructure/local`의 compose를 먼저 올리면 PostgreSQL·Keycloak·Kafka가 함께 뜬다.

### 환경변수

| 변수 | 기본값 | 비고 |
| --- | --- | --- |
| `SERVER_PORT` | `8081` | **로컬에서는 8083 권장** (아래 포트 충돌 참고) |
| `POSTGRES_URL` | `jdbc:postgresql://localhost:5432/device_db` | |
| `POSTGRES_USER` | `nilm_admin` | |
| `POSTGRES_PASSWORD` | (없음) | 볼륨 생성 시 쓴 값과 일치해야 한다 |
| `APP_SECURITY_ENABLED` | `false` | `true`면 JWT 검증 활성화 |
| `JWT_ISSUER_URI` | `http://localhost:8090/realms/nilm` | |
| `KEYCLOAK_SERVER_URL` | `http://localhost:8090` | |
| `KEYCLOAK_REALM` | `nilm` | |
| `KEYCLOAK_BACKEND_CLIENT_ID` | `nilm-backend` | confidential 클라이언트 |
| `NILM_BACKEND_CLIENT_SECRET` | (없음) | 서비스 계정 토큰 발급에 필요 |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | |

> **포트 충돌**: 기본 `8081`은 팀 Spark Master UI가 점유한다. 로컬 실행 시
> `SERVER_PORT=8083`으로 띄울 것.

### 기동

```bash
cd backend/iot-device-service
SERVER_PORT=8083 POSTGRES_PASSWORD=ssafy ./gradlew bootRun
```

확인:

- Swagger UI — `http://localhost:8083/swagger-ui.html`
- 헬스 체크 — `http://localhost:8083/actuator/health`

스키마는 Flyway가 관리한다(`ddl-auto=none`). 마이그레이션 검증에 실패하면
로컬 `device_db`가 다른 브랜치의 마이그레이션을 이미 적용한 상태일 수 있으므로,
DB를 드롭·재생성한 뒤 다시 띄우는 게 가장 빠르다.

## 인증 모드

`app.security.enabled` 하나로 동작이 갈린다.

**`false` (기본, 로컬 개발용)** — 모든 요청이 통과한다. 호출자 식별은 `X-User-Id` 헤더로
대신하므로, Keycloak 없이도 가구·기기·초대 API를 끝까지 시험할 수 있다.

```bash
curl -H "X-User-Id: 3f2b0c1e-....-....-............" http://localhost:8083/api/auth/me
```

**`true` (통합·운영)** — Bearer JWT를 검증한다. `X-User-Id` 폴백은 동작하지 않는다.

| 경로 | 권한 |
| --- | --- |
| `/api/auth/signup`, `/login`, `/refresh` | 인증 없이 허용 (토큰이 없는 상태에서 호출된다) |
| `/api/auth/check-email` | 인증 없이 허용 (아직 계정이 없는 단계) |
| `/api/auth/logout` | 인증 없이 허용 (만료된 토큰으로도 로그아웃되어야 한다) |
| `/actuator/health/**`, `/v3/api-docs/**`, `/swagger-ui/**` | 인증 없이 허용 |
| `/api/devices/admin/**` | `ROLE_ADMIN` 필요 |
| 그 외 전부 | 인증 필요 |

## API 명세

모든 경로는 게이트웨이를 거칠 경우 접두사 없이 그대로 전달된다.
날짜·시각은 ISO-8601 오프셋 형식(`2026-09-21T10:00:00Z`)이다.

### 인증 `/api/auth`

| 메서드 | 경로 | 설명 | 인증 |
| --- | --- | --- | --- |
| GET | `/check-email?email=` | 이메일 사용 가능 여부 | 불필요 |
| POST | `/signup` | 회원가입 → `201` | 불필요 |
| POST | `/login` | 토큰 발급 | 불필요 |
| POST | `/refresh` | 토큰 갱신 | 불필요 |
| POST | `/logout` | refresh token 폐기 → `204` | 불필요 |
| GET | `/me` | 내 프로필 + 접근 가능한 가구 목록 | 필요 |
| PATCH | `/me` | 이름·전화번호 수정 | 필요 |
| POST | `/password` | 비밀번호 변경 → `204` | 필요 |
| POST | `/users` | 대상자 계정 대리 생성 → `201` | 담당자 |

이메일은 **소문자로 정규화**해 저장·조회한다. Keycloak이 사용자명을 소문자로 다루므로,
정규화하지 않으면 `Kim@a.com`이 로컬 중복 검사를 통과한 뒤 Keycloak에서 409로 막혀
중복 확인 결과와 가입 결과가 어긋난다.

**POST `/api/auth/signup`**

```json
{
  "email": "welfare@city.go.kr",
  "password": "password123",
  "displayName": "김복지",
  "phone": "010-1234-5678",
  "organization": "○○구 복지센터"
}
```

`organization`을 넣으면 기관 담당자 가입으로 간주되어, 아웃박스를 통해
모니터링 서비스의 담당자 명단에 등록된다. 개인 사용자는 생략한다.

응답 `201`:

```json
{
  "userId": "3f2b0c1e-...",
  "email": "welfare@city.go.kr",
  "displayName": "김복지",
  "phone": "010-1234-5678",
  "organization": "○○구 복지센터",
  "status": "ACTIVE"
}
```

비밀번호는 Keycloak으로 통과만 하고 이 서비스에 저장되지 않는다.
프로필 저장이 실패하면 생성된 Keycloak 계정을 보상 삭제한다.

**POST `/api/auth/login`** → `{ "email", "password" }`

```json
{
  "accessToken": "eyJ...",
  "refreshToken": "eyJ...",
  "expiresIn": 300,
  "tokenType": "Bearer"
}
```

**POST `/api/auth/logout`** → `{ "refreshToken" }` → `204`

클라이언트가 저장소에서 토큰을 지우는 것만으로는 부족하다. refresh token이 Keycloak에
살아 있으면 유출된 토큰으로 계속 갱신할 수 있으므로 **서버가 폐기해야 실제로 끊긴다.**
access token이 만료된 뒤에도 호출할 수 있어야 하므로 인증을 요구하지 않으며, 폐기 대상은
본문의 refresh token이라 남의 세션을 끊을 수 없다. 이미 무효한 토큰이어도 `204`다.

**POST `/api/auth/password`** → `{ "currentPassword", "newPassword" }` → `204`

현재 비밀번호는 Keycloak 로그인을 시도해 확인한다 — 서비스가 해시를 갖고 있지 않으므로
실제로 통과하는지 물어보는 것이 유일한 검증 방법이다. 변경에 성공하면 **해당 사용자의
모든 세션을 끊는다.** 이전 비밀번호로 발급된 토큰이 살아 있으면 변경의 의미가 없기
때문이며, 호출자 본인도 새 비밀번호로 다시 로그인해야 한다.

**POST `/api/auth/users`** → `{ "displayName", "phone", "email"? }` → `201`

담당자가 대상자 몫으로 **신원만** 만든다. 비밀번호를 만들지 않으므로 로그인은 불가능하지만
UUID가 즉시 생긴다. monitoring이 알림 수신자를 `subjects.auth_sub`로 식별하는데
**이 값이 비면 위험을 감지해도 알림이 만들어지지 않고 로그만 남기 때문이다.**

비밀번호를 대신 정해 주지 않는 이유는 그 값을 누가 보관할지가 문제가 되기 때문이다.
대상자가 직접 앱을 쓰겠다고 하면 그때 자격증명을 연다(후속 과제).
`email`을 주지 않으면 `subject-xxxx@no-email.nilm.local` 형태의 내부 주소를 만든다 —
실제로 보낼 수 없는 주소임이 드러나야 하기 때문이다.
기관 소속(`organization`) 계정만 호출할 수 있고, 아니면 `403`이다.

**GET `/api/auth/me`** — 로그인 직후 화면 구성의 기준. 프로필과 함께 내가 접근 가능한
가구를 관계까지 담아 돌려준다.

```json
{
  "profile": { "userId": "...", "displayName": "김복지", "status": "ACTIVE" },
  "households": [
    { "houseId": "H001", "alias": "어머니 댁", "relation": "STAFF",
      "notifyPriority": "PRIMARY", "notifyEnabled": true }
  ]
}
```

### 가구 `/api/devices/households`

| 메서드 | 경로 | 설명 |
| --- | --- | --- |
| POST | `` | 가구 등록 → `201`. 생성자가 첫 멤버(PRIMARY)로 함께 등록된다 |
| GET | `` | 전체 가구 목록 (관리용 — 내 가구는 `/api/auth/me`) |
| GET | `/{houseId}` | 단건 조회. 해당 가구의 멤버만 가능 |

```json
{ "houseId": "H001", "alias": "어머니 댁", "graceMinutes": 120 }
```

`houseId`는 `H001` 형식(`^H\d{3}$`)이다. `graceMinutes`는 0~1440.

### 가구 멤버 `/api/devices/households/{houseId}/members`

| 메서드 | 경로 | 설명 |
| --- | --- | --- |
| GET | `` | 멤버 목록. **PRIMARY 우선 정렬 — 알림 발송 순서 기준** |
| PATCH | `/{userId}` | 부분 수정 (null 필드는 기존 값 유지) |
| DELETE | `/{userId}` | 접근 해제 → `204` |

수정 가능 필드: `relation`, `notifyPriority`, `notifyPhone`, `notifyEnabled`.

- `relation` — `SELF`(대상자 본인) / `STAFF`(복지사 등 기관 담당자). **필수**이며 기본값이 없다
- `notifyPriority` — `PRIMARY` / `SECONDARY`

**마지막 멤버는 해제할 수 없다.** 해제하면 아무도 접근할 수 없는 가구가 남기 때문이다(`400`).

### 초대 `/api/devices`

| 메서드 | 경로 | 설명 |
| --- | --- | --- |
| POST | `/households/{houseId}/invites` | 코드 발급 → `201`. 해당 가구 멤버만 가능 |
| GET | `/households/{houseId}/invites` | 발급·사용·회수 이력 |
| POST | `/invites/{code}/accept` | 수락 → `201`. 로그인한 계정에 멤버십 생성 |
| DELETE | `/invites/{code}` | 회수. 이미 사용된 코드는 회수 불가 |

```json
{ "relation": "STAFF", "expiresInHours": 72 }
```

`expiresInHours`는 1~336, 생략하면 72시간. 코드는 혼동하기 쉬운 문자(`0/O`, `1/I/L`)를
제외한 8자리다 — 전화로 불러줘야 하는 상황을 고려했다.

**코드는 로그인 수단이 아니다.** 코드를 가진 사람이 아니라 그 코드를 *사용한 계정*에
권한이 생기므로, 누가 언제 들어왔는지가 항상 남고 개인 단위로 회수할 수 있다.
코드 소비와 멤버십 생성은 한 트랜잭션이라 "코드만 쓰이고 권한은 없는" 상태가 생기지 않는다.

사용 불가 사유는 우선순위대로 판정한다: 회수됨 → 사용됨 → 만료됨.

### 기기 `/api/devices`

| 메서드 | 경로 | 설명 |
| --- | --- | --- |
| POST | `` | 등록 → `201`. 계정 발급 + ACL 생성 + 이력 기록이 한 트랜잭션 |
| GET | `?houseId=H001` | 가구별 목록 |
| GET | `/{deviceId}` | 단건 조회 |
| PATCH | `/{deviceId}/status` | 상태 전이 |
| GET | `/{deviceId}/history` | 설치·전이 이력 |

**POST `/api/devices`** → `{ "houseId", "deviceType", "location", "firmwareVer" }`
(`deviceType`은 `CLAMP` 또는 `PLUG`)

```json
{
  "device": { "deviceId": 12, "houseId": "H001", "status": "REGISTERED" },
  "mqttUsername": "dev_h001_clamp12",
  "mqttPassword": "9f3a...",
  "aclTopics": ["v1/power/sim/H001/#"]
}
```

> `mqttPassword`는 **이 응답에서 단 한 번만** 노출된다. 서버에는 해시만 남으므로
> 분실하면 재발급(로테이션)해야 한다.

**접속 상태(LWT)** — `status`(생명주기)와 **독립**이다. `status`는 운영자가 정하고
`connectionStatus`는 브로커가 알려 주는 사실이다. `ACTIVE`인데 `OFFLINE`이면
정상 등록된 기기가 지금 끊겨 있다는 뜻으로, 확인이 필요한 상태다.

```
기기 접속 시  LWT 등록: v1/device/{deviceId}/status  payload "offline"  retain
기기 접속 후  발행:     v1/device/{deviceId}/status  payload "online"   retain
```

기기가 스스로 알릴 수 없는 상황(정전·크래시)에서도 **브로커가 유언 메시지를 대신 발행**한다.
이게 없으면 데이터가 끊긴 원인이 정전인지 네트워크 장애인지 사람이 활동을 안 하는 것인지
구분하지 못해, Wi-Fi가 끊길 때마다 위험 알림이 가는 오경보 시스템이 된다.

`app.mqtt.connection-listener.enabled=true`로 켠다. 브로커에 붙지 못해도 서비스는
정상 기동한다 — 접속 상태는 부가 정보이고, 이것 때문에 기기 등록이 막히면 안 된다.

**이력의 조작자** — `changedBy`는 **클라이언트가 보내지 않는다.** 보내게 하면 남을
사칭할 수 있으므로 서버가 인증된 호출자(JWT subject)로 채운다.

**상태 전이** — 허용되지 않은 전이는 `400 INVALID_STATE_TRANSITION`이다.

```
REGISTERED ----> ACTIVE <----> SUSPENDED
     |             |              |
     +-------------+--------------+----> RETIRED   (종착)
```

규칙은 서비스가 아니라 도메인 엔티티에 있다. 어느 경로로 호출하든 동일하게 막히고,
모든 전이가 설치 이력에 자동 기록된다.

### MQTT 관리 `/api/devices/admin/mqtt`

| 메서드 | 경로 | 설명 |
| --- | --- | --- |
| POST | `/sync` | DB의 유효 계정을 Mosquitto passwd 파일에 반영 |

DB가 원본이고 passwd·ACL 파일은 파생물이다. passwd 동기화는 `dev_*` 네임스페이스만
관리하므로 팀 공용 계정 라인은 보존되며, 정지·폐기된 기기는 파일에서 빠져 접속이 차단된다.
`app.mqtt.reload-enabled=true`면 브로커 SIGHUP까지 수행한다.

**인가(ACL)도 함께 생성한다.** `mosquitto.local.conf`에 `acl_file`이 켜져 있어
기기는 자기 가구 토픽에만 발행할 수 있다.

```
user dev_h001_clamp1
topic write v1/power/sim/H001/#
```

mosquitto의 `acl_file`은 **파일에 없는 사용자의 모든 토픽을 막는다.** 기기가 아닌
공용 계정(시뮬레이터 발행, 브릿지 구독)은 `device_acl`에 없으므로
`app.mqtt.static-acl` 설정으로 주입한다. 이게 없으면 ACL을 켜는 순간 수집 파이프라인이 끊긴다.

```
app.mqtt.static-acl=simulator_user:write:v1/power/sim/#,kafka_bridge_user:read:v1/power/sim/#
```

검증: H001 기기가 `v1/power/sim/H002/main`에 발행을 시도하면 브로커가 거부한다
(`Denied PUBLISH`). 자기 가구 토픽은 통과한다.

## 공통 오류 응답

```json
{
  "timestamp": "2026-09-21T10:00:00Z",
  "status": 400,
  "code": "INVALID_STATE_TRANSITION",
  "message": "ACTIVE 상태에서 REGISTERED로 전이할 수 없습니다",
  "path": "/api/devices/12/status",
  "errors": []
}
```

| code | HTTP | 발생 상황 |
| --- | --- | --- |
| `VALIDATION_ERROR` | 400 | 요청 본문 검증 실패 (`errors[]`에 필드별 사유) |
| `INVALID_STATE_TRANSITION` | 400 | 허용되지 않은 기기 상태 전이 |
| `INVALID_OPERATION` | 400 | 만료·사용된 초대 코드, 마지막 멤버 해제 등 |
| `UNAUTHENTICATED` | 401 | 토큰 없음·만료 |
| `FORBIDDEN` | 403 | 인증됐으나 해당 가구의 멤버가 아님 |
| `RESOURCE_NOT_FOUND` | 404 | 없는 가구·기기·초대 코드 |
| `DUPLICATE_RESOURCE` | 409 | 이미 등록된 이메일·가구 |

가구 접근 판정은 **없는 가구면 404, 멤버가 아니면 403**으로 구분한다.

## 마이그레이션

| 버전 | 내용 |
| --- | --- |
| `V1__init` | 초기 스키마 |
| `V2__device_schema` | 가구·기기·설치 이력·기기 자격·ACL |
| `V3__add_mosquitto_hash` | 브로커 호환 해시 컬럼 |
| `V5__account_and_invite` | 사용자 프로필, 가구 멤버십(보호자 매핑에서 일반화), 초대 |
| `V6__manager_registration_outbox` | 담당자 가입 아웃박스 |

`V4`는 미병합 브랜치(`feature/back/mqtt-credential-lifecycle`)에 있다. 아래 한계 참고.

## 현재 동작의 한계

- **`acl_file`이 브로커 설정에 없다.** `device_acl` → `acl.device` 파일 생성은 동작하지만
  `mosquitto.conf` 어디에도 `acl_file` 지시자가 없어, 현재는 인증만 걸려 있고 인가는
  적용되지 않는다. 켜는 순간 파일에 없는 계정(시뮬레이터·브릿지)이 함께 차단되므로,
  그 계정들의 처리 방식을 먼저 정해야 한다. **실기기 도입 전 필수 선결 과제.**
- **토픽에 구현 주체 이름이 박혀 있다.** `v1/power/sim/{houseId}/#`의 `sim`은 시뮬레이터를
  뜻하므로 실기기 전환 시 어긋난다. 변경하려면 `DeviceService.TOPIC_PATTERN`,
  브릿지 구독 패턴(`MQTT_TOPIC`), 기존 `device_acl` 행을 동시에 옮겨야 한다
- **기기 오프라인을 알 방법이 없다.** MQTT LWT가 설계에 없어, 데이터가 끊긴 원인이
  정전인지 네트워크 문제인지 구분하지 못한다
- **기기 등록 API의 호출자가 `"system"` 고정이다.** 이력의 `changedBy`가 실제 조작자를
  가리키지 않는다
- **미병합 — MQTT 계정 로테이션** (`feature/back/mqtt-credential-lifecycle`):
  `POST /api/devices/{deviceId}/credentials/rotate`와 ACL 파일 동기화가 들어 있다.
  해당 브랜치의 `V4`는 develop에 이미 `V5`·`V6`가 적용된 뒤라 순서가 어긋나므로,
  병합 전 **`V7`로 번호를 올려야** Flyway 검증을 통과한다
- `UserProfile.Status.PENDING`은 승인 절차를 두지 않기로 하면서 더는 부여되지 않는다.
  과거 행을 읽기 위해 열거값만 남아 있다
