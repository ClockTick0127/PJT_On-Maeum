# 대상자 대시보드 API 설계 (v0.1)

> 대상: `monitoring-service` (포트 8082), 게이트웨이 경유 `/api/monitoring/**`
> 화면: **대상자 최초 화면** — 대상자 이름 / 외출 모드 설정 / 담당자 이름·전화번호

---

## 1. 엔드포인트 요약

| Method | Path | 용도 | 권한 |
| --- | --- | --- | --- |
| `GET` | `/api/monitoring/my-dashboard` | 대상자 최초 화면 데이터 일괄 조회 | 대상자 본인 |
| `PUT` | `/api/monitoring/my-dashboard/away-mode` | 외출 모드 시작(또는 종료시각 변경) | 대상자 본인 |
| `DELETE` | `/api/monitoring/my-dashboard/away-mode` | 외출 모드 즉시 해제 | 대상자 본인 |

`my-*` 네임스페이스는 **"토큰 주인 = 대상자"** 규약이다. 담당자가 여러 대상자를 보는 화면은
`/api/monitoring/subjects`, `/api/monitoring/subjects/{subjectId}` 로 따로 두고 이 문서 범위 밖으로 한다.
경로에 `subjectId`를 받지 않으므로 **IDOR(남의 대상자 조회) 자체가 불가능**한 것이 이 설계의 핵심 이점이다.

### 인증 주체 해석

```
JWT.sub  →  subjects.auth_sub  →  Subject
```

- `app.security.enabled=false`(local)일 때는 `X-Auth-Sub` 헤더 또는 `app.local.default-auth-sub` 프로퍼티로 대체
  (`LocalSubjectInitializer`가 만드는 로컬 대상자와 짝을 맞춘다).
- `subjects.auth_sub`는 현재 **nullable** — 담당자만 등록해 둔 대상자는 로그인 계정이 없다.
  이 경우 토큰으로 매칭되는 행이 없으므로 자연스럽게 404가 된다.

---

## 2. `GET /api/monitoring/my-dashboard`

### 요청

```http
GET /api/monitoring/my-dashboard HTTP/1.1
Authorization: Bearer <access_token>
```

쿼리 파라미터 없음. 캐시 금지(`Cache-Control: no-store`) — 외출 모드/위험 상태가 실시간 값이다.

### 응답 200

```json
{
  "subject": {
    "id": 1,
    "name": "홍길동",
    "birthDate": "1945-03-02",
    "gender": "MALE",
    "phone": "010-1234-5678",
    "address": "서울시 강남구 ...",
    "riskLevel": "NORMAL",
    "lastActivityAt": "2026-09-17T08:41:03+09:00",
    "lastActivityAppliance": "kettle"
  },
  "awayMode": {
    "active": false,
    "startedAt": null,
    "until": null
  },
  "manager": {
    "id": 3,
    "name": "김복지",
    "organization": "강남구 노인복지관",
    "phone": "02-555-1234"
  },
  "guardians": [
    {
      "id": 7,
      "name": "홍딸기",
      "relationship": "딸",
      "phone": "010-9876-5432"
    }
  ]
}
```

### 필드 계약

| 필드 | 타입 | 출처 | 비고 |
| --- | --- | --- | --- |
| `subject.id` | number | `subjects.id` | |
| `subject.name` | string | `subjects.name` | **화면 필수** |
| `subject.birthDate` | date (`yyyy-MM-dd`) | `subjects.birth_date` | 프론트에서 나이 계산 |
| `subject.gender` | `MALE` \| `FEMALE` | `subjects.gender` | **컬럼 추가 필요** (§5) |
| `subject.phone` | string | `subjects.phone` | |
| `subject.address` | string | `subjects.address` | 좌표는 대상자 화면에 불필요 → 미포함 |
| `subject.riskLevel` | `NORMAL` \| `WARNING` \| `DANGER` | `subjects.risk_level` | **컬럼 추가 필요**. `RiskLevel` enum 재사용 |
| `subject.lastActivityAt` | OffsetDateTime \| null | `subjects.last_activity_at` | **컬럼 추가 필요**. ON→OFF 전환 시 갱신 |
| `subject.lastActivityAppliance` | string \| null | `subjects.last_activity_appliance` | 계약서 부록 A 코드값 (`kettle`, `microwave`, …) |
| `awayMode.active` | boolean | `!subjects.monitoring_enabled` | §2.1 참고 |
| `awayMode.startedAt` / `until` | OffsetDateTime \| null | `away_started_at` / `away_until` | 비활성 시 둘 다 null |
| `manager` | object \| null | `managers` (`subjects.manager_id`) | 미배정이면 `null` — 프론트는 "담당자 미배정" 표시 |
| `manager.phone` | string | `managers.phone` | **컬럼 추가 필요**. 화면 필수인데 엔티티에 없음 |
| `guardians` | array | 보호자 | 없으면 `[]`. 정렬: `notifyPriority` → `id` |

- 시각은 전부 **ISO-8601 + 오프셋**(`OffsetDateTime`)으로 직렬화한다. 기존 DTO(`NotificationResponseDto`)와 동일.
- `manager`/`guardians`는 값이 없을 때 필드를 빼지 않고 `null` / `[]`로 항상 내려보낸다(프론트 분기 단순화).
- `subjects.manager_id`는 FK가 아니라 값 참조이므로, 삭제된 담당자를 가리킬 수 있다 → 조회 실패 시 500이 아니라 `manager: null`.

#### 2.1 `awayMode.active`를 서버가 계산하는 이유

현재 외출 상태는 `monitoring_enabled`, `away_started_at`, `away_until` **세 컬럼의 조합**으로 표현된다.
프론트가 이 조합을 재현하면 규칙이 두 곳에 생긴다. 응답에서는 불리언 하나로 평탄화하고,
판단 로직은 `Subject` 도메인 안에만 둔다.

```java
// Subject
public boolean isAway() {
    return !monitoringEnabled && awayStartedAt != null;
}
```

> `away_until`이 지났는데 스케줄러가 아직 `endAway`를 돌리지 않은 순간이 존재한다.
> 조회 시점에 `now >= away_until`이면 **응답에서는 이미 해제된 것으로 계산해 내려보낸다**(읽기는 부수효과 없음).
> 실제 상태 복구는 기존 스케줄러(`NotificationExpirationScheduler`와 같은 위치)에 맡긴다.

### 응답 코드

| 코드 | `code` | 상황 |
| --- | --- | --- |
| 200 | — | 정상 |
| 401 | — | 토큰 없음/만료 (Spring Security가 처리) |
| 404 | `SUBJECT_NOT_FOUND` | 토큰 `sub`에 해당하는 대상자 없음 (담당자 계정으로 호출한 경우 포함) |

오류 본문은 기존 `ErrorResponse` 포맷 그대로:

```json
{
  "timestamp": "2026-09-17T09:12:00+09:00",
  "status": 404,
  "code": "SUBJECT_NOT_FOUND",
  "message": "대상자 정보를 찾을 수 없습니다.",
  "path": "/api/monitoring/my-dashboard",
  "errors": []
}
```

`GlobalExceptionHandler`가 `ResponseStatusException`을 `REQUEST_REJECTED`로 뭉개므로,
도메인 코드(`SUBJECT_NOT_FOUND`)를 쓰려면 전용 예외 + 핸들러를 하나 추가해야 한다(§6).

---

## 3. 외출 모드 API

### `PUT /api/monitoring/my-dashboard/away-mode`

```json
{ "until": "2026-09-17T18:00:00+09:00" }
```

- `until`: **필수**, 현재 시각 이후(`@Future`). 상한은 서버에서 제한(예: 최대 72시간) — 무기한 모니터링 정지 방지.
- `Subject.startAway(now, until)` 호출. 멱등하지 않다: 이미 외출 중이면 `IllegalStateException` → **409**.
  (종료 시각만 늘리는 "연장"은 별도 논의. 우선은 해제 후 재설정으로 처리.)

응답 200 — 대시보드의 `awayMode` 블록과 동일한 shape:

```json
{ "active": true, "startedAt": "2026-09-17T09:12:00+09:00", "until": "2026-09-17T18:00:00+09:00" }
```

| 코드 | `code` | 상황 |
| --- | --- | --- |
| 400 | `VALIDATION_ERROR` | `until` 누락/과거/상한 초과 |
| 404 | `SUBJECT_NOT_FOUND` | |
| 409 | `ALREADY_AWAY` | 이미 외출 모드 |

### `DELETE /api/monitoring/my-dashboard/away-mode`

- 본문 없음. `Subject.endAway(now)` 호출.
- **멱등**: 외출 중이 아니어도 200 (`endAway`가 이미 early-return 한다).

```json
{ "active": false, "startedAt": null, "until": null }
```

### 부수효과 (합의 필요)

외출 모드 = `monitoring_enabled=false`이므로, 그 구간의 분석 이벤트는 **알림을 만들지 않는다**.
이벤트 저장 자체를 막을지, 저장하되 발송만 막을지는 `AnalysisEventService`쪽 결정이 필요하다.
→ **저장은 하고 발송만 막는 쪽을 권장**(사후 통계/오탐 분석에 필요).

---

## 4. 이 화면이 쓰지 않는 엔티티

설계상 아래는 대시보드 응답에 **넣지 않는다**. 화면 요구에 없고, 응답만 무거워진다.

- `risk_policy` — 임계치는 서버 내부 판정용. 대상자에게 노출할 값이 아님.
- `hourly_power_usage` — 그래프 화면의 별도 API(`GET /api/monitoring/my-dashboard/power?from=&to=`)로 분리.
- `notification_settings`, `push_subscription` — 담당자 설정 화면 소관. 푸시 구독은 기존 `PushSubscriptionController` 사용.
- `analysis_event` — 이력 화면(`GET /api/monitoring/my-events?page=`)으로 분리.

단, **응답 대기 중인 알림**(`notifications.response_status = PENDING`)은 대상자가 최초 화면에서
바로 응답해야 하는 성격이라 추후 `pendingNotification` 블록으로 합칠 여지가 있다.
현재 화면 정의에 없으므로 v0.1에서는 제외하고, 필요해지면 필드 추가(하위 호환)로 처리한다.

---

## 5. 선행 스키마 변경 (V5 마이그레이션)

이 API를 구현하려면 아래가 **먼저** 들어가야 한다. 굵은 항목은 화면 필수인데 현재 엔티티에 없는 것.

```sql
-- managers: 화면에 담당자 전화번호를 표시해야 하는데 컬럼이 없음
ALTER TABLE managers ADD COLUMN phone VARCHAR(20);

-- subjects: 추가 합의 컬럼
ALTER TABLE subjects ADD COLUMN gender VARCHAR(10) CHECK (gender IN ('MALE', 'FEMALE'));
ALTER TABLE subjects ADD COLUMN latitude  NUMERIC(10, 7);
ALTER TABLE subjects ADD COLUMN longitude NUMERIC(10, 7);
ALTER TABLE subjects ADD COLUMN privacy_enabled BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE subjects ADD COLUMN risk_level VARCHAR(10) NOT NULL DEFAULT 'NORMAL'
    CHECK (risk_level IN ('NORMAL', 'WARNING', 'DANGER'));
ALTER TABLE subjects ADD COLUMN last_activity_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE subjects ADD COLUMN last_activity_appliance VARCHAR(50);
CREATE UNIQUE INDEX uk_subjects_auth_sub ON subjects(auth_sub) WHERE auth_sub IS NOT NULL;

-- risk_policy: 테이블 자체가 없음 (subjects.risk_policy_id가 가리킬 대상)
CREATE TABLE risk_policies (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    warning_threshold INTEGER NOT NULL CHECK (warning_threshold BETWEEN 0 AND 100),
    danger_threshold  INTEGER NOT NULL CHECK (danger_threshold  BETWEEN 0 AND 100),
    min_duration_minutes INTEGER NOT NULL,
    CONSTRAINT ck_risk_policy_threshold_order CHECK (warning_threshold < danger_threshold)
);

-- notifications: 담당자 응답 상태 (미응답/확인/조치 완료)
ALTER TABLE notifications ADD COLUMN manager_response_status VARCHAR(20) NOT NULL DEFAULT 'NONE'
    CHECK (manager_response_status IN ('NONE', 'ACKNOWLEDGED', 'RESOLVED'));
```

`subjects.risk_level`은 `AnalysisEventService`가 이벤트를 저장할 때 함께 갱신하는 **비정규화 컬럼**이다.
매 조회마다 `analysis_events`를 스캔하지 않기 위한 선택이고, 이벤트 저장과 같은 트랜잭션에서 쓰면 정합성이 유지된다.

---

## 6. 구현 스케치

```
api/MyDashboardController.java
  GET    /api/monitoring/my-dashboard
  PUT    /api/monitoring/my-dashboard/away-mode
  DELETE /api/monitoring/my-dashboard/away-mode

dto/MyDashboardResponse.java      // record + 중첩 record (SubjectSummary/AwayMode/ManagerSummary/GuardianSummary)
dto/AwayModeRequest.java          // @NotNull @Future OffsetDateTime until
dto/AwayModeResponse.java

service/MyDashboardService.java   // @Transactional(readOnly = true) getMyDashboard(String authSub)
common/SubjectNotFoundException.java + GlobalExceptionHandler 핸들러 추가
common/AuthSubResolver.java       // JWT sub 추출 + local 모드 fallback

repository/SubjectRepository      // + Optional<Subject> findByAuthSub(String authSub)
repository/ManagerRepository      // 신규 (현재 없음)
repository/GuardianRepository     // 신규 — 보호자 원본 결정 후 (§7)
```

**쿼리 비용**: 대상자 1 + 담당자 1 + 보호자 N — 조회 3회로 끝난다.
`managers`는 `subjects.manager_id`로 값 참조하므로 JPA 연관관계를 걸지 않고 `findById`로 따로 읽는다
(기존 코드가 서비스 간 FK를 피하는 방침과 일치).

### 컨트롤러 시그니처

```java
@GetMapping("/my-dashboard")
public MyDashboardResponse myDashboard(@AuthenticationPrincipal Jwt jwt) {
    return service.getMyDashboard(authSubResolver.resolve(jwt));
}
```

`app.security.enabled=false`이면 `jwt`가 null로 들어오므로 resolver가 로컬 기본값으로 대체한다.
테스트(`AnalysisEventFlowTest` 등)가 보안 off 상태로 도는 만큼 이 경로가 실제로 쓰인다.

---

## 7. 결정이 필요한 항목

1. **담당자 전화번호** — `managers.phone` 컬럼 추가로 확정해도 되는지. (화면 필수인데 현재 엔티티에 없음)
2. **보호자 원본 위치** — 나열해 준 보호자 엔티티(ID/auth_sub/relationship/phone)는
   `iot-device-service`의 `household_members`(relation, notify_phone, notify_priority)와 **필드가 거의 겹친다**.
   계약서 §5는 "가구-보호자 매핑은 device_db가 원본"이라고 못박고 있다.
   → monitoring에 테이블을 또 만들지, `GET /api/devices/households/{houseId}/guardians`를 호출할지 결정 필요.
   **권장: device 서비스 호출.** 다만 대시보드가 외부 호출에 묶이므로, 실패 시 `guardians: []`로 degrade 한다.
3. **보호자 표시 이름** — 나열한 보호자 엔티티에 `name`이 없다. 화면에 이름을 띄우려면
   Keycloak 조회가 필요하거나 `name` 컬럼을 추가해야 한다.
4. **외출 모드 최대 시간** — 상한 없이 허용할지, 72시간 등으로 제한할지.
5. **외출 중 이벤트 처리** — 저장+발송억제 / 완전 무시 중 택일 (§3).
