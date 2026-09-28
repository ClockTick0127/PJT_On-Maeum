# 모니터링 MVP 테이블 목록

> 작성일: 2026-09-10  
> 대상 기능: `ROUTINE_MISSED` 위험 이벤트 수신, Web Push 발송, 사용자 응답 및 30초 무응답 처리, SSE 화면 갱신

## 설계 원칙

- 모든 테이블의 기본키 이름은 테이블명과 관계없이 `id`로 통일한다.
- 외래키는 참조하는 대상에 따라 `analysis_event_id`, `incident_id`처럼 작성한다.
- `correlation_key` 문자열은 저장하지 않는다.
- 논리적 중복은 `household_id + event_type + event_date + expected_until` 복합 UNIQUE로 방지한다.
- Web Push 알림은 기기별이 아니라 사건과 수신 사용자별 논리 알림으로 관리한다.
- 기기별 발송 시도 이력은 MVP 범위에서 제외한다.
- 사건 액션과 UI 아웃박스는 기존 행을 수정하지 않고 계속 추가한다.

## 테이블 관계

```text
analysis_event
  └─ incident
       ├─ incident_action
       ├─ notification_delivery
       └─ ui_outbox

household_access
  └─ 알림 수신자 및 SSE 조회 권한 결정

push_subscription
  └─ 사용자별 Web Push 발송 대상 결정
```

## 1. 위험 분석 이벤트 (`analysis_event`)

- ID: `id` PK
- 가구 ID: `household_id`
- 이벤트 타입: `event_type`
- 위험 점수: `score`
- 이벤트 기준 날짜: `event_date`
- 루틴 기준 시각: `expected_until`
- 이벤트 발생 시각: `occurred_at`
- 발생 근거: `reason`
- 이벤트 수신 시각: `received_at`

제약 조건:

- `score`는 0 이상 100 이하
- `household_id + event_type + event_date + expected_until` UNIQUE

중복 처리 기준:

- 동일한 Kafka 이벤트의 재전달은 `id` PK로 차단한다.
- 분석 서비스가 같은 사건을 새로운 ID로 다시 발행한 경우에는 복합 UNIQUE로 차단한다.
- 현재 MVP에서는 `event_type`으로 `ROUTINE_MISSED`를 사용한다.

## 2. 사건 (`incident`)

- ID: `id` PK
- 위험 분석 이벤트 ID: `analysis_event_id` FK
- 가구 ID: `household_id`
- 사건 타입: `incident_type`
- 사건 상태: `status`
- 사건 발생 시각: `opened_at`
- 사건 종료 시각: `closed_at`, nullable
- 수정 시각: `updated_at`

제약 조건:

- `analysis_event_id` UNIQUE
- `status`는 다음 값만 허용
  - `OPEN`: 사용자 응답 대기
  - `CONFIRMED`: 사용자가 위험을 선택했거나 30초 동안 응답하지 않음
  - `FALSE_POSITIVE`: 사용자가 위험하지 않음을 선택함

상태 전이:

```text
OPEN
 ├─ 사용자 yes 또는 30초 무응답 → CONFIRMED
 └─ 사용자 no                  → FALSE_POSITIVE
```

## 3. 사건 액션 (`incident_action`)

- ID: `id` PK
- 사건 ID: `incident_id` FK
- 알림 전달 ID: `notification_delivery_id` FK, nullable
- 액션 타입: `action_type`
- 수행자 타입: `actor_type`
- 수행자 ID: `actor_id`, nullable
- 이전 사건 상태: `previous_status`, nullable
- 변경된 사건 상태: `next_status`
- 발생 시각: `occurred_at`

액션 타입:

- `DETECTED`: 위험 이벤트를 수신하고 사건 생성
- `NOTIFIED`: Web Push 발송 성공
- `RESPONDED_YES`: 사용자가 위험 선택
- `RESPONDED_NO`: 사용자가 위험하지 않음 선택
- `AUTO_CONFIRMED`: 30초 무응답으로 위험 자동 확정

수행자 타입:

- `SYSTEM`
- `USER`

## 4. 가구 접근 권한 (`household_access`)

- ID: `id` PK
- 가구 ID: `household_id`
- 사용자 ID: `user_id`
- 생성 시각: `created_at`

제약 조건:

- `household_id + user_id` UNIQUE

사용 목적:

- 위험 사건의 알림 수신자 결정
- 사건 목록 및 상세 조회 권한 확인
- SSE 이벤트 전달 대상 확인

## 5. Web Push 구독 (`push_subscription`)

- ID: `id` PK
- 사용자 ID: `user_id`
- Push Endpoint: `endpoint`
- 브라우저 공개키: `p256dh`
- 브라우저 인증값: `auth`
- 구독 만료 시각: `expires_at`, nullable
- 구독 해지 시각: `revoked_at`, nullable
- 생성 시각: `created_at`
- 수정 시각: `updated_at`

제약 조건:

- `endpoint` UNIQUE

처리 원칙:

- 같은 Endpoint가 재등록되면 INSERT하지 않고 기존 행을 갱신한다.
- Push 서버가 404 또는 410을 반환하면 행을 삭제하지 않고 `revoked_at`을 기록한다.
- 실제 발송에서는 `revoked_at IS NULL`이고 만료되지 않은 구독만 사용한다.

## 6. 알림 전달 (`notification_delivery`)

- ID: `id` PK
- 사건 ID: `incident_id` FK
- 수신 사용자 ID: `recipient_user_id`
- 전달 상태: `delivery_status`
- 알림 내용: `payload`
- 발송 시각: `sent_at`, nullable
- 응답 기한: `response_deadline_at`, nullable
- 응답: `answer`, nullable
- 응답 출처: `response_source`, nullable
- 응답 시각: `responded_at`, nullable
- 실패 사유: `failure_reason`, nullable
- 생성 시각: `created_at`

제약 조건:

- `incident_id + recipient_user_id` UNIQUE
- `delivery_status`는 다음 값만 허용
  - `PENDING`
  - `SENT`
  - `FAILED`
- `answer`는 `yes`, `no` 또는 null
- `response_source`는 `user`, `timeout` 또는 null
- 응답이 없는 경우 `answer`, `response_source`, `responded_at`은 모두 null
- 응답이 있는 경우 `answer`, `response_source`, `responded_at`은 모두 값이 있어야 함

발송 및 타임아웃 원칙:

- `PENDING` 행을 먼저 저장한 후 별도 Worker가 Web Push를 발송한다.
- Worker는 수신 사용자의 활성 `push_subscription`을 모두 조회한다.
- 각 기기에 같은 알림 ID를 전달하여 어느 기기에서든 동일 알림에 응답하게 한다.
- 하나 이상의 Push 서버가 요청을 수락하면 `SENT`로 변경한다.
- `SENT` 변경 시 `sent_at`과 `response_deadline_at = sent_at + 30초`를 기록한다.
- 모든 발송이 실패하면 `FAILED`로 변경하고 자동 응답 대상으로 처리하지 않는다.
- 서버 재시작 후에도 남아 있는 `PENDING` 행을 Worker가 다시 처리한다.

## 7. UI 아웃박스 (`ui_outbox`)

- ID: `id` PK
- 가구 ID: `household_id`
- UI 이벤트 이름: `event_name`
- UI 전달 데이터: `payload`
- 생성 시각: `created_at`
- 발행 완료 시각: `published_at`, nullable

UI 이벤트 이름:

- `incident-opened`
- `incident-updated`

처리 원칙:

- 사건 생성 또는 상태 변경과 같은 DB 트랜잭션 안에서 아웃박스 행을 추가한다.
- 디스패처가 미발행 행을 SSE로 전송한 후 `published_at`을 기록한다.
- `id`는 SSE 이벤트 ID로 사용한다.
- 브라우저 재연결 시 `Last-Event-ID`보다 큰 행을 다시 전달한다.

## 응답과 사건 상태 매핑

| 입력 또는 상황 | 알림 응답 | 응답 출처 | 사건 상태 | 사건 액션 |
| --- | --- | --- | --- | --- |
| 사용자가 위험 선택 | `yes` | `user` | `CONFIRMED` | `RESPONDED_YES` |
| 사용자가 위험하지 않음 선택 | `no` | `user` | `FALSE_POSITIVE` | `RESPONDED_NO` |
| 30초 동안 응답 없음 | `yes` | `timeout` | `CONFIRMED` | `AUTO_CONFIRMED` |

## 트랜잭션 단위

위험 이벤트 수신:

```text
analysis_event INSERT
→ incident INSERT
→ incident_action DETECTED INSERT
→ notification_delivery PENDING INSERT
→ ui_outbox incident-opened INSERT
→ COMMIT
```

사용자 응답 또는 자동 응답:

```text
notification_delivery 응답 여부 확인 및 잠금
→ notification_delivery 응답 UPDATE
→ incident 상태 UPDATE
→ incident_action INSERT
→ ui_outbox incident-updated INSERT
→ COMMIT
```

동일한 응답이 재전송되면 기존 결과를 반환하고, 이미 저장된 응답과 반대되는 요청이 들어오면 충돌로 처리한다.
