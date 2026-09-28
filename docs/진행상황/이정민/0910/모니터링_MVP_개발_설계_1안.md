# 모니터링 MVP 개발 설계 1안 — analysis.event.v1 소비 · 사건 · Web Push 알림

> 대상 저장소: `C:\dev\특화 프로젝트 - 분산`
> 작성일: 2026-09-10
> 관련 문서: [모니터링 MVP 테이블 목록](./모니터링_MVP_테이블_목록.md), [NILM 3안 백엔드 WBS](../0905/NILM_3안_백엔드_WBS_1주.md) §M-1~M-6, [Realtime Analysis Service README](../../../../ai/realtime-analysis-service/README.md), 모니터링 서비스 개발 가이드(`C:\dev\프로젝트분석\개발\모니터링_서비스_개발_가이드.md`) §15~§18, §27

---

## 0. 목표와 범위

`realtime-analysis-service`가 발행하는 `analysis.event.v1`을 monitoring-service가 소비해 사건(incident)을 만들고, 보호자에게 **Web Push 알림 1종(ROUTINE_MISSED)** 을 보내고, 보호자의 `위험` / `위험하지 않음` 응답 또는 30초 무응답을 사건 상태에 반영한다. 화면 갱신은 `ui_outbox` → SSE 경로를 쓴다.

이번 범위에 **포함**:

- Kafka Consumer(수동 커밋, DLQ), `analysis_event` 저장, incident 생성, 멱등 처리
- Web Push 구독 저장 API, 알림 발송, 응답 API, 30초 만료 자동 처리
- `ui_outbox` 기록과 폴링 디스패처, SSE 스트림 최소 구현
- Flyway `V2__monitoring.sql`, 로컬·EC2-A Compose 변경, 테스트 분리

이번 범위에서 **제외**:

- Snapshot(`analysis.snapshot.v1`) 소비와 Redis 최신 상태 — 분석 서비스가 아직 발행하지 않음
- `incident.lifecycle.v1` 발행, Notification Service 분리, 다중 인스턴스 Redis Pub/Sub fan-out
- ACKNOWLEDGED / RESOLVED 등 담당자 대응 단계 — MVP 알림 시나리오에 없음
- `SENSOR_GAP` 운영 이벤트

## 1. 전제와 확인한 사실

### 1-1. 현재 코드 상태

| 구성 | 상태 |
| --- | --- |
| monitoring-service | Ping 컨트롤러, JPA·Redis·OAuth2 의존성만 있음. Kafka 의존성 없음. Flyway `V1__init.sql`은 `SELECT 1` |
| realtime-analysis-service | `analysis.event.v1`에 가구 단위 `ROUTINE_MISSED` 후보 발행. 해소 이벤트(`ANOMALY_CLEARED`) 없음 |
| Kafka | `power.raw.v1`, `analysis.event.v1`, `dlq.analysis` 각 24파티션. `analysis.event.v1` 소비자 없음 |
| 테스트 | H2(PostgreSQL 모드) + 이미지 빌드 중 `./gradlew test bootJar` 실행 |

### 1-2. 실제 이벤트 계약

프로듀서 코드(`anomaly_detector.py` → `event_producer.py`)를 그대로 실행해 확인한 직렬화 결과다. `AnalysisEvent` 스키마가 `extra="forbid"`이므로 이 5필드 외에는 나오지 않는다.

```json
{"event_id": "f942ef73-eced-4cf8-80de-e6f0748f06b4", "household_id": "house-001", "score": 86, "occurred_at": "2026-09-02T09:00:00Z", "reason": {"expected_until": "08:10", "normal_days": 12, "window_days": 14}}
```

- Kafka Key: `household_id` UTF-8 바이트. 타입 헤더 등 추가 헤더 없음
- `occurred_at`: 항상 UTC `Z`. 마이크로초가 있으면 `...:00.120000Z` 6자리 소수. 파서는 둘 다 받아야 함
- `score`: `normal_days / window_days × 100 × reliability_weight` 반올림. 임계치(기본 80) 이상만 발행
- baseline은 `config/baselines.json`에 `H001` 하나. 시뮬레이터 `household_id`가 이와 일치해야 이벤트가 나옴

### 1-3. 개발 가이드 · WBS와의 차이

가이드 §5.2와 WBS §C1은 이벤트에 `event_type`, `severity`, `correlation_key`, `title`, `model_version`이 있다고 가정하지만 현재 계약에는 없다. 이 설계는 저장에 필요한 최소 필드를 **컨슈머가 도출**한다. `correlation_key` 문자열은 저장하지 않는다.

| 항목 | 도출 규칙 | 계약 확장 시 |
| --- | --- | --- |
| `event_type` | 없으면 `ROUTINE_MISSED` 고정 | 프로듀서 값 우선 |
| `event_date` | `occurred_at`을 `Asia/Seoul` 날짜로 변환 | 프로듀서가 명시하면 계약 검토 후 우선 |
| `expected_until` | `reason.expected_until`을 `TIME`으로 변환 | 프로듀서가 명시하면 계약 검토 후 우선 |
| `severity` | 저장하지 않음. MVP 단일 종류 | 컬럼 추가 |
| 자동 해소 | 없음. 응답 또는 만료로만 닫힘 | `ANOMALY_CLEARED` 수신 시 RESOLVED 전이 추가 |

DTO는 `@JsonIgnoreProperties(ignoreUnknown = true)`로 두어 필드가 추가되어도 기존 컨슈머가 깨지지 않게 한다.

## 2. 전체 흐름

```text
analysis.event.v1 (key=household_id, 24 파티션)
  │
  ▼  [Kafka Consumer, 수동 ack]
  ErrorHandlingDeserializer(JsonDeserializer)     JSON 깨짐 → dlq.monitoring, ack
  Bean Validation                                 검증 실패 → dlq.monitoring, ack
  │
  ▼  [트랜잭션 1: AnalysisEventIngestService]
  analysis_event INSERT (Kafka event_id를 id PK로 저장 → 물리 중복 차단)
  analysis_event INSERT (household_id + event_type + event_date + expected_until UNIQUE → 논리 중복 차단)
  incident OPEN INSERT
  incident_action DETECTED
  household_access 기준 수신자별 notification_delivery PENDING INSERT
  ui_outbox incident-opened
  COMMIT → ack
  │
  ▼  [NotificationDeliveryWorker, 커밋 후 별도 실행]
  PENDING notification_delivery 조회
  recipient_user_id의 활성 push_subscription 전체 조회
  모든 기기에 같은 notification_delivery.id로 Web Push 발송
  1건 이상 수락 → SENT + sent_at + response_deadline_at
  전부 실패 → FAILED, 404/410 구독은 revoked_at 기록
  incident_action NOTIFIED
  │
  ├─ 보호자 응답 PUT /api/notifications/{id}/responses ──┐
  └─ 30초 만료 @Scheduled(5s) ─────────────────────────┤
                                                        ▼  [NotificationResponseService 트랜잭션]
                          notification_delivery 잠금 후 answer IS NULL 확인
                          incident OPEN → CONFIRMED(yes/timeout) / FALSE_POSITIVE(no)
                          incident_action RESPONDED_YES / RESPONDED_NO / AUTO_CONFIRMED
                          ui_outbox incident-updated
  │
  ▼  [UiOutboxDispatcher @Scheduled(500ms)]
  published_at IS NULL 행 → SseHub → published_at 갱신
  SSE id = ui_outbox.id, 재연결 시 Last-Event-ID 이후 재전송
```

외부 Push 서버 호출은 Kafka 컨슈머 트랜잭션 밖에서 수행한다. 위험 이벤트 수신 트랜잭션은 사건과 수신자별 `notification_delivery PENDING`까지만 확정하고, 별도 Worker가 `PENDING`을 반복 조회해 발송한다. 따라서 PENDING 저장 직후 서버가 종료돼도 재시작 후 다시 처리할 수 있다.

## 3. 패키지 구조

```text
com.nilm.monitoring
├─ config/
│  ├─ KafkaConsumerConfig.java         ErrorHandler, DeadLetterPublishingRecoverer, 컨테이너 팩토리
│  ├─ WebPushConfig.java               VAPID 키, PushService Bean
│  └─ SchedulingConfig.java
├─ event/                              Kafka 인바운드
│  ├─ AnalysisEventMessage.java        record DTO, Bean Validation
│  └─ AnalysisEventListener.java       @KafkaListener → IngestService → ack
├─ incident/
│  ├─ AnalysisEvent.java, Incident.java, IncidentAction.java   엔티티
│  ├─ IncidentStatus.java, ActionType.java, ActorType.java     enum
│  ├─ *Repository.java
│  ├─ AnalysisEventNormalizer.java     event_date·expected_until 도출
│  ├─ AnalysisEventIngestService.java  @Transactional 트랜잭션 1
│  └─ IncidentController.java          조회 API
├─ notification/
│  ├─ PushSubscription.java, NotificationDelivery.java, HouseholdAccess.java
│  ├─ *Repository.java
│  ├─ PushSubscriptionController.java  POST /api/monitoring/push-subscriptions
│  ├─ NotificationDeliveryWorker.java  @Scheduled PENDING 발송·복구
│  ├─ NotificationResponseService.java @Transactional 응답·사건 전이
│  ├─ NotificationExpiryScheduler.java @Scheduled 30초 만료
│  ├─ NotificationController.java      PUT /api/monitoring/notifications/{id}/responses
│  └─ WebPushClient.java               외부 호출 격리 (인터페이스 + 구현)
└─ outbox/
   ├─ UiOutbox.java, UiOutboxRepository.java
   ├─ OutboxWriter.java                트랜잭션 안에서 행 기록
   ├─ UiOutboxDispatcher.java          @Scheduled 폴링
   ├─ SseHub.java                      ConcurrentHashMap<userId, List<SseEmitter>>
   └─ StreamController.java            GET /api/monitoring/stream
```

`@Transactional` 서비스는 Listener·Controller와 다른 Bean에 둔다. 같은 클래스 내부 호출은 프록시를 거치지 않아 트랜잭션이 적용되지 않는다(가이드 §16).

## 4. 테이블 설계 — `V2__monitoring.sql`

가이드 §15의 5개 테이블에서 MVP 알림 흐름에 쓰이지 않는 컬럼을 제거하고, Web Push 구독과 가구-보호자 매핑 2개를 추가했다.

```sql
-- 1. 분석 서비스가 발견한 사실 (불변)
CREATE TABLE analysis_event (
    id               UUID         PRIMARY KEY,
    household_id     VARCHAR(50)  NOT NULL,
    event_type       VARCHAR(40)  NOT NULL DEFAULT 'ROUTINE_MISSED',
    score            SMALLINT     NOT NULL CHECK (score BETWEEN 0 AND 100),
    event_date       DATE         NOT NULL,
    expected_until   TIME         NOT NULL,
    occurred_at      TIMESTAMPTZ  NOT NULL,
    reason           JSONB        NOT NULL,
    received_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),

    UNIQUE (household_id, event_type, event_date, expected_until)
);
CREATE INDEX ix_analysis_event_household ON analysis_event (household_id, occurred_at DESC);

-- 2. 사건의 현재 상태
CREATE TABLE incident (
    id                UUID         PRIMARY KEY,
    analysis_event_id UUID         NOT NULL UNIQUE REFERENCES analysis_event (id),
    household_id     VARCHAR(50)  NOT NULL,
    incident_type    VARCHAR(40)  NOT NULL,
    status           VARCHAR(20)  NOT NULL
        CHECK (status IN ('OPEN', 'CONFIRMED', 'FALSE_POSITIVE')),
    opened_at        TIMESTAMPTZ  NOT NULL,
    closed_at        TIMESTAMPTZ,
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX ix_incident_household_status ON incident (household_id, status, opened_at DESC);

-- 3. 사건에 대한 액션 이력 (추가만 함)
CREATE TABLE incident_action (
    id                        BIGSERIAL    PRIMARY KEY,
    incident_id               UUID         NOT NULL REFERENCES incident (id),
    notification_delivery_id  UUID,
    action_type               VARCHAR(30)  NOT NULL, -- DETECTED, NOTIFIED, RESPONDED_YES, RESPONDED_NO, AUTO_CONFIRMED
    actor_type                VARCHAR(10)  NOT NULL CHECK (actor_type IN ('SYSTEM', 'USER')),
    actor_id                  VARCHAR(100),            -- JWT sub, SYSTEM이면 NULL
    previous_status           VARCHAR(20),
    next_status               VARCHAR(20)  NOT NULL,
    occurred_at               TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX ix_incident_action_history ON incident_action (incident_id, id);

-- 4. 가구 ↔ 보호자 매핑 (알림 수신자 결정)
CREATE TABLE household_access (
    id               BIGSERIAL    PRIMARY KEY,
    household_id     VARCHAR(50)  NOT NULL,
    user_id          VARCHAR(100) NOT NULL,   -- JWT sub
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),

    UNIQUE (household_id, user_id)
);
CREATE INDEX ix_household_access_user ON household_access (user_id);

-- 5. Web Push 구독
CREATE TABLE push_subscription (
    id               BIGSERIAL    PRIMARY KEY,
    user_id          VARCHAR(100) NOT NULL,
    endpoint         TEXT         NOT NULL UNIQUE,
    p256dh           TEXT         NOT NULL,
    auth             TEXT         NOT NULL,
    expires_at       TIMESTAMPTZ,
    revoked_at       TIMESTAMPTZ,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX ix_push_subscription_user_active
    ON push_subscription (user_id)
    WHERE revoked_at IS NULL;

-- 6. 사건·수신 사용자별 논리 알림 전달 + 응답
CREATE TABLE notification_delivery (
    id                    UUID         PRIMARY KEY,
    incident_id           UUID         NOT NULL REFERENCES incident (id),
    recipient_user_id     VARCHAR(100) NOT NULL,
    delivery_status       VARCHAR(10)  NOT NULL DEFAULT 'PENDING'
        CHECK (delivery_status IN ('PENDING', 'SENT', 'FAILED')),
    payload               JSONB        NOT NULL,
    sent_at               TIMESTAMPTZ,
    response_deadline_at  TIMESTAMPTZ,
    answer                VARCHAR(10)  CHECK (answer IN ('yes', 'no')),
    response_source       VARCHAR(10)  CHECK (response_source IN ('user', 'timeout')),
    responded_at          TIMESTAMPTZ,
    failure_reason        TEXT,
    created_at            TIMESTAMPTZ  NOT NULL DEFAULT now(),

    UNIQUE (incident_id, recipient_user_id),
    CHECK (
        (answer IS NULL AND response_source IS NULL AND responded_at IS NULL)
        OR
        (answer IS NOT NULL AND response_source IS NOT NULL AND responded_at IS NOT NULL)
    )
);
CREATE INDEX ix_notification_delivery_pending
    ON notification_delivery (id)
    WHERE delivery_status = 'PENDING';
CREATE INDEX ix_notification_delivery_timeout
    ON notification_delivery (response_deadline_at)
    WHERE delivery_status = 'SENT' AND answer IS NULL;
CREATE INDEX ix_notification_delivery_incident
    ON notification_delivery (incident_id);

ALTER TABLE incident_action
    ADD CONSTRAINT fk_incident_action_notification_delivery
    FOREIGN KEY (notification_delivery_id) REFERENCES notification_delivery (id);

-- 7. 커밋 후 SSE로 보낼 화면 이벤트
CREATE TABLE ui_outbox (
    id               BIGSERIAL    PRIMARY KEY,
    household_id     VARCHAR(50)  NOT NULL,
    event_name       VARCHAR(40)  NOT NULL,   -- incident-opened, incident-updated
    payload          JSONB        NOT NULL,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at     TIMESTAMPTZ
);
CREATE INDEX ix_ui_outbox_unpublished ON ui_outbox (id) WHERE published_at IS NULL;
CREATE INDEX ix_ui_outbox_replay      ON ui_outbox (household_id, id);
```

### 4-1. 가이드 대비 제거한 컬럼과 이유

| 테이블 | 제거 | 이유 |
| --- | --- | --- |
| analysis_event | `severity`, `title`, `model_version`, `rule_version` | 현재 프로듀서가 보내지 않음 |
| analysis_event | `kafka_topic/partition/offset` + UNIQUE | Kafka `event_id`를 저장한 `id` PK가 같은 역할. 디버깅용이면 nullable 컬럼으로 추후 추가 |
| analysis_event·incident | `correlation_key` | `household_id + event_type + event_date + expected_until` 복합 UNIQUE로 논리 중복 차단 |
| incident | `severity`, `assigned_to`, `acknowledged_at`, `resolved_at`, `version`, `created_at` | MVP에 확인·배정 단계가 없고, 응답 동시성은 `notification_delivery` 행 잠금으로 제어 |
| incident_action | `action_event_id`, `note` | 응답 멱등성은 `notification_delivery.answer`가 담당 |
| notification_delivery | `subscription_id`, `channel`, `provider_message_id` | 사건·수신 사용자별 논리 알림으로 관리하며 Web Push 단일 채널만 지원. 기기별 시도 이력은 MVP에서 제외 |
| — | 응답 테이블 신설 안 함 | API가 알림 ID 하나로 응답을 받으므로 `notification_delivery`에 응답 3컬럼 병합 |

### 4-2. 각 테이블을 DB에 두는 이유

- **ui_outbox**: DB 커밋과 SSE 전송은 하나의 트랜잭션이 될 수 없다. 사건과 같은 트랜잭션에 outbox 행을 넣으면 커밋된 화면 이벤트를 재시작 후에도 미발행 행부터 이어서 보낼 수 있다. `id`가 SSE 이벤트 ID가 되어 재연결 시 `Last-Event-ID` 이후 재전송이 가능하다. 컨슈머가 느린 브라우저에 끌려가지 않도록 소비와 전송을 분리한다.
- **notification_delivery**: 응답 API가 서버 발급 ID를 요구한다. 앱이 닫힌 상태가 핵심 시나리오이므로 30초 만료는 서버가 `response_deadline_at`으로 판정해야 하고, 메모리 타이머는 재시작 시 사라진다. 사용자의 `no`와 타이머의 `yes`가 동시에 오면 행 잠금 후 아직 `answer IS NULL`인 경우만 최초 응답을 반영한다. 발송에 모두 실패한 행은 `FAILED`로 두고 자동 응답 대상에서 제외한다.

## 5. Kafka Consumer 설계

### 5-1. 설정

```properties
spring.kafka.bootstrap-servers=${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
spring.kafka.consumer.group-id=monitoring-service-analysis-event-v1
spring.kafka.consumer.auto-offset-reset=${KAFKA_AUTO_OFFSET_RESET:earliest}
spring.kafka.consumer.enable-auto-commit=false
spring.kafka.consumer.isolation-level=read_committed
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
spring.kafka.consumer.properties.spring.deserializer.value.delegate.class=org.springframework.kafka.support.serializer.JsonDeserializer
spring.kafka.consumer.properties.spring.json.value.default.type=com.nilm.monitoring.event.AnalysisEventMessage
spring.kafka.consumer.properties.spring.json.use.type.headers=false
spring.kafka.listener.ack-mode=manual_immediate
spring.kafka.listener.concurrency=3

app.kafka.analysis-event-topic=${KAFKA_ANALYSIS_EVENT_TOPIC:analysis.event.v1}
app.kafka.dlq-topic=${KAFKA_MONITORING_DLQ_TOPIC:dlq.monitoring}
app.kafka.consumer.enabled=${KAFKA_CONSUMER_ENABLED:true}
app.incident.timezone=Asia/Seoul
app.incident.max-event-age=PT24H
app.notification.response-timeout=PT30S
```

- `use.type.headers=false` 필수. Python 프로듀서는 타입 헤더를 붙이지 않는다.
- `app.kafka.consumer.enabled`는 `@KafkaListener(autoStartup=...)`에 연결. IDE 실행이나 Kafka 없는 테스트에서 리스너를 끈다.
- `auto-offset-reset=earliest`로 컨슈머 배포 전 이벤트도 저장한다. 단 `occurred_at`이 `max-event-age`보다 오래되면 `analysis_event`만 저장하고 incident는 만들지 않는다.
- Key가 `household_id`라 같은 가구는 같은 파티션에서 순서대로 처리된다. 24파티션이므로 인스턴스·스레드 합계 24까지 확장 가능.

### 5-2. 멱등 처리

| 중복 유형 | 원인 | 차단 지점 |
| --- | --- | --- |
| 물리적 중복 | 커밋 전 재시작, 리밸런스 | `analysis_event.id` PK. 존재하면 아무것도 하지 않고 ack |
| 논리적 중복 | 분석 서비스 재시작으로 같은 날 같은 baseline 이벤트가 새 `id`로 재발행 | `analysis_event(household_id, event_type, event_date, expected_until)` 복합 UNIQUE. 사건 상태와 관계없이 같은 기준 이벤트는 한 번만 저장 |

복합 UNIQUE 위반은 재시도하지 않고 이미 처리한 논리 이벤트로 간주해 정상 ack한다. DB 일시 장애만 재시도 대상으로 분류한다.

### 5-3. 오류 처리

- `DefaultErrorHandler` + `DeadLetterPublishingRecoverer`, `FixedBackOff(1000ms, 3회)`
- 재시도 없이 DLQ: 역직렬화 실패, Bean Validation 실패, `occurred_at` 파싱 실패
- 재시도 후 DLQ: `TransientDataAccessException` 등 일시 장애
- DLQ 토픽은 `dlq.monitoring` 신설. `dlq.analysis`는 분석 서비스의 `DlqMessage` 스키마가 정해져 있어 섞지 않는다. 원본 바이트와 예외는 Spring Kafka가 `kafka_dlt-*` 헤더로 붙인다.
- DLQ 발행 자체가 실패하면 ack하지 않아 재전달된다. 이전 리뷰에서 지적한 분석 서비스식 무한 재시작을 여기서는 피한다.

## 6. 사건 상태 전이

```text
OPEN ──(응답 yes 또는 30초 만료)──▶ CONFIRMED
  └──(응답 no)──────────────────▶ FALSE_POSITIVE
```

| 전이 | actor_type | action_type | 트리거 |
| --- | --- | --- | --- |
| (없음) → OPEN | SYSTEM | DETECTED | Kafka 이벤트 |
| OPEN → OPEN | SYSTEM | NOTIFIED | Push 발송 성공 |
| OPEN → CONFIRMED | USER | RESPONDED_YES | `answer=yes, source=user` |
| OPEN → FALSE_POSITIVE | USER | RESPONDED_NO | `answer=no, source=user` |
| OPEN → CONFIRMED | SYSTEM | AUTO_CONFIRMED | 서버 만료 스케줄러 |

가이드의 ACKNOWLEDGED·RESOLVED는 상태 CHECK에 추가만 하면 되도록 컬럼 구조를 유지했다.

## 7. Web Push 알림 설계

### 7-1. 구독 저장 — `POST /api/monitoring/push-subscriptions`

요청 본문은 첨부 스펙 그대로 `endpoint`, `expirationTime`, `keys.p256dh`, `keys.auth`. `user_id`는 JWT `sub`에서 취한다. `endpoint` UNIQUE 기준으로 UPSERT하여 같은 기기 재등록 시 행이 늘지 않게 한다. 응답은 201과 `subscriptionId`.

경로는 Gateway 라우팅 규칙(`/api/monitoring/**`)에 맞춰 `/api/monitoring/` 접두어를 붙인다. 첨부 스펙의 `/api/push-subscriptions`, `/api/notifications/...`는 Gateway route를 추가하지 않으면 도달하지 않는다.

### 7-2. 발송 — `NotificationDeliveryWorker`

`@Scheduled(fixedDelay = 2s)`로 `delivery_status='PENDING'`인 `notification_delivery`를 조회한다. 위험 이벤트 수신 트랜잭션이 사건과 수신 사용자별 PENDING 행을 함께 만들기 때문에 Worker는 서버 재시작 후에도 남은 행부터 재처리할 수 있다.

1. `recipient_user_id`로 `revoked_at IS NULL`이고 만료되지 않은 `push_subscription` 전체 조회
2. 모든 기기에 같은 `notification_delivery.id`를 넣어 Web Push 발송(VAPID). 페이로드: `notificationId`, `incidentId`, `householdId`, `title`
3. 404/410을 반환한 구독은 삭제하지 않고 `revoked_at=now()`로 비활성화
4. 한 기기 이상 Push 서버가 수락하면 `SENT`, `sent_at=now()`, `response_deadline_at=sent_at+30초`로 변경
5. 전부 실패하거나 활성 구독이 없으면 `FAILED`와 `failure_reason` 기록. 이 경우 30초 자동 확정 대상에서 제외
6. 성공 시 해당 알림 ID를 참조하는 `incident_action NOTIFIED` 추가

기기별 발송 결과 테이블은 만들지 않는다. `SENT`는 브라우저 표시 완료가 아니라 하나 이상의 Push 서버가 요청을 수락했다는 뜻이다. 실제 기기별 재시도·감사가 필요해지면 `notification_delivery_attempt`를 별도로 추가한다.

VAPID 키는 `WEB_PUSH_VAPID_PUBLIC_KEY`, `WEB_PUSH_VAPID_PRIVATE_KEY`, `WEB_PUSH_SUBJECT` 환경변수로 주입한다. 라이브러리는 `nl.martijndwars:web-push` 계열을 사용한다.

### 7-3. 응답 — `PUT /api/monitoring/notifications/{notificationId}/responses`

```sql
SELECT *
  FROM notification_delivery
 WHERE id = :id
 FOR UPDATE;

UPDATE notification_delivery
   SET answer = :answer,
       response_source = :source,
       responded_at = now()
 WHERE id = :id
   AND delivery_status = 'SENT'
   AND answer IS NULL;
```

- 갱신 1행: 같은 트랜잭션에서 incident 전이, `incident_action`, `ui_outbox incident-updated`
- 갱신 0행이고 같은 답변이 저장됨: 현재 저장값을 200으로 반환해 API를 멱등하게 한다
- 갱신 0행이고 반대 답변이 저장됨: `409 Conflict`
- 존재하지 않는 ID: 404. 다른 사용자의 알림: 403 (`recipient_user_id`와 JWT `sub` 비교)
- `PENDING` 또는 `FAILED` 알림 응답: 아직 발송되지 않았거나 전달 실패이므로 `409 Conflict`
- 응답 본문은 스펙대로 `id`, `status`(YES/NO 소문자), `respondedAt`, `responseSource`

`responded_at`은 요청값이 아니라 서버 `now()`를 기준으로 기록한다. 사용자 API는 `source=user`, 서버 만료 스케줄러는 `source=timeout`으로 처리한다. 두 경로는 동일한 `NotificationResponseService`를 호출하되, 서버 스케줄러가 공개 API를 HTTP로 호출하지는 않는다.

### 7-4. 30초 만료 — `NotificationExpiryScheduler`

`@Scheduled(fixedDelay = 5s)`로 `delivery_status='SENT' AND answer IS NULL AND response_deadline_at < now()`인 행을 찾아 7-3과 같은 서비스 경로로 `yes / timeout`을 기록한다. 앱이 닫혀 있으면 클라이언트 타이머가 없으므로 서버 측 처리가 기본 경로이고, 클라이언트 타이머는 보조 수단이다. `PENDING`과 `FAILED`는 만료 대상이 아니다.

### 7-5. 보호자가 여럿일 때

`notification_delivery`는 기기별이 아니라 **사건과 수신 사용자별**로 한 행만 만든다. 같은 사용자의 여러 기기에 동일한 알림 ID를 보내므로 어느 기기에서든 응답하면 그 사용자의 논리 알림이 완료된다. 보호자가 여러 명이면 각 보호자별 행이 생성되며, MVP 정책은 먼저 확정된 응답이 사건 상태를 결정한다. 사건이 이미 `OPEN`이 아니면 다른 보호자의 후속 응답은 알림 행에는 기록하되 사건 상태를 다시 변경하지 않는다.

## 8. ui_outbox 디스패처와 SSE

- `UiOutboxDispatcher`: `@Scheduled(fixedDelay = 500ms)`, `published_at IS NULL ORDER BY id LIMIT 100` → `SseHub.send(householdId, id, eventName, payload)` → `published_at = now()`
- `SseHub`: `ConcurrentHashMap<userId, List<SseEmitter>>`. 전송 대상은 `household_access`로 해당 가구 사용자 목록을 조회해 결정
- `GET /api/monitoring/stream`: `SseEmitter(timeout 0)`, 25초 heartbeat comment, `Last-Event-ID` 헤더가 있으면 `ui_outbox WHERE id > :last AND household_id IN (사용자 가구)` 재전송 후 실시간 구독
- 전송 중복은 허용한다(at-least-once). 프론트는 `id`가 같은 이벤트를 재적용하지 않는다
- Gateway는 `/api/monitoring/stream` 전용 route에 response timeout 해제가 필요하다(WBS §G-1). Nginx는 이미 `proxy_buffering off`, `read_timeout 3600s`

다중 인스턴스 fan-out(Redis Pub/Sub)은 이번 범위에서 제외한다. 단일 인스턴스에서는 디스패처가 자기 Hub로만 보내도 충분하다.

## 9. API 목록

| Method | Path | 권한 | 설명 |
| --- | --- | --- | --- |
| POST | `/api/monitoring/push-subscriptions` | 인증 | Web Push 구독 저장(UPSERT) |
| PUT | `/api/monitoring/notifications/{id}/responses` | 알림 소유자 | 응답 저장, 사건 전이 |
| GET | `/api/monitoring/stream` | 인증 | SSE, `Last-Event-ID` 재전송 |
| GET | `/api/monitoring/incidents?status=` | 인증 | 사용자 가구의 사건 목록 |
| GET | `/api/monitoring/incidents/{id}` | 인증 | 사건 상세 + 액션 이력 + 알림 목록 |

권한 필터는 모두 `household_access` 기준이다. ADMIN은 전체 가구를 본다.

## 10. 설정과 인프라 변경

| 파일 | 변경 |
| --- | --- |
| `backend/monitoring-service/build.gradle` | `spring-kafka`, `web-push`, `spring-kafka-test`, Testcontainers(postgresql, kafka, junit-jupiter) 추가. `integrationTest` 태스크 분리 |
| `infrastructure/local/compose.yaml` monitoring-service | `KAFKA_BOOTSTRAP_SERVERS: kafka:19092`, VAPID 환경변수, `depends_on: kafka-init: service_completed_successfully` |
| `infrastructure/ec2-a/compose.yaml` monitoring-service | `KAFKA_BOOTSTRAP_SERVERS: ${KAFKA_BOOTSTRAP_SERVERS}`, VAPID 환경변수. 값은 이미 분석 서비스용으로 `.env`에 존재 |
| `infrastructure/local/compose.yaml`, `ec2-b/compose.yaml` kafka-init | `dlq.monitoring` 토픽 생성 추가 |
| `infrastructure/*/.env.example` | `WEB_PUSH_VAPID_PUBLIC_KEY`, `WEB_PUSH_VAPID_PRIVATE_KEY`, `WEB_PUSH_SUBJECT` |
| `backend/api-gateway` | `/api/monitoring/stream` route timeout 해제 (WBS G-1) |

A→B 9092 보안 그룹은 분석 서비스 때문에 이미 열려 있어야 하므로 추가 작업이 없다.

## 11. 테스트 전략

**이미지 빌드 제약을 먼저 짚어야 한다.** 부분 인덱스와 `jsonb`는 H2가 온전히 지원하지 않아 통합 테스트는 Testcontainers PostgreSQL이 필요하다. 그러나 백엔드 Dockerfile은 이미지 빌드 중 `./gradlew test`를 실행하므로 Docker 데몬이 없어 Testcontainers가 돌 수 없다.

| 태스크 | 범위 | 실행 위치 |
| --- | --- | --- |
| `test` | 단위 테스트. `AnalysisEventNormalizer`, 만료 판정, 상태 전이, 응답 멱등 규칙(Mockito) | Dockerfile 빌드 단계(그대로) |
| `integrationTest` (JUnit 태그 `integration`) | Testcontainers PostgreSQL + Kafka | Jenkins CI `ci` 에이전트, `build.sh` 전 |

통합 시나리오:

1. 타입 헤더 없는 JSON 발행 → `analysis_event`, `incident`, `incident_action`, `notification_delivery`, `ui_outbox` 생성
2. 같은 `event_id` 2회, 다른 `event_id`지만 같은 `household_id + event_type + event_date + expected_until` 1회 → incident 1건
3. 깨진 JSON 발행 → `dlq.monitoring` 1건, 다음 정상 메시지 처리 계속
4. `notification_delivery SENT`에 `no` 응답 후 동일 `no` 재요청 → 저장 결과로 200
5. `no` 저장 후 `yes` 요청 → 409, 사건 상태 `FALSE_POSITIVE` 유지
6. `response_deadline_at` 경과 → 스케줄러가 `yes/timeout`, incident `CONFIRMED`
7. `FAILED` 알림은 기한이 지나도 자동 확정되지 않음
8. Web Push 클라이언트를 Fake로 바꿔 410 응답 → 구독의 `revoked_at` 기록
9. PENDING 저장 후 Worker 재시작 → 동일 알림 ID로 발송 재개

기존 `ValidationErrorResponseTest`는 H2 기반이라 V2에서 깨진다. Testcontainers로 옮기고 `integration` 태그를 붙인다.

## 12. 구현 순서

| 순서 | 작업 | 완료 기준 |
| --- | --- | --- |
| 1 | `build.gradle` 의존성, `integrationTest` 분리, Jenkinsfile CI 단계 추가 | `./gradlew test`는 Docker 없이 통과, `integrationTest`는 CI에서 통과 |
| 2 | `V2__monitoring.sql`, 엔티티 7개, Repository | Flyway 적용, Testcontainers 컨텍스트 로드 |
| 3 | `AnalysisEventNormalizer`, `AnalysisEventIngestService`, `OutboxWriter` | 단위 테스트 통과 |
| 4 | `KafkaConsumerConfig`, DTO, Listener, DLQ | 통합 시나리오 1~3 |
| 5 | 구독 API, `NotificationDeliveryWorker`, `WebPushClient` | 로컬 브라우저에서 Push 수신, 재시작 후 PENDING 복구 |
| 6 | 응답 API, `NotificationResponseService`, 만료 스케줄러 | 통합 시나리오 4~5 |
| 7 | `UiOutboxDispatcher`, `SseHub`, 스트림 API, Gateway route | 브라우저 2개에서 각자 가구 이벤트만 수신, 재연결 후 누락 없음 |
| 8 | 조회 API, Swagger | 프론트 연동 가능 |
| 9 | Compose·env·kafka-init 반영, 로컬 E2E | `FAKE_ON_APPLIANCES` 비움 → 08:10 이후 이벤트 → Push → 응답 → SSE 갱신 |

M-2(Consumer)는 분석 서비스 없이도 테스트 프로듀서로 진행할 수 있다. 1-2의 JSON을 `kafka-console-producer`로 `--property parse.key=true`와 함께 발행하면 된다.

## 13. 결정이 필요한 사항

| 항목 | 1안 선택 | 대안 | 합의 대상 |
| --- | --- | --- | --- |
| 이벤트 계약 확장 시점 | 컨슈머가 `event_type`, `event_date`, `expected_until` 도출 | 분석 서비스가 `event_type`, `appliance_type` 추가. 같은 시각의 baseline이 둘이면 현재 복합 키가 충돌하므로 계약 확장 필요 | AI 담당 |
| API 경로 접두어 | `/api/monitoring/...` | 스펙의 `/api/push-subscriptions` 유지 + Gateway route 추가 | 프론트·Gateway 담당 |
| 보호자 다수 | 첫 응답이 사건 결정 | 전원 응답 대기 | 기획 |
| 자동 해소 | 없음, 응답·만료로만 닫힘 | 날짜 변경 시 `EXPIRED` 자동 종결 | 기획 |
| 통합 테스트 분리 | Dockerfile은 단위만, CI에서 통합 | Dockerfile에서 Testcontainers 시도(불가) | 인프라 |

## 14. 이전 리뷰 지적과의 연결

- "analysis.event.v1 소비자가 없다" → 본 설계 §5로 해소
- "핸들러 예외가 프로세스를 죽인다"(분석 서비스) → 모니터링 측은 §5-3의 DLQ·재시도로 같은 문제를 피함. 분석 서비스 자체 수정은 별도 작업
- "테스트가 H2로 돌아간다" → §11에서 Testcontainers로 전환하되 Dockerfile 제약 때문에 태스크를 분리
