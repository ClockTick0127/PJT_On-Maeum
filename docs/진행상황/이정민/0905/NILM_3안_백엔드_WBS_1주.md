# NILM 3안 아키텍처 도입 — 백엔드 3인 1주 WBS 역할 분배안

> 대상 저장소: `C:\dev\특화 프로젝트 - 분산`
> 기준 아키텍처: [NILM_최종_아키텍처_3안.md](NILM_최종_아키텍처_3안.md) (축소 3안 포함)
> 기간: 5 영업일 (D1~D5) + 통합 리허설 반나절
> 작성일: 2026-09-07

---

## 0. 현재 코드 상태 (WBS 산정 근거)

| 영역 | 현재 상태 | 3안 목표와의 거리 |
| --- | --- | --- |
| api-gateway | 라우팅 2개(`/api/devices/**`, `/api/monitoring/**`), JWT 검증 on/off 스위치, CORS. 필터·타임아웃·SSE 설정 없음 | SSE 통과 설정, JWKS 캐싱 명시, 기기/사용자 토큰 경로 분리 필요 |
| iot-device-service | Ping 컨트롤러 + 예외 처리 + `V1__init.sql`은 `SELECT 1` (테이블 없음) | 도메인 전체 신규. Mosquitto는 passwd 파일 기반, ACL 없음 |
| monitoring-service | Ping 컨트롤러, Redis·JPA 의존성만 있음. 테이블 없음 | Kafka Consumer, incident 도메인, SSE Hub 전부 신규 |
| realtime-analysis-service | `ai/realtime-analysis-service/.gitkeep` 만 존재 | Flink Job 전부 신규 |
| 인프라 | EC2-A/B compose 분리 완료, Kafka `power.raw.v1` 24파티션, Bridge(Python) 동작, Nginx `/api/` proxy_buffering off 적용됨 | `analysis.*` 토픽 생성, Flink 컨테이너, Redis 연결 확인 |
| 이슈 (채지현 0904) | 토픽명 불일치(`power-raw` vs `power.raw.v1`), payload가 `power_w` 단독 | D1에 계약 확정으로 흡수 |

한 줄 결론: **Gateway는 보강, 나머지 세 서비스는 신규 구축**이다. 1주에 완성 가능한 범위는 "MVP 데모 경로 1개가 끝까지 통과"이며, 아래 WBS는 그 경로를 기준으로 자른다.

---

## 1. 역할 분배 원칙

1. **데이터 흐름 방향으로 사람을 나눈다.** 기기 → Kafka(입구), Kafka → 판정(중간), 판정 → 사용자(출구). 서로의 계약(토픽 스키마·API)만 맞추면 병렬 진행이 가능하다.
2. **Gateway와 인프라 공통 설정은 한 사람이 소유한다.** 셋이 나눠 만지면 compose·env 충돌이 가장 먼저 난다.
3. **D1 오전에 계약 3개를 먼저 고정한다.** 이게 늦어지면 나머지 4일이 전부 흔들린다.
4. **D5는 새 기능 금지.** 통합·측정·문서만.

---

## 2. 담당 배정

| 담당 | 영역 | 소유 서비스 | 인프라 소유 |
| --- | --- | --- | --- |
| **BE-1 (Edge·인증)** | 1. 기본 config + API Gateway, 2. IoT Device Service | `backend/api-gateway`, `backend/iot-device-service` | EC2-A compose, Keycloak realm, Mosquitto 설정, Nginx |
| **BE-2 (Stream)** | 3. Realtime Analysis Service (Flink) | `ai/realtime-analysis-service` (→ Flink Job 모듈) | EC2-B compose의 Flink·Kafka 토픽 초기화, `kafka-init` |
| **BE-3 (Serving)** | 4. Monitoring Service | `backend/monitoring-service` | Redis 설정, monitoring_db Flyway |

BE-1이 1번과 2번을 함께 맡는 이유: Gateway의 기기/사용자 토큰 분리와 Device Service의 기기 인증 생명주기는 **같은 설계 결정**(기기 토큰을 누가 발급하고 누가 검증하나)에서 나온다. 사람을 나누면 D2에 서로 기다리게 된다.

---

## 3. D1 오전 공동 작업 — 계약 고정 (3인 전원, 3시간)

| # | 계약 | 결정 내용 | 산출물 |
| --- | --- | --- | --- |
| C1 | **Kafka 토픽·스키마** | `power.raw.v1` 로 통일(`power-raw` 폐기). `analysis.snapshot.v1`(12), `analysis.event.v1`(12) 스키마 확정. `event_id`(UUID), `household_id`, `event_type`, `occurred_at`, `severity`, `score`, `reason`, `correlation_key` | `docs/contracts/kafka-topics.md` + `kafka-init`에 토픽 추가 |
| C2 | **토큰 모델** | 사용자 토큰 = Keycloak `nilm-dashboard`(realm role USER/GUARDIAN/ADMIN). 기기 토큰 = Device Service가 발급하는 별도 자격(아래 §4.1 참조). Gateway는 두 경로를 path로 구분 | `docs/contracts/auth-model.md` |
| C3 | **Monitoring API·SSE 이벤트** | `GET /api/monitoring/stream` SSE, 이벤트 이름 `snapshot`/`incident`, `Last-Event-ID` 재연결 규약. 조회 API 목록 | `docs/contracts/monitoring-api.md` |

이 3시간을 아끼면 D3에 반드시 되돌아온다. 결정만 하고 코드는 각자 오후부터 시작한다.

---

## 4. 담당별 WBS

### 4.1 BE-1 — 기본 config · API Gateway · IoT Device Service

**목표**: 대시보드 로그인 → Gateway → SSE가 끊기지 않고 통과. 기기가 발급받은 자격으로만 MQTT 발행 가능.

| 일 | 작업 | 상세 | 완료 기준 |
| --- | --- | --- | --- |
| D1 오후 | G-1 SSE 프록시 통과 | Gateway route에 `/api/monitoring/stream` 전용 route 추가: response timeout 해제(`spring.cloud.gateway.httpclient.response-timeout` route별 override), `Connection: keep-alive`. Nginx는 이미 `proxy_buffering off`·`read_timeout 3600s`이므로 **Gateway 쪽 timeout이 실제 병목**임을 확인하고 수정. `X-Accel-Buffering: no` 헤더 응답 필터 | curl로 SSE 5분 유지, 60초마다 heartbeat 수신 |
| D1 오후 | G-2 JWKS 캐싱 명시 | `NimbusReactiveJwtDecoder`를 Bean으로 직접 구성. JWK Set 캐시 TTL(기본 5분)·Keycloak 다운 시 캐시 유지 동작 확인. `jwk-set-uri`를 컨테이너 내부 주소(`http://keycloak:8080/...`)로, `issuer-uri`는 외부 주소로 분리(ec2-a compose 이미 이 형태) | Keycloak 컨테이너 중단 후 5분간 기존 토큰 검증 성공 로그 |
| D2 | G-3 기기 토큰 vs 사용자 토큰 분리 | Gateway에 경로 2종: `/api/**`(사용자 JWT, Keycloak issuer) / `/api/devices/provision/**`(기기 자격). 기기 자격은 **Keycloak client-credentials 대신 Device Service 자체 발급 opaque 토큰**으로 결정 권장(Keycloak 클라이언트를 가구 수만큼 만들지 않기 위함). Gateway는 `/provision` 경로를 인증 없이 Device Service로 통과시키고, Device Service가 자체 검증 | 사용자 토큰으로 `/provision` 호출 시 401, 기기 토큰으로 `/api/monitoring` 호출 시 401 |
| D2 | G-4 공통 config | `application.yml` 프로파일 분리(local/prod), Actuator 노출 범위, 모든 서비스 JVM `-XX:MaxRAMPercentage=65` + compose `mem_limit`(3안 §6 예산: Gateway 0.8G, Device 0.6G, Monitoring 0.8G) | `docker stats`에서 상한 확인 |
| D3 | I-1 Device 도메인 스키마 | `V2__device.sql`: `household`(id, name, region, status), `device`(id, household_id, serial, mqtt_username, credential_hash, status[PROVISIONED/ACTIVE/REVOKED], last_seen_at), `device_credential_history` | Flyway 적용, JPA 엔티티 3개 |
| D3 | I-2 기기 등록·자격 발급 API | `POST /api/devices` (ADMIN), `POST /api/devices/{id}/provision` → MQTT username/password 1회 노출 + 기기 토큰. 비밀번호는 argon2/bcrypt 해시만 저장 | Swagger에서 시나리오 통과 |
| D4 | I-3 Mosquitto 인증·ACL 연동 | 선택지 2개 중 **A안 권장**: `mosquitto-go-auth` 플러그인 + PostgreSQL backend(device_db의 `device` 테이블 직접 조회, ACL 쿼리로 `v1/power/{household}/+` 발행만 허용). B안: Device Service가 `passwd`·`acl` 파일 재생성 + `SIGHUP` (단순하지만 파일 동기화 문제). A안 실패 시 D4 오후 B안 폴백 | 등록 기기 계정으로 자기 토픽 발행 성공, 다른 가구 토픽 발행 시 거부, `kafka_bridge_user`는 `v1/power/#` 구독만 허용 |
| D4 | I-4 생명주기 | `PATCH /api/devices/{id}/revoke` → status REVOKED → 다음 연결부터 MQTT 거부. `last_seen_at` 갱신은 Bridge가 아니라 Monitoring의 `SENSOR_GAP` 판정에 맡기고, Device는 상태만 소유 | 폐기 후 재연결 거부 로그 |
| D5 | 통합 | Keycloak realm에 `GUARDIAN` role 추가(현재 USER/ADMIN만 존재), 테스트 사용자 3종 realm export. EC2-A compose에 Mosquitto ACL 설정 반영. 측정: JWT 검증 p95, SSE 연결 유지 시간 | §6 체크리스트 |

**BE-1 리스크**: `mosquitto-go-auth`는 공식 Mosquitto 이미지에 없어 별도 이미지가 필요하다. D4 오전 2시간 안에 컨테이너가 안 뜨면 B안(파일 재생성)으로 즉시 전환한다.

---

### 4.2 BE-2 — Realtime Analysis Service (Flink)

**목표**: `power.raw.v1` → Flink → `analysis.snapshot.v1`/`analysis.event.v1`. MVP 판정은 `ROUTINE_MISSED` 1종 + 해소 이벤트(패턴감지 문서 §9 MVP 범위).

| 일 | 작업 | 상세 | 완료 기준 |
| --- | --- | --- | --- |
| D1 오후 | F-1 프로젝트 골격 | `ai/realtime-analysis-service` → Flink 1.19 Java 프로젝트(Gradle). Kafka Source/Sink connector, JSON 직렬화. **PyFlink가 아닌 Java** 권장(EC2-B 메모리 예산 TM 2.5GB 안에서 Python 프로세스 추가 부담 회피) | 로컬 MiniCluster에서 `power.raw.v1` 읽고 그대로 출력 |
| D1 오후 | F-2 토픽 초기화 | `kafka-init`에 `analysis.snapshot.v1`(12), `analysis.event.v1`(12), `dlq.analysis`(3) 추가. 리플레이 시뮬레이터·HDFS 로더의 `power-raw` → `power.raw.v1` 변경 PR | `kafka-topics --list`에 4개 |
| D2 | F-3 전처리 스테이지 | `keyBy(household_id)` → 스키마 검증(누락 시 DLQ) → 중복 제거(`(household, ts)` 기준, 5분 상태 TTL — Bridge QoS1 재전달 대응) → 이벤트 시간 워터마크(허용 지연 10초) | 중복 메시지 100건 주입 시 다운스트림 1건 |
| D2 | F-4 Snapshot 생성 | 1초 텀블링 윈도우 → 가구별 `power_w` 평균·최대 → `analysis.snapshot.v1` 발행. 초당 110msg 처리 확인 | 리플레이 110msg/s 10분, Consumer Lag 0 유지 |
| D3 | F-5 변화점 탐지 + 가전 상태 판정 | 최근 N초 이동 윈도우 대비 ΔP 임계(예: +800W 3초 유지 → ON, 히스테리시스로 OFF). 가전 시그니처는 MVP에서 규칙 기반(전기포트/인덕션 임계값 표). 모델 연동은 인터페이스만(`ApplianceClassifier`) | ON/OFF 30회 시나리오 80% 이상 판정 |
| D3 | F-6 `baseline.v1` Broadcast State | 가구별 루틴 기준선(예: "아침 07~09시 전기포트 ON")을 `baseline.v1` 토픽에서 Broadcast State로 로드. 초기값은 CSV → 토픽 적재 스크립트 | 기준선 변경 후 재배포 없이 판정 반영 |
| D4 | F-7 부재 타이머 → `ROUTINE_MISSED` | KeyedProcessFunction 이벤트 시간 타이머. 기준 시각 + 유예 도달 시 미사용이면 `ROUTINE_MISSED`(score 80), 이후 해당 가전 ON 감지 시 `ANOMALY_CLEARED`(같은 `correlation_key`). 기준선 없는 가구는 `LEARNING` 상태 snapshot 플래그 | 리플레이로 미사용 시나리오 → 이벤트 1건 → ON → 해소 1건 |
| D4 | F-8 Checkpoint·복구 | Checkpoint 30초, 상태 백엔드 RocksDB. MVP는 로컬 디스크 checkpoint(`/data/flink/ckpt`), S3 자격 나오면 `s3a://`로 전환. TaskManager 강제 종료 → 재기동 → 중복 없이 이어짐 확인 | 종료 전후 이벤트 수 동일 |
| D5 | 통합·부하 | EC2-B compose에 JobManager(1G)·TaskManager(2.5G, 슬롯 2) 추가, Application 모드 이미지. 리플레이 1,000가구 부하 시 Lag·Checkpoint 시간 기록. Flink Web UI 캡처 | §6 체크리스트 |

**BE-2 리스크**: Flink 학습 곡선. D2 저녁까지 F-4(Snapshot)가 EC2-B에서 안 돌면 **폴백**: Python Kafka Consumer Group(서버역할분리 문서의 1안 폴백)으로 F-5~F-7을 구현하고 Flink는 D5 이후로 이연. 폴백 결정은 D2 회고에서 즉시 내린다.

---

### 4.3 BE-3 — Monitoring Service

**목표**: `analysis.*` 소비 → Redis 최신 상태 + PostgreSQL incident → SSE로 대시보드 전달. 담당자가 사건을 확인·오탐 처리.

| 일 | 작업 | 상세 | 완료 기준 |
| --- | --- | --- | --- |
| D1 오후 | M-1 스키마 | `V2__monitoring.sql`: `analysis_event`(event_id PK, household_id, event_type, score, occurred_at, payload jsonb), `incident`(id, correlation_key UNIQUE-open, household_id, type, severity[WATCH/CONFIRM], status[OPEN/ACKED/RESOLVED], resolved_by, resolution_code, opened_at, resolved_at), `incident_action`(incident_id, actor, action, note, at), `ui_outbox`(id, event_name, payload, created_at, published_at) | Flyway 적용, 엔티티 4개 |
| D2 | M-2 Kafka Consumer (Spring Kafka) | `spring-kafka` 의존성 추가. Snapshot Consumer → Redis Hash `house:{id}:latest`(TTL 120s). Event Consumer → `analysis_event` insert(`event_id` Unique로 멱등) → incident 규칙(패턴감지 §4.2: 80점 → OPEN/CONFIRM, `ANOMALY_CLEARED` → RESOLVED by SYSTEM) → `ui_outbox` insert **같은 트랜잭션**. `isolation.level=read_committed`, 수동 ack | 같은 이벤트 2회 소비 시 incident 1건 |
| D2 | M-3 Outbox 퍼블리셔 | `@Scheduled` 500ms 폴링 → 미발행 outbox → SSE Hub 전달 → `published_at` 갱신. Hub는 in-memory `ConcurrentHashMap<userId, List<SseEmitter>>` | DB에 기록된 이벤트가 1초 내 SSE로 도달 |
| D3 | M-4 SSE 엔드포인트 | `GET /api/monitoring/stream` (`SseEmitter`, timeout 0), 25초 heartbeat comment, `Last-Event-ID`로 outbox id 기준 재전송. 권한 필터: JWT의 role·`household` 관계로 구독 가구 제한(ADMIN=담당 가구 전체, GUARDIAN=연결 가구, USER=본인). 가구-사용자 관계 테이블 `household_access`는 MVP에서 Monitoring DB에 두고 Device 쪽 이관은 이후 | 브라우저 2개(다른 role)에서 각자 가구 이벤트만 수신 |
| D3 | M-5 조회 API | `GET /api/monitoring/households`(그리드: Redis 최신 상태 + 열린 incident 수), `GET /api/monitoring/households/{id}`, `GET /api/monitoring/incidents?status=`, `GET /api/monitoring/incidents/{id}` | Swagger, 프론트 연동 가능 |
| D4 | M-6 대응 API | `POST /incidents/{id}/ack`, `POST /incidents/{id}/resolve`(resolution_code: FALSE_POSITIVE/HANDLED), `incident_action` 기록, `ui_outbox`로 상태 변경 브로드캐스트. 알림은 MVP 범위대로 **인터페이스만**(`NotificationPort`, 로그 구현) | 담당자 확인 → 다른 브라우저 카드 즉시 갱신 |
| D4 | M-7 `SENSOR_GAP` 운영 이벤트 | Redis 최신 상태 TTL 만료 감지(`@Scheduled` 30초 스캔) → `SENSOR_GAP` incident(운영형, 알림 없음). Device Service `last_seen_at`은 여기서 REST 호출로 갱신하지 않고 **Kafka `device.status.v1`** 발행만(Device 소비는 다음 주) | 리플레이 중단 2분 후 카드 배지 표시 |
| D5 | 통합·측정 | EC2-A compose Monitoring env에 `KAFKA_BOOTSTRAP=<B 사설IP>:9092` 추가(보안그룹 A→B 9092 확인). 완료 기준 1번 측정: 리플레이 ON 시각 → SSE 수신 시각 p95 | §6 체크리스트 |

**BE-3 리스크**: Kafka 접속이 EC2-A→B 크로스 노드다. D2 오전에 로컬 compose가 아니라 **EC2-B 실제 Kafka**로 한 번 붙여 보안그룹·advertised listener 문제를 먼저 잡는다.

---

## 5. 일자별 통합 뷰

```text
        BE-1 (Edge·인증)              BE-2 (Stream)                 BE-3 (Serving)
D1 AM  ┌──────────── 계약 고정 C1·C2·C3 (전원) ─────────────────────────────────┐
D1 PM  G-1 SSE 통과, G-2 JWKS       F-1 골격, F-2 토픽 통일        M-1 스키마
D2     G-3 토큰 분리, G-4 config    F-3 전처리, F-4 Snapshot       M-2 Consumer, M-3 Outbox
       ─── D2 저녁 회고: Flink 폴백 여부 결정 ───
D3     I-1 스키마, I-2 등록 API     F-5 변화점·판정, F-6 baseline  M-4 SSE, M-5 조회 API
D4     I-3 Mosquitto ACL, I-4 폐기  F-7 ROUTINE_MISSED, F-8 ckpt   M-6 대응 API, M-7 SENSOR_GAP
D5     ┌──────────── 통합 리허설: 리플레이 → Flink → Monitoring → SSE → 대시보드 ─────┐
       측정·문서·회고
```

**의존 관계**
- M-2는 C1 스키마만 있으면 F-4 완료 전에도 **테스트 프로듀서**로 진행 가능. BE-3이 D1 오후에 `analysis.event.v1` 샘플 JSON 10건을 스크립트로 만들어 공유한다.
- G-1(SSE 통과)은 M-4보다 먼저 끝나야 D3 오후에 Gateway 경유 SSE를 검증할 수 있다.
- I-3(ACL)은 Bridge 계정 권한을 바꾸므로 BE-2와 D4 오전에 합의 후 적용한다.

---

## 6. D5 완료 체크리스트 (3안 §8 완료 기준 매핑)

| # | 확인 항목 | 담당 | 3안 완료 기준 |
| --- | --- | --- | --- |
| 1 | 리플레이 가구 포트 ON → 대시보드 표시 5초 이내 (p95 기록) | BE-3 측정, BE-2 협력 | 1 |
| 2 | ON/OFF 30회 시나리오 판정 80% 이상 | BE-2 | 2 |
| 3 | 110msg/s 10분, Flink Lag 0, Monitoring `analysis_event` 건수 = 발행 건수 | BE-2·BE-3 | 3 |
| 4 | `ROUTINE_MISSED` 발생 → incident OPEN → 포트 ON → 자동 RESOLVED | BE-2·BE-3 | 6 |
| 5 | Gateway 경유 SSE 30분 유지, Keycloak 중단 5분간 기존 토큰 유효 | BE-1 | 운영 규칙 |
| 6 | 등록 기기만 자기 가구 토픽 발행 가능, 폐기 기기 거부 | BE-1 | 보안 |
| 7 | 사용자 토큰으로 기기 경로 401, 기기 토큰으로 사용자 API 401 | BE-1 | 보안 |
| 8 | TaskManager 강제 종료 후 이벤트 중복·유실 0 | BE-2 | 3 |
| 9 | 컨테이너 `mem_limit` 적용, `docker stats` 캡처 (A 6.8G·B 9.6G 예산 대비) | BE-1 | §6 |
| 10 | 계약 문서 3개 + 각 서비스 README 갱신 | 전원 | 문서 |

---

## 7. 이번 주 범위에서 의도적으로 제외한 것

| 제외 항목 | 이유 | 다음 주 담당 제안 |
| --- | --- | --- |
| 문자·카카오 실제 발송 | 패턴감지 MVP 축소판 기준 인터페이스만 | BE-3 |
| GPU 모델 추론 연동 | AI 파트 추론 API 명세 미확정. `ApplianceClassifier` 인터페이스로 자리만 확보 | BE-2 + AI |
| Archive Sink → S3 Bronze | S3 자격 미발급(3안 §12-8). HDFS 로더는 현재대로 유지 | BE-2 또는 데이터 담당 |
| 부재 등록 API, 에스컬레이션 스케줄러 | Phase 2 | BE-3 |
| Device Service의 `device.status.v1` 소비 | M-7에서 발행만 | BE-1 |
| Spark job7/job8 | 백엔드 3인 범위 밖(데이터 파트) | — |

---

## 8. 운영 규칙

- **브랜치**: `feature/back/gateway-sse`, `feature/back/device-auth`, `feature/back/flink-analysis`, `feature/back/monitoring-consumer` 등 작업 단위로 분리. develop 병합은 매일 1회 이상.
- **일일 15분 싱크**: 18:00, 계약(C1~C3) 변경 요청은 이 자리에서만. 변경 시 문서 먼저 수정 후 코드.
- **compose·env 변경**: BE-1만 직접 수정. 다른 담당은 필요한 env 키를 BE-1에게 MR 코멘트로 요청.
- **D2 저녁 Flink 폴백 판단**은 BE-2 단독이 아니라 3인이 함께 내린다. 폴백하면 F-5~F-7은 Python Consumer로 옮기고 BE-2 D5 작업에 "Flink 재도입 계획서"를 추가한다.

---

## 부록 A. MQTT(Mosquitto) 분산 가능성 검토

> 검토 대상: `infrastructure/mqtt/config/mosquitto.production.conf`, `infrastructure/mqtt-kafka-bridge/bridge.py`, EC2-A/B compose. 검토일 2026-09-07.

### A.1 결론

**Mosquitto 브로커 자체는 분산(클러스터링)이 안 된다. 그러나 이 프로젝트의 병목은 브로커가 아니라 Bridge 1대이고, Bridge는 Mosquitto 기능(MQTT 5 공유 구독)만으로 수평 확장이 가능하다.** 이번 주에는 브로커를 단일로 두고 Bridge를 2대로 늘리는 것이 3안 원칙(B 외부 미노출, A 메모리 예산)을 지키면서 "수집 경로 분산"을 보이는 유일한 현실적 방법이다.

### A.2 Mosquitto가 할 수 있는 것과 없는 것

| 방식 | 가능 여부 | 설명 | 이 프로젝트 적합성 |
| --- | --- | --- | --- |
| 클러스터링(세션·구독 상태 공유, 부하 분산) | **불가** | Mosquitto는 설계상 단일 프로세스. 노드 간 세션·retained·QoS 인플라이트 상태를 공유하는 기능이 없다 | — |
| 브로커 브리지(broker-to-broker 전달) | 가능 | `connection` 블록으로 다른 브로커에 토픽을 전달. 계층형(엣지 → 중앙) 토폴로지용 | 현장별 엣지 브로커가 생길 때 유효. EC2 A↔B 사이에 두면 홉만 늘고 B 공개 포트 원칙 위배 |
| L4 로드밸런서 뒤에 Mosquitto N대 | 조건부 | Nginx `stream`으로 8883을 분배 가능. 그러나 세션이 브로커마다 따로 있어 재접속 시 다른 노드로 가면 QoS 1 미전달 큐·persistent session이 사라진다. Bridge는 N대 모두를 각각 구독해야 한다 | 유실 0 목표(완료 기준 3)와 충돌. 비권장 |
| MQTT 5 공유 구독 `$share/<group>/<topic>` | **가능** (Mosquitto 1.6+, 현재 이미지 2.x) | 같은 그룹의 구독자 여러 개에 메시지를 분배. 구독자(=Bridge) 수평 확장 | **권장.** 브로커 변경 없이 Bridge만 수정 |
| 브로커 교체: EMQX / VerneMQ | 가능 | 진짜 클러스터링·공유 구독·세션 복제 지원. 노드당 메모리 0.5~1GB(Erlang VM) | 2노드 클러스터를 만들려면 B에 브로커를 두어야 하는데 B는 외부 포트를 열지 않는다. A 한 대에 2노드는 의미 없음. 로드맵 4단계(HA) 항목 |

### A.3 실제 병목: Bridge 인플라이트 상한

현재 Bridge는 다음 조합으로 동작한다.

- QoS 1, `manual_ack=True`, **Kafka 전달 확인 후에만 PUBACK** (`bridge.py` `delivered` 콜백)
- MQTT v3.1.1 (paho 기본값. `protocol` 미지정)
- Mosquitto `max_inflight_messages` 미설정 → **기본 20**

브로커는 클라이언트별로 미확인 QoS 1 메시지를 20건까지만 보내고 멈춘다. Bridge가 한 건을 ACK하기까지 Kafka 왕복(`linger.ms 20` + A→B 사설망 + `acks=all`)이 약 25~50ms이므로:

| 항목 | 값 |
| --- | --- |
| 인플라이트 상한 | 20건 |
| ACK 지연(추정) | 25~50ms |
| **Bridge 1대 처리 상한** | **약 400~800 msg/s** |
| MVP 부하(110 msg/s) | 여유 있음 |
| 1,000가구 × 1Hz | **초과 가능** |

즉 완료 기준 3번(110 msg/s)은 지금 구조로 충분하지만, 채지현 님 다음 할 일에 있는 "100 → 1,000가구 부하 테스트"에서 이 상한이 먼저 드러난다. 브로커 CPU가 아니라 이 인플라이트 창이 병목이다.

### A.4 권장 조치 (BE-1 · BE-2 공동, 0.5일)

| # | 조치 | 담당 | 내용 |
| --- | --- | --- | --- |
| 1 | `mosquitto.production.conf`에 `max_inflight_messages 200`, `max_queued_messages 10000` 추가 | BE-1 | Bridge 1대 상한을 약 10배로 올린다. `max_connections 500`도 1,000가구 대비 `2000`으로 |
| 2 | Bridge를 MQTT v5 + 공유 구독으로 전환 | BE-2 | `mqtt.Client(..., protocol=mqtt.MQTTv5)`, `clean_session` 대신 `clean_start=False` + `SessionExpiryInterval` 속성, 구독 토픽 `$share/bridge/v1/power/+/main`. 기존 단위 테스트(`test_bridge.py`)의 ACK 규약은 그대로 유효 |
| 3 | EC2-B compose에 `mqtt-kafka-bridge` `deploy.replicas: 2` (또는 서비스 2개, `MQTT_CLIENT_ID`를 다르게) | BE-1 | 두 인스턴스가 같은 공유 그룹으로 구독. 한 대를 죽여도 나머지가 전량 수신 |
| 4 | ACL에 공유 구독 반영 | BE-1 (I-3와 함께) | `kafka_bridge_user`에 `$share/bridge/v1/power/#` 읽기 허용. Mosquitto ACL은 `$share/` 접두를 뗀 실제 토픽으로 평가하므로 기존 `v1/power/#` read 규칙으로 충분한지 D4에 실측 |
| 5 | 순서 영향 확인 | BE-2 | 공유 구독은 같은 가구의 연속 메시지를 다른 Bridge로 보낼 수 있어 Kafka 파티션 안에서 순서가 뒤바뀔 수 있다. Kafka key는 여전히 `household_id`라 파티션은 같다. Flink F-3의 이벤트 시간 워터마크(허용 지연 10초)가 이를 흡수하는지 리플레이로 검증 |

### A.5 이번 주에 하지 않는 것

- **브로커 2대 배치**: 3안의 B 비공개 원칙과 A 메모리 예산을 그대로 두는 한 이득이 없다. 시연에서 "MQTT 분산"을 보여야 한다면 Bridge 2대 중 1대 종료 → 수신 지속 화면으로 대체한다.
- **EMQX 전환**: 클러스터링이 필요한 시점(HA 로드맵, 현장 다중 사이트)에 검토. 그때는 Bridge가 아니라 브로커가 공유 구독을 클러스터 전역으로 처리한다.
- **현장 엣지 브로커 → 중앙 Mosquitto 브리지**: 실물 ESP32가 여러 현장에 배치될 때의 토폴로지. MVP 리플레이 데이터에는 해당 없음.

