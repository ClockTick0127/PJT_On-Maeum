# 실제 추론 세션과 위험 이벤트

선택 장면은 `SCENE_EVENTS_ENABLED=true`일 때 별도 세션·outbox를 만든다.
legacy usage/일일활동/운영 알림 테이블은 변경하지 않는다. schema는 1번 계약과 같다.
AI 세션 저장과 snapshot/window 저장, outbox INSERT는 같은 DB 트랜잭션이다.
발행 ACK 후 published를 기록하며 ACK 유실은 동일 event_id로 재전송한다.
따라서 downstream도 event_id/revision 멱등성이 필요하다(exactly-once 전달을 주장하지 않는다).

세션은 `selected_scene_usage`, 재시작 상태는 `selected_scene_projection`, 발행 대기는
`selected_scene_outbox`에 저장한다. Alembic `20260921_18`이 추가한다.
ready 목표 가전만 세션을 만들며 SYNC ON은 start_known=false다. UNKNOWN은 GAP 중단,
고정 panel의 마지막 입력에서 아직 ON이면 EOF 중단이다. 둘 다 ended_at=null이고 OFF를 만들지 않는다.
정상 worker 재시작은 세션을 취소하지 않는다. 영구 중단은 worker를 먼저 멈춘 후 같은 설정으로
`python -m realtime_analysis.scene_cancel`을 실행한다. 가구 lock으로 실행 중 worker와 충돌을 막는다.

위험 이벤트는 기본 비활성이다. 테스트하려면 아래 세 값을 명시한다.

```dotenv
SCENE_EVENTS_ENABLED=true
MODEL_HOUSEHOLD_ID=r3-test-house
SCENE_TEST_HOUSEHOLD_ID=r3-test-house
SCENE_RISK_THRESHOLD_SECONDS=10
SCENE_POLICY_ID=selected-test-v1
```

known start부터 단절 없는 ON이 10초가 되면 세션/정책당 한 이벤트를 만든다.
test household가 다르면 생성하지 않는다. 가정 전체 무활동·루틴 판정은 생성하지 않는다.
정책 설정은 run 중간에 변경할 수 없고 새 run으로 시작한다. 모델 문턱은 그대로다.

출력: `analysis.scene-session.v1` / `analysis.scene-event.v1`. 가구 key로 발행하지만 서로 다른
토픽 사이 전달 순서는 보장되지 않는다. backend는 risk가 session보다 먼저 도착할 수 있음을
수용해야 한다. outbox는 시작 시 및 입력 처리 후 복구하며 발행 실패 시 입력 offset을 저장하지 않는다.
실제 푸시 전송이나 backend 알림 생성 자체는 이 AI 모듈에 포함하지 않는다.
