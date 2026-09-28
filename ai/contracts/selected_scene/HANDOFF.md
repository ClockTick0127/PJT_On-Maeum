# 담당자 인계 — 계약 revision 1

아래 항목은 **요청·합의 대상**이며 이 브랜치에서 해당 서비스가 수정되었다는 뜻이 아니다.
명세와 `validate.py`, examples를 함께 전달한다. 외부 담당자의 완료 전에는 최종 연동 완료로 표시하지 않는다.

| 담당 | 요청 사항 | 수락 시험 |
|---|---|---|
| AI 후속 2번 | 잠긴 프로파일과 계약의 엄격한 입력 검사, 255초 창·단일 decoder, 복구·멱등성 유지 | 6개 선정 장면의 기존 점수/상태 회귀, 잘못된 run/profile 거부 |
| AI 후속 3번 / 시뮬레이터 | fixture 선택, TLS MQTT 발행, run/profile/source_index/마스크 보존 | raw 네 feature 왕복 일치, 재전송 동일 실행 유지 |
| AI 후속 4번 | 세션 투영과 위험 이벤트를 저장 후 신뢰성 있게 발행 | SYNC/EOF로 가짜 시작·종료 없음, 중복 세션·위험 이벤트 없음 |
| 인프라 | 전용 MQTT demo 경로·Kafka 4개 토픽, 가구 partition key, TLS·권한, 가구당 단일 worker | sim 운영 경로 혼입 없음, consumer 준비 후 fixture 발행 |
| 백엔드 | 상태 v2·세션·위험 이벤트 소비, run/profile 격리, revision 멱등성, 기존 조회·알림 생성 adapter | UNKNOWN 보존, 과거 revision 덮어쓰기 금지, 테스트 가구 알림 1개 생성 |
| 프론트엔드 | 인증된 서비스 조회로 상태·사용 세션 표시, 부분 분석/준비/중단 구분 | 미분석 가전을 OFF로 표시하지 않음, 서로 다른 장면 합치지 않음 |

## 연결 순서와 토픽

1. 이 계약과 합성 예제로 backend/frontend 수신·표시 규칙을 확인한다.
2. 후속 AI 런타임·발행기·이벤트 구현과 인프라 토픽/이미지 설정을 준비한다.
3. 전용 수신 consumer를 먼저 켠 뒤 테스트 가구 worker와 fixture 발행을 시작한다.
4. 상태는 analysis.scene.v2, 세션은 analysis.scene-session.v1, 위험은 analysis.scene-event.v1에서 확인한다.
5. 실제 알림 **레코드 생성까지만** 확인한다. 실제 푸시 전송은 이번 범위가 아니다.

기존 사용자에게 테스트 결과가 연결되지 않도록 테스트 가구 매핑은 담당자가 별도로 제공한다.
TLS credential·실제 가구/계정 정보는 예제에 넣지 않는다. 기존 운영 위험 정책은 유지한다.
되돌릴 때 전용 worker/consumer를 끄며 기존 서비스나 DB 기록을 삭제하지 않는다.

## 합의해야 하는 배포 식별자

구현 계약은 여기서 고정하되 MQTT endpoint/CA/credential, 테스트 household와 service account,
운영 인증 정보는 환경별 설정으로 받는다. 담당자 확인 없이 추정하거나 저장소에 기록하지 않는다.
분석 서비스가 직접 사용자 화면·푸시를 호출하는 구조는 사용하지 않는다.

## 검증 책임의 경계

오프라인 검증기는 단일 JSON의 타입·nullable·범위·필드 관계만 보장한다. topic ACL,
Kafka ordering, DB deduplication, 프로파일 실물 SHA, 상태 연속성, 모델 실제 forward 및 화면
표시는 후속 구현과 통합 시험의 책임이다. 합성 예제를 실제 모델 성능 증거로 사용하지 않는다.
