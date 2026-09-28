# 2026-09-21 로컬 검수 결과

기준 develop aa8d8bb, 변경 범위 ai/만. CPU torch 2.11.0+cpu, float32, thread 1, batch 1.
`test-scenes.ps1` batch `aec97a17a03a`로 TLS MQTT → bridge → 전용 Kafka → 실제 모델 →
PostgreSQL 증거/세션/outbox → Kafka 출력 readback을 검수했다.

| 장면 | 원본 행 | READY | 세션 | 테스트 위험 | 결과 |
|---|---:|---:|---:|---:|---|
| kettle | 592 | 338 | 1 | 1 | PASS |
| induction | 585 | 331 | 3 | 2 | PASS |
| iron | 591 | 337 | 1 | 1 | PASS |
| microwave | 825 | 571 | 1 | 1 | PASS |
| hair_dryer | 761 | 507 | 1 | 1 | PASS |
| vacuum_cleaner | 613 | 359 | 1 | 1 | PASS |
| 합계 | 3967 | 2443 | 8 | 7 | PASS |

induction의 SYNC ON 세션은 시작을 추정하지 않아 위험을 생성하지 않는다. 테스트 정책은
명시한 ai-acceptance 가구의 연속 ON 10초다. raw 값·event time·index·Kafka provenance·
자산 SHA와 실제 저장/발행 payload가 일치했다. outbox pending 없이 전체 전달을 확인했다.

이번 broker snapshot의 별도 H200 BF16 기준 비교: 상태 불일치 0, locked confusion counts 일치,
최대 확률 차이 0.0133075714. 확률이 완전히 동일하다는 뜻은 아니다. microwave의 기존
FP 1/FN 5도 유지되므로 무오류 모델로 표현하지 않는다.

저장소 루트 기준 외부 `../handoff/r3/ai-sequential/final-six/broker/`와
`../handoff/r3/ai-sequential/final-six/broker-reference-parity.json`에 증거를 보관했다.
원본 JSONL·DB·TLS 키는 커밋하지 않았다. 테스트 스택은 종료했고 기존 demo 컨테이너는 유지했다.

## 자동 검사

- 실제 자산 Python: **173 passed**, PostgreSQL 10 deselected.
- 폐기 가능한 PostgreSQL 18: **10 passed**, 나머지 173 deselected.
- 기존 서비스 Docker CI: **151 passed, 32 skipped**. torch/자산/DB가 없는 context의 skip은 위 별도 검사로 보완.
- 계약 unittest: **14 passed** (런타임 통합 단계).
- 추론·발행 이미지 빌드, 자산 SHA, 컨테이너 실제 forward 통과.
- 실제 모델 SQLite에서 handler 복구·전체 재전송 시 ID 유지, 추가 세션/위험 생성 없음.
- 검수기는 누락·raw 변형·wrong run·위험/세션 누락·Kafka provenance 부재를 거부.
- PostgreSQL migration 왕복, 가구 advisory lock 중복 거부/해제 확인.
- outbox ACK 실패 재시도, snapshot/세션/outbox 동시 rollback, GAP/SYNC/EOF/CANCELLED 검사 통과.

## 외부 서비스 인수

이번 브로커 실행은 interval=0 가속 기능 시험이다. 전체 1Hz 실행, EC2 배포, 운영 계정/ACL,
backend 인증 조회, 알림 레코드 생성, UI 검수는 **NOT_RUN**이다. Kafka 위험 전달은 실제
알림 생성과 구분했다. 담당자 작업·수락 절차는 `ACCEPTANCE.md`에 있다.
새 feature의 원격 push/PR/배포는 실행하지 않았다.
