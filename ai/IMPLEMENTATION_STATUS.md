# 후속 기능 구현 기록

기준 develop: aa8d8bb (1번 계약 병합 포함). 모든 새 변경은 ai/ 내부다.
각 feature는 develop에서 생성하고 직전 완료 feature를 의존성 merge한다.
develop/master 직접 push 또는 임의 배포는 하지 않는다. PR은 아래 순서로 검토·병합한다.

| 순서 | 브랜치 | 상태 |
|---|---|---|
| 2 | feature/ai/inference-runtime | 구현, Python 144 passed / PG 전용 8 제외 |
| 3 | feature/ai/fixture-replay | 구현, 발행기 시험 7 passed |
| 4 | feature/ai/service-events | 구현, 세션·위험 outbox 및 중단/복구 검사 |
| 5 | feature/ai/deployment-package | 이미지 2종 빌드, 컨테이너 실제 forward 및 기존 Docker CI 151 passed / 23 skipped |
| 6 | feature/ai/inference-e2e | 구현, TLS 브로커 6장면 PASS / Python 173 / PostgreSQL 10 passed |

## 2번 구현

미병합 real-model-predictor의 AI 코드/실제 자산만 복원했다. backend/frontend/infrastructure
변경을 가져오지 않았다. assets의 기존 byte/SHA를 유지하며 검증기와 런타임은
`realtime_analysis.scene_contracts`를 단일 계약으로 사용한다. standalone validator는 재수출한다.

입력 JSON은 strict boolean/number/timezone으로 검사하고 발행 전 Snapshot 계약을 검사한다.
배포 worker는 MODEL_HOUSEHOLD_ID가 필수이며 다른 가구를 거부한다. PostgreSQL advisory
lock을 가구 단위로 유지하고, 각 입력 처리 전 해당 연결이 살아 있는지 확인한다.
같은 가구의 다른 run worker도 동시에 실행할 수 없다. 중단 시 lock을 해제한다.
실제 모델 창·decoder·정규화와 durable 재시도는 유지하며 stage metric에 모델 forward를 기록한다.

선정 profile/window가 아닌 범용 6종 추론으로 확장하지 않는다. 원래 ai/evidence 검수 문서는
이전 브랜치에서 실시한 기록으로 보존되며 이 브랜치에서 외부 서비스가 구현됐다는 뜻이 아니다.

