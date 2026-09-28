# AI 브랜치 develop 통합 검수

조사 기준: 원격 develop `3fbacc5`, 원격 feature/ai 브랜치 15개.
작업 중 갱신된 develop `77fe263`도 병합하고 변경된 백엔드 전체 시험을 다시 수행했다.
`git rev-list --left-right --count origin/develop...<branch>`로 조사했다.

미병합: `feature/ai/real-model-predictor`의 `f2c93a9`, `511190c`, `35765d7` 3개 커밋.

이미 병합된 14개: consumer-load-testing, consumer-rebalance-recovery,
deployment-package, detection-metrics, detection-sleep, fixture-replay,
inference-contract, inference-e2e, inference-runtime, pattern-detection,
realtime-consumer-scaling, realtime-model-integration, service-events, sonarcube.
모두 `feature/ai/` 접두사이며 develop에 없는 커밋 수가 0이었다.

## 충돌 해결

- develop의 실제 6모델 런타임, 엄격한 scene 계약, 가구 소유권 lease, 이벤트 outbox,
  기존 로그인/대시보드 경로를 보존했다. 이미 포함된 모델 자산은 다시 변경하지 않았다.
- 누락된 `/demo/ai`, selected-scene Java 저장/조회 API, 독립 데모 Compose, 평가 기록을 추가했다.
- 과거 데모의 Alembic `_15_selected_scene_evidence`는 develop `_17`과 동일한 테이블 생성이므로
  중복 파일을 제외했다. 기존 `_15` 정책 및 `_16`~`_18` 마이그레이션은 수정하지 않았다.
- Flyway 데모 테이블은 기존 V17 알림 변경과 충돌하지 않게 **V18**로 추가하고 업그레이드 시험을 갱신했다.
- 새 worker가 요구하는 household 설정을 Compose와 장면별 runner에 전달한다.
- 기존 실험 DB 볼륨의 번호 충돌 및 새 Compose 프로젝트 사용법을 안내했다. DB 삭제/repair는 수행하지 않았다.

## 검증

- Python: 205 passed, PostgreSQL 전용 10 deselected. Alembic 단일 head/고유 revision 포함.
- 프론트엔드: 23 passed, TypeScript + Vite production build 통과.
- 자산: 214파일 SHA256 MATCH, 6개 체크포인트.
- Compose config 검증 통과.
- 현재 통합 코드의 실제 kettle 592행 출력 생성 및 SQLite 저장 성공.
- 최종 Java 전체 시험: 최신 develop 반영 후 212개 중 **208 passed / 기존 disabled 4 skipped**.
  실제 출력 592행을 사용하는 SceneSnapshotFlowTest 3/3 통과(제외 0).
  Windows 한글 경로 문제로 ASCII 임시 폴더에서 실행했으며 모든 src 파일의 SHA256 동일함을 확인했다.

기존 작업 폴더와 미추적 문서는 그대로 두고 별도 worktree에서 작업했다.
develop 직접 push 대신 통합 브랜치의 merge request를 사용한다.
