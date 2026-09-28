# AI 순차 브랜치 인수 절차

이 패키지는 선정 장면 한 개의 실제 추론 경로다. 서로 다른 집의 6개 장면을 한 가구의
동시 추론으로 합치지 않는다. 추론 대상 외 5종은 UNKNOWN이다.

## 로컬 재현

저장소 루트에서 Docker Desktop과 OpenSSL을 준비한다. 먼저 `README.md`의 명령으로
`nilm-scene-inference:local`, `nilm-scene-fixture:local` 두 이미지를 빌드한다.

```powershell
./ai/deployment/test-scenes.ps1 -OutputDirectory C:/temp/nilm-ai-acceptance-new
```

Windows Git의 OpenSSL은 `-OpenSSL 'C:/Program Files/Git/usr/bin/openssl.exe'`로 지정할 수 있다.
출력 디렉터리는 새 경로이며 저장소 밖이어야 한다. 스크립트는 `nilm-ai-acceptance` 프로젝트의
전용 PostgreSQL/Kafka/MQTT/bridge/worker만 사용하고 순차적으로 6개 장면을 실행한다.
다른 사람이 같은 테스트 프로젝트를 실행 중이면 먼저 조율한다. 성공하면 해당 스택만 정지하며
실패하면 진단을 위해 남겨둔다. 기존 demo나 운영 컨테이너는 대상이 아니다.

TLS 서버 인증서와 호스트명을 검증한다. 로컬 테스트 브로커는 호스트 포트가 없고 익명 연결을
허용한다. 이 시험은 운영 MQTT 계정/ACL 검증을 대신하지 않는다. 테스트용 비밀키는 2일짜리며
출력 디렉터리에만 남는다. 운영 구성에는 `compose.example.yaml`과 별도 자격 증명을 사용한다.

기본 `-Interval 0`은 가속 기능 시험이다. 실제 1Hz 수락 시험은 `-Interval 1`로 다시 실행한다.
발행 ACK는 모델·서비스 완료로 판정하지 않는다. verifier가 다음 사항을 모두 확인해야 PASS다.

- 실제 Kafka snapshot 전 행과 PostgreSQL 증거의 일치, 입력 Kafka provenance 존재
- frozen profile/model/정규화 SHA, 원본 P/Q/PF/I 왕복, 정확한 시각·source_index·255초 readiness
- 대상만 inferred, 나머지 UNKNOWN, run/profile/가구 격리
- 관측된 전이에서 계산한 최종 세션 및 10초 테스트 정책 위험 이벤트의 일치
- outbox 전 행 ACK 완료와 동일 payload의 Kafka 전달, 누락·충돌 중복·추가 이벤트 없음

`broker/<run>-readback/summary.json`과 JSONL이 결과다. 이 결과는 **AI 브로커 경로**이며
backend API, 알림 생성, 프론트엔드, EC2 배포는 `NOT_RUN`이다. 모델 수치의 H200 BF16
동등성은 별도 `ai/tools/evaluate_scenes.py` 회귀 결과를 함께 확인한다.

## Python 및 PostgreSQL 검사

서비스 폴더에서 `python -m pytest -m 'not postgres'`를 실행한다. 실제 모델 검사는
inference/dev 의존성과 `ai/assets`가 있어야 한다. 서비스만 복사하는 기존 Docker CI에서는
자산·torch·PostgreSQL 전용 검사가 skip되므로 전체 검수로 간주하지 않는다.

```powershell
docker build --target test -t nilm-scene-ci:local ai/realtime-analysis-service
docker run --rm --network nilm-ai-acceptance_default `
  -e TEST_DATABASE_URL=postgresql+psycopg://test:local-disposable-only@postgres:5432/analysis_test `
  --entrypoint python nilm-scene-ci:local -m pytest -q -m postgres
```

위 PostgreSQL 검사는 schema를 초기화한다. **오직 전용 폐기 가능한 test DB에서, worker를
정지한 상태로 실행한다.** migration 왕복·outbox와 실제 advisory lock의 가구 독점/해제를 검사한다.

## 서비스 담당자 인계와 최종 수락

구현 계약은 `../contracts/selected_scene/HANDOFF.md` 및 `../realtime-analysis-service/docs/scene-events.md`다.
AI 밖의 코드는 이 기능 브랜치에서 수정하지 않는다.

| 담당 | 구현/검수 내용 | 제출 증거 |
|---|---|---|
| 인프라 | 전용 demo MQTT 경로/ACL/TLS, Kafka 토픽, 가구별 단일 worker, 모델 이미지와 migration 배포 | 이미지 digest, readiness, broker 인증 성공/실패, 1Hz 전체 실행 |
| backend | snapshot v2·session revision·risk consumer, immutable ID 중복 방지, 기존 조회 adapter | 인증 API 조회 원문, DB 유일성 및 중복 발행 후 건수 |
| backend 알림 | 테스트 가구 위험 event_id로 알림 생성, 실제 외부 push는 시험 범위 제외 | risk event_id → notification_id 매핑, 한 위험당 한 알림, 재전송 후 추가 생성 0 |
| frontend | 인증 조회로 대상 상태/세션 표시, UNKNOWN·warmup·중단 구분 | 같은 run의 화면 증거, 시각/세션/위험 일치 |

backend 담당자는 **해당 가구/run/profile의 전체 결과**를 계약 payload 형식으로 export한다.
페이지 누락 없이 `snapshots.jsonl`, 현재 최종 revision의 `sessions.jsonl`, `risks.jsonl`을 제공한다.
인증 토큰·개인정보는 패키지에 넣지 않는다. 아래 검사는 전송 자체가 아니라 export 내용의 무결성을 검사한다.

```powershell
python -m realtime_analysis.scene_acceptance readback --asset-root ../assets/nilm_r3 `
  --appliance kettle --household TEST_HOUSE --run-id RUN_ID `
  --start-time 2026-09-21T00:00:00+00:00 --directory C:/temp/backend-export
```

위험 비활성 실행은 `--risk-threshold 0`, 기본 테스트 정책은 10초다. 다른 policy_id를 사용하면
검수기의 정책 설정도 합의해 변경한다. 읽기 결과 PASS만으로 알림/UI 완료를 표시하지 않는다.
최종 수락은 API 결과 검증 + 알림 생성 DB 증거 + 실제 화면 + 1Hz 배포 실행까지 모두 확인한다.

## 브랜치 병합 순서

`inference-runtime` → `fixture-replay` → `service-events` → `deployment-package` → `inference-e2e`.
각 feature는 develop에서 만들고 직전 feature를 의존성 merge했다. 앞 PR부터 develop으로 병합한다.
앞 PR을 squash하면 후속 PR에 이전 변경이 다시 보일 수 있으므로 병합 방식과 후속 branch 정리를
검토자가 맞춘다. 현재 작업은 로컬 커밋이며 원격 push/PR/EC2 배포는 별도 실행이다.
