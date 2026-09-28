# 인프라 담당자용 실제 모델 이미지

기본 배포는 `configurable-inference` target을 사용해 같은 이미지에서 `fake`, `real`,
`selected_scene`을 환경변수로 전환한다. fail-closed 검증이 필요한 배포에는
`realtime-inference` 또는 `inference` target을 별도로 사용할 수 있다.

저장소 루트에서:

```sh
docker build -f ai/deployment/Dockerfile --target realtime-inference -t nilm-realtime-inference:REV ai
docker build -f ai/deployment/Dockerfile --target configurable-inference -t nilm-configurable-inference:REV ai
docker build -f ai/deployment/Dockerfile --target inference -t nilm-scene-inference:REV ai
docker build -f ai/deployment/Dockerfile --target fixture-publisher -t nilm-scene-fixture:REV ai
docker run --rm --entrypoint python nilm-realtime-inference:REV -m realtime_analysis.asset_check --asset-root /assets
docker run --rm --entrypoint python nilm-scene-inference:REV -m realtime_analysis.asset_check --asset-root /assets
docker run --rm nilm-scene-fixture:REV --help
```

`configurable-inference`는 Jenkins build와 EC2 Compose의 기본 분석 서비스가 사용한다.
`MODEL_BACKEND`로 세 모드를 선택하며, `real`은 255행 창을 요구하고
`selected_scene`은 대상 가구와 격리된 토픽·컨슈머 그룹 설정을 추가로 요구한다.
`realtime-inference`는 `real`, `inference`는 `selected_scene`만 허용하는 전용 target이다.

로컬 Compose의 `local-inference`는 같은 configurable target의 호환 alias다.
로컬과 EC2-B 모두 `ANALYSIS_MODEL_BACKEND` 값으로 모드를 전환할 수 있다.
전용 `realtime-inference`와 `inference` target의 fail-closed 제한은 그대로 유지한다.

추론 이미지는 체크포인트 6개·norm·잠긴 모델 코드를 포함한다. 별도 host asset mount가 필요 없다.
발행 이미지는 raw panel만 포함한다. 두 이미지 모두 정답·기준 점수·외부 credential을 포함하지 않는다.
빌드 시와 worker 시작 시 SHA를 검사한다. 운영용 worker는 fake 설정 또는 가구 미지정 시 시작을 거부한다.
CPU float32/thread 1/batch 1이 기본이며 worker 시작 전에 Alembic head를 적용한다.
공유 DB에 여러 가구 worker를 처음 배포할 때 migration을 먼저 한 번 완료한 뒤 순차 시작한다.
가구 lock은 모델 처리 독점용이며 Alembic 동시 migration을 직렬화하지 않는다.

`worker.env.example`을 secret store 기반으로 채우고 `compose.example.yaml`의 변수를 연결한다.
fixture와 worker의 가구/run/profile 선택은 반드시 동일해야 한다. fixture MQTT 인증은 MQTT_USER,
MQTT_PASS 환경변수, CA는 읽기 전용 mount다. non-root UID 65532에 report 디렉터리 쓰기 권한을 준다.
token/password를 build arg로 전달하거나 이미지에 COPY하지 않는다.

인프라가 만들 Kafka 토픽: power.scene.v2, analysis.scene.v2, analysis.scene-session.v1,
analysis.scene-event.v1, dlq.scene. demo MQTT topic을 power.scene.v2로 전달할 별도 bridge가 필요하다.
분석 readiness는 DB·모델·입력/출력 토픽을 검사한다. downstream consumer까지 준비된 뒤 publisher를
실행하고, 중지·재시도 시 run과 start-time을 유지한다. 위험 이벤트는 테스트 가구에서만 명시 활성화한다.

DB 추가: selected_scene_evidence, selected_scene_projection, selected_scene_usage,
selected_scene_outbox. 기존 테이블을 DROP하지 않는다. 단일 가구 worker는 PostgreSQL lock으로 보호한다.
backend는 revision/event_id 멱등성을 구현하고 부분 분석 상태를 기존 조회로 연결해야 한다.

롤백은 publisher와 scene worker를 중지하고 이전 immutable image로 되돌린다. DB downgrade나
볼륨 삭제를 자동 실행하지 않는다. 새 계약 consumer는 담당자가 별도로 중지한다.
기존 monitoring/일일 집계와 테스트 가구의 데이터를 합치지 않는다. 실제 push 알림은 비활성으로 유지한다.

로컬 실제 브로커 재현·서비스 인수 절차는 [ACCEPTANCE.md](ACCEPTANCE.md)를 따른다.
재현 스크립트를 실행할 때 위 이미지 태그의 `REV`는 `local`로 지정한다.
