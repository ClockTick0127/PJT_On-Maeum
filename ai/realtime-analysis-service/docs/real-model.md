# R3 실제 모델 Predictor 및 선택 장면 통합

`SelectedScenePredictor`는 고정 R3 체크포인트를 실제로 forward한다. 원본 모델 코드는
`real_models/models.py`에 그대로 보존했고, 서비스에 포함한 reviewer lock으로 자산 해시를
검증한다. 외부 Python 코드를 실행하거나 실패 시 fake 모델로 대체하지 않는다.

입력은 정규화하지 않은 P/Q/PF/I 255행이다. 정규화는 Predictor 내부에서 정확히 한 번
수행한다. **StandardizingPredictor로 감싸지 않는다.** 출력은 선택 기기 하나의 확률이다.
나머지 기기는 OFF가 아니라 미추론이다.

## 로컬 실행

서비스 디렉터리에서 Python 3.11 이상으로 실행한다. 체크포인트는 저장소 `ai/assets/nilm_r3`에 포함되어 있다.

```powershell
python -m venv .venv
.venv/Scripts/python -m pip install torch==2.11.0 --index-url https://download.pytorch.org/whl/cpu
.venv/Scripts/python -m pip install -e '.[inference,dev]'
.venv/Scripts/python -m realtime_analysis.model_replay --asset-root ../assets/nilm_r3 --appliance kettle --device cpu --dtype float32 --output ../evidence/kettle-new
```

출력 디렉터리는 매 실행마다 새 경로를 지정한다. `scores.jsonl`은 실제 forward 결과와
UNKNOWN/ON/OFF 및 SYNC/전환을 담고, `runtime.json`은 해시·dtype·장치·행 수를 기록한다.
평가용 저장 점수와 정답 파일은 실행 경로에서 읽지 않는다. CPU float32 결과를 H200 BF16과
동일하다고 간주하지 말고, 사후 source index별 점수·상태 비교 결과를 별도로 기록한다.

```powershell
$env:R3_TEST_ASSET_ROOT = (Resolve-Path ../assets/nilm_r3).Path
.venv/Scripts/python -m pytest tests/test_real_predictor.py -q
```

## 실시간 서비스 통합

`MODEL_BACKEND=real`이면 기존 `power.raw.v1` 소비, 활동 세션, 이상 탐지와
`analysis.snapshot.v1` 계약은 유지하면서 실제 체크포인트 6개를 모두 실행한다.
각 체크포인트는 같은 255행 원본 P/Q/PF/I 창을 입력받고 내부에서 자신의 정규화를
적용한다. `RealtimeModelPredictor`가 결과를 기존 가전 순서의 6개 확률로 합치며,
상태 판정은 reviewer lock의 가전별 ON/OFF threshold와 `confirm`을 사용한다.

```dotenv
MODEL_BACKEND=real
MODEL_ASSET_ROOT=../assets/nilm_r3
MODEL_WINDOW_SIZE=255
MODEL_DEVICE=cpu
MODEL_DTYPE=float32
MODEL_THREADS=1
```

체크포인트 하나라도 없거나 SHA가 다르거나 모델 로딩·forward가 실패하면 메시지 처리를
실패시키며 fake 결과로 대체하지 않는다. 로컬·EC2 Compose의 기본 분석 서비스 이미지는
가중치와 PyTorch를 포함한 `realtime-inference` target을 사용한다.

현재 잠긴 6개 profile은 서로 다른 가구의 선택 장면에서 각각 검증된 결과다. 따라서
6개 모델을 한 가구 스트림에 실행할 수 있게 된 것은 서버 통합 완료를 의미하지만,
임의 가구에 대한 6종 동시 정확도 검증을 의미하지 않는다. 운영 전 대표 가구 데이터로
교차 가구 정확도와 오탐률을 별도로 검수해야 한다.

## 선택 장면 통합

`MODEL_BACKEND=selected_scene`이면 `realtime-analysis` 진입점이 실제 Predictor와
`SceneHandler`를 사용한다. Python 설정의 기본값 `fake`는 단위 테스트와 명시적인
개발 실행을 위해 유지한다. 선택 모드는 별도 input topic, consumer group, run ID,
asset root를 필수로 요구한다.

입력은 run_id/profile_id/source_index/valid/context를 보존한다. context는 prefix 표시가
아닌 추론 유효성 마스크이며, 결측 행은 참조 norm의 mean으로 채워 정규화 후 0이 된다.
시간은 source index와 함께 정확히 1초씩 진행한다. 각 회차는 고정 prefix 첫 행부터 시작한다.

각 행의 스냅샷과 255행 입력 창·decoder 상태를 `selected_scene_evidence`에 원자적으로
저장한 뒤 Kafka `analysis.scene.v2`로 발행한다. 발행 실패/재전송 시 저장된 동일 payload를
재발행하며 중복 입력을 창에 다시 넣지 않는다. 재시작·partition 소유권 이전도 DB에서 복구한다.
같은 회차에서 다른 runtime이나 같은 source index의 다른 입력은 거부한다.

monitoring은 `APP_SCENE_DEMO_ENABLED=true`일 때 선택 장면 consumer와 관리용 조회 API를
활성화한다. `selected_scene_snapshots`는 여섯 항목의 확률/UNKNOWN/null을 그대로 저장한다.
run/profile별 조회로 회차를 분리하며 기존 가전 bool 상태·위험 평가·알림 경로를 호출하지 않는다.
보안이 활성화된 환경에서는 기존 `/api/monitoring/admin/**`의 ADMIN 정책이 적용된다.

실제 MQTT 발행과 별도 로컬 스택 실행은
[R3 시연 실행 안내](../../../infrastructure/r3-demo/README.md)를 참고한다.

검증 기준은 R3 C03/C04 및 TEST_MATRIX다. 단순 forward 성공은 긴 prefix·OFF 대조,
주동작 종료 검수 또는 실제 백엔드 연동 완료를 뜻하지 않는다.

## 2026-09-21 로컬 검증

- Python 3.12.14 / PyTorch 2.11.0+cpu, batch 1, thread 1.
- 기존 테스트 포함 132 passed. 별도 PostgreSQL을 요구하는 8개는 제외했다.
- kettle H046 592행 → 338 ready scores. CPU float32 cold 2회 점수 동일.
- H200 BF16 참조 대비 CPU float32 최대 절대 점수 차이 0.0082429051,
  CPU BF16은 0.0115510225. 두 설정 모두 상태·ready 불일치 0.
- 선택 평가 구간에서 TP 38 / TN 300 / FP 0 / FN 0 / UNKNOWN 0.
  SYNC source index 2490671, ON 2490791, OFF 2490829.
- TCN/LSTM/Transformer 실제 체크포인트 forward, 호출 사이 hidden state 비누적,
  잘못된 길이·비유한 입력·해시 불일치·마스크·미추론 UNKNOWN을 검증했다.

수치 평가는 이 kettle 선택 구간에 한정한다. 긴 prefix·OFF 대조 및 실제 transport,
  검수는 별도이며 아래 후속 결과를 참고한다.

후속 통합 검증: Python 140개 통과, PostgreSQL 전용 8개 제외. Java 전체 194개 중
190개 통과/4개 기존 비활성 테스트 제외. 실제 Python kettle 출력 592건을 Java consumer
코드에 넣어 Flyway V17/H2 DB 저장과 MockMvc API readback을 검증했다. 이는 실제 broker
전송 검증과 별개다. Windows Gradle의 한글 경로 argfile 오류는 동일 소스의 ASCII 임시
사본에서 테스트해 우회했다. 기존 고정 날짜 기반 프로필 테스트는 현재 날짜 기준으로 수정했다.

## 실제 broker / PostgreSQL / HTTP 검증

`nilm-r3-demo` 스택에서 실제 MQTT로 300행을 보낸 뒤 analysis만 재시작하고 나머지
292행을 전송했다. 입력 창을 DB에서 복구해 592행/338 ready scores를 처리했다.
monitoring PostgreSQL의 모든 payload와 원본 CSV 네 feature가 정확히 같았고,
H200 참조 상태 및 ON/OFF source index도 모두 일치했다. 최대 점수 차이는
0.0082429051이며 선택 구간 TP 38 / TN 300 / FP·FN·UNKNOWN 0이다.

같은 592행을 다시 MQTT로 전송한 뒤 두 DB 모두 592행을 유지했다. 입력/출력 Kafka
consumer offset은 각각 1184/1184, lag 0이었다. 최신 행과 ON(2490791), OFF(2490829)
행을 실제 HTTP 관리 API에서 조회해 PostgreSQL payload와 일치함을 확인했다.

분석/브리지는 저장소 Dockerfile로 빌드했다. monitoring은 현재 소스와 해시가 동일한
ASCII 경로 사본에서 전체 테스트·bootJar를 완료하고 JRE 런타임 이미지에 넣었다.
표준 monitoring Dockerfile 안의 Gradle 재다운로드는 느려 중단했으므로, 그 Dockerfile의
전체 빌드까지 완료했다고 간주하지 않는다. 저장소의 표준 빌드 설정은 유지했다.

추가 범위 검증: 6개 장면의 실제 broker/DB/API 왕복, fresh-process 반복 12회,
긴 prefix/OFF/다른 날짜 18개 대조군과 프론트엔드 데모를 완료했다.
현재 전체 결과는 [추가 검수 기록](../../evidence/VALIDATION.md)을 우선 참고한다.
