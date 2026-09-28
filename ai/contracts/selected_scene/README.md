# 선정 장면 서비스 연동 계약 — revision 1

상태: **AI 측 제안 계약 / 상대 서비스 구현·승인 대기**.
이 브랜치는 검증기·예제·인계 명세만 제공한다. 추론, 세션 생성, broker 발행,
배포 설정, 서비스 화면 또는 알림 동작을 변경하지 않는다.

## 실행

저장소 루트, Python 3.11 이상과 `pydantic>=2,<3`가 필요하다.
기존 분석 서비스 가상환경을 재사용할 수 있다. GPU·torch·DB·MQTT는 필요 없다.

```powershell
ai/realtime-analysis-service/.venv/Scripts/python ai/contracts/selected_scene/validate.py measurement ai/contracts/selected_scene/examples/measurement-valid.json
ai/realtime-analysis-service/.venv/Scripts/python ai/contracts/selected_scene/validate.py snapshot ai/contracts/selected_scene/examples/snapshot-on.json --broker
ai/realtime-analysis-service/.venv/Scripts/python ai/contracts/selected_scene/validate.py session ai/contracts/selected_scene/examples/session-closed.json
ai/realtime-analysis-service/.venv/Scripts/python ai/contracts/selected_scene/validate.py risk ai/contracts/selected_scene/examples/risk-threshold.json
ai/realtime-analysis-service/.venv/Scripts/python ai/contracts/selected_scene/validate.py snapshot actual-output.jsonl --jsonl --broker
ai/realtime-analysis-service/.venv/Scripts/python ai/contracts/selected_scene/validate.py snapshot --schema
ai/realtime-analysis-service/.venv/Scripts/python -m unittest discover -s ai/contracts/selected_scene/tests -v
```

Linux에서는 가상환경의 `bin/python`을 사용한다. 성공은 exit 0, 데이터/파일 오류는 1,
잘못된 CLI 사용은 2다. 오류에는 위치와 종류만 기록하며 입력 전체를 출력하지 않는다.
`--schema`는 구조적 JSON Schema를 stdout으로 출력한다. 필드 간 의미 검사는 Python
검증기가 기준이다. 이 검증기는 레코드 단위이며 순서·멱등성·모델 해시 실물 대조는 아래
명세대로 런타임/수신 측이 검사해야 한다. 검증기 통과가 실제 추론 실행의 증거는 아니다.

## 채널과 식별자

| 종류 | 채널 | 계약 모델 | 상태 |
|---|---|---|---|
| 입력 | MQTT `v1/power/demo/{household_id}/main` → Kafka `power.scene.v2` | Measurement | 기존 selected_scene 입력 유지 |
| 초별 상태 | Kafka `analysis.scene.v2` | Snapshot, schema_version=2 | 기존 출력 유지 |
| 세션 변경 | Kafka `analysis.scene-session.v1` | Session, schema_version=1 | 신규 제안, 발행 미구현 |
| 위험 이벤트 | Kafka `analysis.scene-event.v1` | RiskEvent | 신규 제안, 발행 미구현 |

Kafka partition key는 household_id다. 동일 가구에는 한 번에 하나의 run/profile worker만
활성화한다. 실행 식별자는 `(household_id, run_id, profile_id)`, 입력 식별자는 여기에
`source_index`를 더한다. topic의 가구와 payload의 household_id는 발행기/bridge에서 일치시킨다.
run은 신규 실행마다 새 값이며, 재시도는 기존 run과 시작 시각을 유지한다.
다른 실제 집의 장면을 하나의 집에서 6종을 동시에 관측한 것처럼 합치지 않는다.

## 입력과 상태 규칙

- 입력은 raw P/Q/PF/I다. simulator에서 재정규화하거나 확률·정답을 주입하지 않는다.
- valid=true이면 네 feature 모두 유한한 수여야 한다. false이면 null을 허용하고 런타임에서
  norm mean으로 채워 시간 슬롯을 보존한다. context는 별도의 추론 유효성 마스크다.
- 창은 `[1,255,4]`, prefix 254초다. ready는 창 확보와 context로 결정되므로 valid/context를
  같은 값으로 강제하지 않는다. profile의 시작·종료 인덱스 및 SHA는 런타임의 잠긴 설정으로 검사한다.
- source_index와 measured_at은 매 행 1씩, 1초씩 증가한다. 가속 재생도 측정 시각은 1 Hz다.
  시각은 timezone을 포함한다. published_at과 observed_at의 크기 관계는 가속 시험 때문에 강제하지 않는다.
- 여섯 가전 순서는 KETTLE, INDUCTION, IRON, MICROWAVE, HAIR_DRYER, VACUUM_CLEANER다.
  ready인 목표 가전만 inferred=true다. 나머지는 UNKNOWN, probability=null, is_on=null이다.
- 목표 가전도 최초 MID에서 inferred=true/UNKNOWN일 수 있다. 확률과 확정 상태를 혼동하지 않는다.
- 최초/복구 확정은 SYNC다. TURNED_ON/ TURNED_OFF는 실제 확정 상태 전환만 나타낸다.
  잠긴 decoder 이후 기존 서비스가 문턱·확인 횟수를 다시 적용하지 않는다.
- runtime에는 profile, checkpoint/model-code/norm SHA, torch/device/dtype, batch/input_shape와
  참조 환경 정보를 담는다. `reference_parity_verified=false`는 실패 시 fake 대체 허용을 뜻하지 않는다.
- broker 검수에서는 source의 topic/partition/offset이 필수다. 로컬 저장 시험에서만 null을 허용한다.
- 기존 시뮬레이터의 house/power_w 등 부가 필드는 입력에서 무시한다. 네 canonical feature를
  대신하지 않는다. 문자열 숫자·문자열 boolean의 자동 변환은 허용하지 않는다.

## 세션·위험 이벤트

Session은 사용 상태의 **전체 최신 투영**이다. session_id는 같은 세션에서 유지하고 revision은
1부터 증가한다. `(session_id, revision)`마다 event_id와 payload를 고정해 재발행한다.
소비자는 동일 event_id/동일 payload를 무시하고, 충돌은 오류 처리한다. 낮은 revision은 최신
상태를 덮어쓰지 않으며, 같은 revision의 다른 payload는 오류다. 이 멱등성은 DB에서 구현한다.

| 상태 | started_at | ended_at | end_reason |
|---|---|---|---|
| 확정 ON 이후 OPEN | 관측한 ON 시각 | null | null |
| 최초 SYNC ON 이후 OPEN | null, start_known=false | null | null |
| 실제 OFF로 CLOSED | 기존 값 유지 | OFF 시각 | TURNED_OFF |
| 관측 중단 INTERRUPTED | 기존 값 유지 | null | GAP / EOF / CANCELLED |

observed_start_at은 첫 ON 관측, observed_until_at은 마지막 평가 가능 관측 시각이다.
observed_on_seconds는 샘플 개수가 아닌 관측된 ON 구간의 경과 초이며 첫 ON에서 0이다.
결측으로 중단하면 마지막 유효 관측 이후 시간을 더하지 않는다. 복구 SYNC ON은 새 session_id와
start_known=false로 시작한다. 소스가 끝났다는 이유로 물리적 OFF나 사용 완료를 만들어내지 않는다.

위험 이벤트 외곽은 기존 AnalysisEvent의 event_id/household_id/event_type/occurred_at/reason을
유지한다. reason을 구조화해 scope/run/profile/appliance/session/source_index/policy와
연속 ON 시간을 전달한다. 이번 revision에서는 테스트 가구의 PROLONGED_APPLIANCE_USE만 지원한다.
known start 이후 단절 없는 ON 경과 시간이 정책 기준 이상일 때만 발생한다. 첫 SYNC ON,
UNKNOWN 또는 INTERRUPTED 세션에서 판정하지 않는다. 검증기의 단일 이벤트만으로 연속성을
증명할 수 없으므로 생성기가 세션 상태와 원본 기록을 확인해야 한다.

10초는 합성 계약 예제와 테스트 가구 검수 정책이다. 모델 threshold 또는 운영 위험 정책을
10초로 변경하는 지시가 아니다. 최초 기준 도달 시 세션/정책당 한 이벤트를 만들고 동일 ID로
재시도한다. 가정 전체 무활동·루틴 누락·활동점수는 부분 분석으로 생성하지 않는다.

## 호환성과 예제 출처

`examples/cases.json`에 19개 정상/오류 사례, 기대 결과와 출처를 기록한다. snapshot-on과
snapshot-warmup은 기존 실제 kettle broker 기록이며 measurement-valid는 그 기록의 raw
feature를 이용한 입력 계약 예제다. **세션·위험 이벤트는 합성 데이터이며 실행 증거가 아니다.**
각 예제는 독립 사례이므로 여러 파일을 하나의 run으로 발행하지 않는다.

기존 bool 전용 analysis.snapshot.v1에 UNKNOWN을 false로 변환하지 않는다. 새로운 전용
토픽을 기존 운영 consumer가 바로 읽게 하지 않는다. backend는 부분 분석용 adapter를 구현하고
인증된 조회에 연결한다. 현존 Java scene validator보다 새 오프라인 검증기가 엄격한 항목
(runtime 식별자·전환 일관성 등)이 있으므로 후속 런타임/백엔드 작업에서 같이 적용한다.

이 명세는 feature/ai/real-model-predictor의 기존 출력에 기반하지만 해당 브랜치는 작성 시점에
develop에 미병합이다. 본 브랜치는 develop에서 독립 분기했으며 기반 구현을 병합하지 않는다.
후속 런타임 작업 전 기반 PR의 담당자 검토·통합이 필요하다.
