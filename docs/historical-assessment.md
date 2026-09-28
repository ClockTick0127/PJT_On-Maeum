# 과거 위험 평가 실행

이 기능은 저장된 `analysis.snapshot.v1`을 측정 시각 순서로 재생해 기존 `RiskAssessor`로 과거 점수를 계산한다. Spring 서버, DB, Kafka, 알림 서비스는 시작하지 않는다. 결과는 운영 `risk_assessments` 테이블과 분리된 파일이며 기존 보고서 입력으로 변환할 수 있다.

## 의미와 범위

- `EVENT_TIME_REASSESSMENT`: 선택한 정책으로 과거 관측을 재평가한다. 실제 수신 지연, 당시 화면의 히스테리시스, 알림 재발송 이력을 복원한 결과는 아니다.
- 활동 감소(A) 참고 점수와 평가 가능 여부를 계산한다. `risk_level`은 언제나 null이다(`monitoring-score-v2-A`). 무활동·장시간 사용의 이벤트 등급과 최종 화면 등급은 분석 서비스 이벤트 경로다. 루틴 미사용은 판단에 쓰지 않는다. v1 결과(`monitoring-score-v1-MIA`)는 M/I/A 최대값과 등급을 담고 있어 v2 결과와 섞어 비교하지 않는다.
- 관측 커버리지와 사용 구간은 기존 `HouseholdObservation`, `ApplianceUsageEpisode`, `ValidUseContract`로 재구성한다. 짧은 사용 제외, 인접 사용 병합, 처음부터 ON인 사용의 시작 추정 표시를 재사용한다.
- 성공 receipt만으로 snapshot 발행 성공을 증명할 수 없다. raw나 최종 세션만을 snapshot으로 가장하지 않는다. 과거 snapshot이 보관돼 있지 않다면 테스트 분석 재생 시 snapshot을 함께 수집해야 한다. 이 도구는 raw 모델 재추론/시나리오 생성기가 아니다.
- 파일/상태는 가구별로 분리한다. snapshot은 한 줄당 JSON 하나, `observed_at` 오름차순이어야 한다. 동일한 인접 재전송은 허용하지만 다른 내용의 동일 시각 또는 역순은 실패한다.
- 처음 며칠 기준이 없으면 LEARNING, 관측 품질이 부족하면 해당 평가 상태를 그대로 보존한다. null 점수를 0점으로 바꾸지 않는다.

## 입력 준비

1. **snapshots.jsonl**: 실제 분석 snapshot 메시지. 대상 시작일 이전 warm-up과 마지막 유효 활동을 포함한다. 측정 timestamp를 현재 시각으로 변경하지 않는다.
2. **profiles.json**: Gold `gold.household-profile.v1` 메시지의 JSON 배열. ACTIVE/READY만 선택한다. `as_of_date < 평가일(KST)`, `effective_from <= 평가 시각`, `published_at <= 평가 시각`을 모두 요구한다. 같은 날짜에서는 큰 revision을 선택한다. 이후 변경 때문에 SUPERSEDED가 된 프로필도 원래 발행 메시지로 보존하면 과거 평가에 쓸 수 있다.
3. **policy.json**: `RiskProperties`의 camelCase 필드. Duration은 `PT15M` 같은 ISO 표기다. 생략 필드는 현재 코드 기본값을 사용하며 최종 적용 정책 전체를 결과 manifest에 기록한다. 실제 사용 정책과 버전을 명시하는 것을 권장한다.
4. **config.json**: 아래 형식. 상대 경로는 config 파일 디렉터리를 기준으로 해석한다. start 포함/end 제외이며 기본 예시는 1분마다 계산한다. `awayPeriods: []`는 외출이 없다는 명시적 테스트 가정이다. 외출 이력을 모르는 실제 가구에 자동 적용하지 않는다.

```json
{
  "householdId": "H001",
  "start": "2026-09-01T00:00:00+09:00",
  "end": "2026-09-24T00:00:00+09:00",
  "stepSeconds": 60,
  "snapshots": "snapshots.jsonl",
  "profiles": "profiles.json",
  "policy": "policy.json",
  "awayPeriods": [
    {"start": "2026-09-10T09:00:00+09:00", "end": "2026-09-10T18:00:00+09:00"}
  ]
}
```

```json
{
  "policyVersion": "demo-backfill-v1",
  "observationGapThreshold": "PT2M",
  "profileMaxAge": "P3D",
  "observationMaxAge": "PT15M"
}
```

주의: 오늘 생성한 Gold 메시지의 `published_at`은 오늘이다. 그대로 과거에 적용하지 않는다. 실제 당시 생성된 버전을 쓰거나, 테스트용 기준을 날짜순으로 만들고 가상 적용 시각을 명시한 별도 테스트 데이터로 준비한다. 실제 이력의 시간 필드를 몰래 수정해 당시 결과라고 부르지 않는다.

## 실행

`backend/monitoring-service`에서 실행한다(Java 21).

```sh
./gradlew historicalAssessment --args='/data/backfill/config.json /data/backfill/result-001'
```

Windows에서는 `gradlew.bat`을 쓴다. 결과 디렉터리의 부모는 존재해야 하고 결과 디렉터리 자체는 새 경로여야 한다. 데이터는 스트리밍으로 읽으며 raw 수백만 건을 메모리에 올리지 않는다. 프로필과 누적 논리 사용 구간은 메모리에 유지하므로 가구 단위로 실행한다.

출력:

- `assessments.jsonl`: 보고서 필수 컬럼, 근거, confidence, run ID, 재평가 모드.
- `manifest.json`: 완료 증빙, 입력별 SHA-256, 출력 SHA-256, 기록 수, 실제 생성 시각, 적용 정책.

동일 입력/설정/구현 버전은 동일 run ID와 assessment ID를 만든다. 기존 디렉터리를 덮어쓰지 않는다. 실패한 실행은 완료 manifest가 없으므로 전달 완료로 인정하지 않는다. 입력 전체 정렬/무결성 검증이 끝난 뒤 완료 manifest를 쓴다. `assessment_cutoff`는 end이며 평가 시각은 항상 그보다 작다.

## 보고서 연결

Gold 서비스 런타임에서 실행한다. Java 결과 디렉터리를 컨테이너에 읽기 전용 마운트하고 로컬 Spark 실행 또는 모든 executor가 해당 경로에 접근 가능한 환경을 사용한다.

```sh
household-report import-assessments \
  --input-manifest /data/backfill/result-001/manifest.json \
  --output-base /nilm/gold/historical_assessments \
  --report-input /nilm/report-input/draft.json \
  --report-output /nilm/report-input/with-history.json

household-report build --input-manifest /nilm/report-input/with-history.json
household-report publish --manifest <build가 출력한 manifest_path>
```

import는 checksum·중복 ID·가구/run ID·cutoff·행 수를 확인하고 Parquet과 lake manifest를 확정한다. 같은 run 경로는 덮어쓰지 않는다. 이미 가져온 결과는 기존 확정 manifest의 `sources.assessments`를 재사용한다.

`--report-input`/`--report-output`은 함께 생략할 수 있다. 지정하면 기존 보고서 문서를 복사하고 assessments 소스와 cutoff/전달완료/재평가 모드만 연결한다. usage/baseline/statistics/targets와 Gold 버전은 기존 보고서 작성자가 준비해야 한다. 평가 기록이 여러 날짜의 프로필을 사용한 것과 보고서의 대표 Gold 버전은 별개다.

보고서 최대 위험 점수는 **설정한 평가 간격에서 기록한 점수의 최대값**이며 하루 중 모든 순간의 최대값을 보장하지 않는다. 실제 임계 진입과 해제를 검증하려면 간격을 줄이거나 별도 전체 이벤트 재현 테스트를 추가한다.

## 실행 가능한 작은 예제

`examples/historical-assessment/`는 baseline이 없는 가구의 짧은 snapshot 예제다. LEARNING/null 결과가 보고서까지 보존되는지 확인한다. 정상/위험 시나리오 성능 검증 자료는 아니다.

```sh
./gradlew historicalAssessment --args='../../examples/historical-assessment/config.json /data/smoke-new'
```

## 검증

```sh
./gradlew test --tests '*HistoricalAssessmentTest' --tests '*RiskAssessorTest'
```

`batch/gold_profile_service`에서:

```sh
pytest tests/test_historical_assessments.py
```

Parquet 통합 테스트는 PySpark와 사용 가능한 Hadoop 파일시스템 런타임을 요구한다.
