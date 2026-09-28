# Consumer 성능 측정 자동화 및 ClockSkew 트러블슈팅

> 작성일: 2026-09-21  
> 작성자: 최보경  
> 대상 서비스: `ai/realtime-analysis-service`  
> 환경: 로컬 Docker Compose, Kafka 24 partitions, Prometheus, Windows 호스트 시뮬레이터

## 1. 작업 목적

2026-09-20에는 Consumer를 `1 → 2 → 4 → 2 → 1 → 2`로 변경하면서
cooperative-sticky 리밸런싱과 파티션별 가구 상태 초기화를 기능적으로 검증했다.

오늘은 같은 코드로 Consumer `1개`, `2개`, `4개`의 성능을 반복 측정할 수 있도록 자동화하고
다음 항목을 비교하는 것이 목적이었다.

- 실제 입력량과 전체 처리량
- Consumer 증설에 따른 처리량 향상률과 확장 효율
- Consumer lag와 입력 중단 후 회복 시간
- E2E p95와 단계별 평균·p95 지연
- 오류 및 DLQ
- CPU와 RSS 메모리
- 파티션 할당, 리밸런싱, 가구 상태 초기화, warm-up

성능 비교는 가구 하나만 사용하는 대신 여러 `household_id`를 사용한다. 같은 가구의 메시지는
항상 같은 Kafka 파티션으로 전달되므로 `H001` 하나만 사용하면 Consumer를 늘려도 병렬 처리
효과를 확인할 수 없기 때문이다.

---

## 2. 정식 성능 비교 자동화 추가

### 2.1 실행 명령

로컬 Compose와 Prometheus가 실행 중인 상태에서 다음 명령으로 전체 비교를 자동 수행한다.

```cmd
cd /d C:\Users\SSAFY\Desktop\D201\S15P21D201\ai\realtime-analysis-service
python -m pip install -r ..\..\infrastructure\mqtt\simulator\requirements.txt
python tools\consumer_load_test.py --consumers 1,2,4 --houses 40 --hz 1 --duration 300
```

코드를 수정하지 않은 반복 측정에서는 기존 Docker 이미지를 사용할 수 있다.

```cmd
python tools\consumer_load_test.py --consumers 1,2,4 --houses 40 --hz 1 --duration 300 --skip-build
```

코드를 수정한 직후에는 새 이미지를 빌드해야 하므로 첫 실행에서 `--skip-build`를 사용하면 안
된다.

### 2.2 자동화 흐름

각 Consumer 수마다 다음 작업을 자동 수행하도록 했다.

1. Compose Consumer 수 변경
2. Prometheus에서 실행 중인 Consumer 수 확인
3. 전체 할당 파티션 합계 `24` 확인
4. 테스트 시작 전 lag `0` 확인
5. 다중 가구 random 시뮬레이터 실행
6. `가구 수 × 299개` 처리 및 warm-up `0` 대기
7. 안정 구간에서 처리량, lag, 지연, 오류, CPU, 메모리 수집
8. 시뮬레이터 종료 후 lag가 `0`이 되는 시간 측정
9. 다음 Consumer 수로 전환
10. 성공 또는 실패 후 Consumer 1개로 복구

### 2.3 생성 결과

측정 결과는 다음 경로에 실행 시각별로 저장한다.

```text
ai/realtime-analysis-service/load-test-results/<실행시각>/
```

| 파일 | 내용 |
| --- | --- |
| `report.md` | 사람이 읽기 쉬운 요약과 단계별 p95 비교 |
| `summary.csv` | 스프레드시트용 핵심 지표 |
| `results.json` | 전체 원본 측정 결과 |
| `metadata.json` | 입력량, 측정 시간, Consumer 설정 |
| `simulator-consumers-*.log` | Consumer 수별 시뮬레이터 로그 |

관련 커밋:

```text
cb82101 fix(ai): consumer 테스트 수정
```

---

## 3. 40가구 1Hz 기준 성능 결과

기준 입력은 다음과 같다.

```text
40가구 × 가구당 1Hz = 약 40 msg/s
```

`20260921-121844` 결과의 주요 값은 다음과 같다.

| Consumers | Input | Throughput | Max lag | Lag recovery | E2E p95 | CPU | RSS avg |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 40.11 msg/s | 29.73 msg/s | 3,249 | 118.18s | 109.56s | 0.56 cores | 91.84 MiB |
| 2 | 39.91 msg/s | 39.91 msg/s | 36 | 13.03s | 920.29ms | 0.70 cores | 176.46 MiB |
| 4 | 39.76 msg/s | 39.76 msg/s | 32 | 13.03s | 679.58ms | 0.83 cores | 346.71 MiB |

### 3.1 해석

Consumer 1개는 약 `40 msg/s` 입력보다 처리량이 낮아 lag가 누적됐다. Kafka backlog가 E2E에
포함되면서 p95도 약 109초까지 증가했다.

Consumer 2개부터는 입력량을 모두 처리해 lag가 거의 누적되지 않았고 E2E p95가 1초 아래로
감소했다. Consumer 4개는 2개보다 E2E를 조금 더 낮췄지만 전체 처리량은 증가하지 않았다.
이미 Consumer 2개가 입력 `40 msg/s`를 모두 처리하고 있어 추가로 처리할 메시지가 없기
때문이다.

따라서 `40 msg/s` 기준에서는 Consumer 2개가 비용 대비 적절하다. Consumer 4개는 처리량
증가 없이 RSS 메모리가 약 2배로 증가한다.

### 3.2 Speedup과 Efficiency 해석

자동화는 첫 번째 측정 결과를 기준으로 Speedup을 계산한다.

```text
Speedup = 현재 처리량 / 기준 처리량
Efficiency = Speedup / Consumer 증가 비율 × 100
```

Consumer `2,4`만 측정한 경우 2개가 기준 `1.00x`가 된다. 4개로 두 배 증설했지만 입력량이
그대로라 처리량도 약 `1.00x`이고 Efficiency는 약 `50%`가 된다. 이는 코드가 절반 속도로
동작한다는 의미가 아니라 현재 입력량에서 절반의 증설 여유가 사용되지 않는다는 의미다.

---

## 4. 문제: Consumer 2개와 4개에서 ClockSkew WARN 발생

### 4.1 증상

`20260921-121844` 결과에서 기능 처리와 DLQ는 정상이었지만 다음 오류 때문에 판정이 WARN으로
나왔다.

```text
Consumer 2: e2e:ClockSkew = 98
Consumer 4: e2e:ClockSkew = 159
```

기존 E2E 기록 로직은 다음 조건을 사용했다.

```python
if duration_seconds < 0:
    record_error("e2e", "ClockSkew")
```

### 4.2 원인

E2E는 다음 두 시각의 차이로 계산한다.

```text
published_at - observed_at
```

- `observed_at`: Windows 호스트에서 실행되는 시뮬레이터가 생성
- `published_at`: Docker 컨테이너의 분석 서비스가 생성

Producer와 Consumer가 서로 다른 시계를 사용하기 때문에 Windows와 Docker VM 사이에 수십
ms 수준의 차이가 있으면 처리 속도가 빠른 메시지에서 음수 E2E가 발생할 수 있다.

Consumer 1개에서는 backlog가 커서 실제 대기시간이 시계 차이보다 훨씬 크므로 문제가 드러나지
않았다. Consumer 2개와 4개에서는 lag가 거의 없고 메시지를 즉시 처리하면서 미세한 시계 차이가
관측됐다.

이는 Kafka 처리 실패나 DLQ 문제가 아니라 로컬 분산 환경의 wall clock 측정 오차였다.

---

## 5. ClockSkew 허용 오차 적용

### 5.1 설정 추가

기본 100ms 허용 오차를 추가하고 환경변수로 변경할 수 있게 했다.

```env
ANALYSIS_E2E_CLOCK_SKEW_TOLERANCE_SECONDS=0.1
```

설정 허용 범위는 `0~60초`다. `0`을 지정하면 기존처럼 모든 음수 E2E를 ClockSkew로 처리한다.

### 5.2 처리 규칙

수정 후 동작은 다음과 같다.

```text
duration < -100ms  -> ClockSkew 오류 기록, E2E histogram에는 기록하지 않음
-100ms <= duration < 0 -> 로컬 시계 오차로 보고 0초 기록
duration >= 0 -> 실제 E2E 값 기록
```

핵심 로직은 다음과 같다.

```python
if duration_seconds < -self._e2e_clock_skew_tolerance_seconds:
    self.record_error("e2e", "ClockSkew")
    return
self.e2e_duration.observe(max(duration_seconds, 0.0))
```

다음 파일에 설정을 반영했다.

- `src/realtime_analysis/config.py`
- `src/realtime_analysis/metrics.py`
- `src/realtime_analysis/__main__.py`
- 서비스 `.env.example`
- 로컬 Compose `.env.example`
- `infrastructure/local/compose.yaml`
- 서비스 README

### 5.3 테스트

다음 항목을 검증했다.

- 기본 허용값이 `0.1초`인지 확인
- 환경변수로 `0.25초` 등 다른 값 로딩
- 허용 범위 안의 음수 E2E를 0초로 기록
- 허용 범위를 초과한 음수만 ClockSkew로 기록
- 음수 허용값 거절

전체 테스트 결과:

```text
140 passed, 8 skipped
```

관련 커밋:

```text
425806c fix(ai): clockskew 오류 수정
```

---

## 6. ClockSkew 수정 후 재검증

새 Docker 이미지를 빌드하기 위해 첫 재검증에서는 `--skip-build`를 제외했다.

```cmd
python tools\consumer_load_test.py --consumers 2,4 --houses 40 --hz 1 --duration 120
```

`20260921-132835` 결과는 다음과 같다.

| Consumers | Result | Input | Throughput | Max lag | Recovery | E2E p95 | Errors | DLQ |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2 | PASS | 40.19 msg/s | 40.10 msg/s | 49 | 13.03s | 998.00ms | 0 | 0 |
| 4 | PASS | 40.30 msg/s | 40.12 msg/s | 31 | 13.02s | 797.98ms | 0 | 0 |

수정 전 2개에서 98건, 4개에서 159건 발생하던 ClockSkew가 모두 0이 됐다. 처리 실패와 DLQ도
0이며 두 조건 모두 PASS가 나와 허용 오차가 의도대로 적용된 것을 확인했다.

보고서의 오류 상세에 다음 값이 표시되는 경우가 있다.

```text
Consumer 4: e2e:ClockSkew=0
```

이는 Prometheus에 값이 0인 시계열이 존재하여 보고서가 이를 출력한 표시 문제다. 전체 오류
합계가 0이고 판정이 PASS이므로 실제 오류는 아니다. 추후 보고서 생성 시 값이 0인 오류 항목을
제외할 수 있다.

---

## 7. consumer_poll p95가 4개에서 증가한 이유

재검증 결과의 `consumer_poll` p95는 다음과 같았다.

```text
Consumer 2: 0.18ms
Consumer 4: 633.23ms
```

`consumer_poll`은 메시지 처리시간이 아니라 다음 Kafka 메시지를 기다리는 시간이다.

```python
message = consumer.poll(timeout=1.0)
```

시뮬레이터는 40가구 데이터를 매초 한꺼번에 발행한다. Consumer 4개에서는 한 Consumer가 약
10건을 빠르게 처리한 뒤 다음 1초 주기의 메시지가 올 때까지 `poll()`에서 기다린다. 따라서
Consumer 수가 많고 입력량이 부족할수록 poll 대기 p95가 커질 수 있다.

이번 결과에서는 다음 값이 정상이므로 병목이 아니다.

- 처리량이 입력량과 동일
- lag가 작고 최종적으로 0 회복
- E2E p95가 2개보다 감소
- 오류와 DLQ 0

`consumer_poll`은 처리 단계 병목보다는 Kafka 메시지 대기 또는 Consumer 유휴 시간으로
해석해야 한다. `consumer_poll` 증가와 함께 lag 증가, 처리량 감소, E2E 증가가 동시에 나타날
때만 Kafka 또는 Consumer 문제를 의심한다.

---

## 8. 리밸런싱과 warm-up 지표 해석

`20260921-132835`의 리밸런싱 결과는 다음과 같다.

| Consumers | Assignment stable | Warm-up | Peak households | Resets | Partitions | assign/revoke/lost |
| ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 2 | 20.06s | 320.69s | 40 | 0 | 24 | 2/0/0 |
| 4 | 20.03s | 320.66s | 40 | 18 | 24 | 8/2/0 |

### 8.1 Assignment stable

Compose 스케일 변경 후 Prometheus에서 요청한 Consumer 수와 파티션 합계 24를 관측할 때까지의
시간이다. 컨테이너 시작, Kafka 그룹 참가, 파티션 할당, Prometheus scrape와 자동화 polling이
모두 포함되므로 순수 Kafka 리밸런싱 시간은 아니다.

### 8.2 Warm-up

가구별 299개 버퍼가 준비되고 warm-up 가구 수가 0이 될 때까지의 시간이다.

```text
299개 / 가구당 1Hz = 이론상 약 299초
```

실제 약 320초에는 시작 지연과 Prometheus 수집 주기가 포함된다. Consumer 2개와 4개의 시간이
같은 이유는 처리량이 아니라 가구별 1Hz 생성 속도가 warm-up 시간을 제한하기 때문이다.

### 8.3 Resets

2개에서 4개로 증설할 때 기존 Consumer가 반납한 파티션에 속한 가구 18개의 휘발성 상태가
초기화됐다. 전체 40가구가 아니라 이동 파티션 관련 가구만 초기화됐고 `lost=0`이므로
cooperative-sticky 선택 초기화가 정상 동작했다.

### 8.4 assign/revoke/lost

이 값은 파티션 수가 아니라 콜백 호출 횟수다. cooperative 방식은 파티션을 단계적으로 이동해
한 Consumer에서 assign 콜백이 여러 번 발생하거나 빈 목록으로 호출될 수 있다.

```text
2개: 최초 assign 2회, revoke 0, lost 0
4개: 단계적 assign 8회, 기존 Consumer revoke 2회, lost 0
```

### 8.5 Peak households 주의사항

자동 성능 비교는 각 Consumer 조건 사이에 시뮬레이터를 중단했다가 다시 시작한다. 입력 공백이
데이터 품질 gap 기준을 넘으면 다음 입력에서 기존 가구 버퍼도 초기화될 수 있다. 따라서
`Peak households=40`만으로 이동한 가구만 warm-up에 들어갔는지는 판단할 수 없다.

이동한 가구만 warm-up되는 기능은 2026-09-20 문서처럼 시뮬레이터를 계속 실행한 채
`2 → 4`로 증설하고 revoke 로그와 reset 수를 비교하는 방식으로 검증한다. 성능 자동화의
`Resets=18`은 파티션 revoke에 따른 선택 초기화를 보여주지만, Peak households에는 테스트 사이
입력 공백의 영향이 포함될 수 있다.

---

## 9. 80가구 부하 시험 시작

40가구 1Hz에서는 Consumer 2개가 이미 전체 입력을 처리하므로 Consumer 4개의 처리량 확장 효과를
확인할 수 없었다. 실제 가구당 1Hz 조건을 유지하고 가구 수를 80으로 늘렸다.

```cmd
python tools\consumer_load_test.py --consumers 1,2,4 --houses 80 --hz 1 --duration 180 --skip-build
```

입력 목표는 다음과 같다.

```text
80가구 × 1Hz = 약 80 msg/s
```

문서 작성 시점의 `20260921-135613` 결과에는 Consumer 1개 측정만 완료되어 있다.

| Consumers | Input | Throughput | Max lag | Lag recovery | E2E p95 | Warm-up |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 79.59 msg/s | 20.71 msg/s | 29,887 | 1,425.63s | 300,000ms | 545.97s |

Consumer 1개는 `80 msg/s` 입력을 따라가지 못해 대규모 backlog가 발생했다. E2E p95
`300,000ms`는 현재 Prometheus histogram의 최상위 300초 bucket에 도달한 값이므로 정확한
p95라기보다 최소 300초 수준의 큰 지연으로 해석해야 한다.

lag 회복에 약 24분이 걸려 고부하에서 낮은 Consumer 수를 함께 측정하면 전체 시험 시간이 매우
길어진다는 사실도 확인했다. Consumer 2개와 4개 결과가 완료된 뒤 다음을 비교한다.

- 2개가 `80 msg/s`를 처리할 수 있는지
- 4개에서 처리량이 증가하고 lag가 안정되는지
- DB 단계 p95가 Consumer 수 증가에 따라 악화되는지
- 오류와 DLQ가 계속 0인지
- RSS 증가 대비 E2E 개선 효과가 있는지

---

## 10. 현재 결론

오늘 확인한 내용은 다음과 같다.

```text
40 msg/s, Consumer 1개: 처리량 부족, lag와 E2E 급증
40 msg/s, Consumer 2개: 입력 전체 처리, E2E 약 1초
40 msg/s, Consumer 4개: 처리량 이득 없음, E2E 소폭 개선, 메모리 약 2배

ClockSkew 수정 전: Consumer 2/4에서 98/159건 WARN
100ms 허용 오차 적용 후: Consumer 2/4 모두 Errors 0, DLQ 0, PASS

파티션 합계: 24 유지
리밸런싱 lost: 0
2 -> 4 증설 시 파티션 관련 가구 reset: 18
```

`40 msg/s` 기준 최적 Consumer 수는 현재 2개다. 다만 최적 개수는 고정값이 아니며 실제 입력량,
Consumer 하나의 처리 능력, DB 처리량, CPU·메모리, 목표 E2E에 따라 달라진다. Kafka 파티션이
24개이므로 같은 Consumer Group에서 동시에 유효하게 동작할 수 있는 Consumer의 논리적 상한은
24개지만, 실제 최적값은 그보다 훨씬 작을 수 있다.

---

## 11. 남은 작업

1. 진행 중인 80가구 1Hz의 Consumer 2개와 4개 결과 확인
2. 80 msg/s에서 처리량, lag, E2E, CPU, RSS, DB p95 비교
3. 4개에서도 입력량이 부족하면 120가구는 Consumer 2개와 4개만 비교
4. 보고서 오류 상세에서 값이 0인 시계열 숨기기
5. `consumer_poll`을 처리 단계 병목과 분리해 유휴 대기시간으로 표시하는 방안 검토
6. 성능 자동화의 Peak households와 연속 리밸런싱 warm-up 검증 목적 분리

고부하 결과를 해석할 때는 처리량만 보지 않고 다음 조건을 함께 확인한다.

```text
Throughput가 Observed input을 따라가는가
lag가 지속 증가하지 않는가
입력 중단 후 lag가 제한 시간 안에 0으로 회복하는가
E2E p95가 목표 범위인가
Errors와 DLQ가 0인가
DB p95와 메모리가 Consumer 증가에 따라 과도하게 증가하지 않는가
```

