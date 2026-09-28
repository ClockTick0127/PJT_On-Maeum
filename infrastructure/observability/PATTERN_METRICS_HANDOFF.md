# AI 패턴 감지 및 일일 작업 메트릭

현재 AI `src/realtime_analysis/metrics.py`, `anomaly_detector.py`, `handler.py`, `daily_activity_index.py`, `routine_change_detector.py` 구현을 기준으로 정리한 관측 설정입니다. 기존 `ai-analysis` job과 `/metrics`를 그대로 사용합니다. 추가 exporter나 AI 코드 변경은 필요하지 않습니다. 실행 중인 AI 이미지에는 해당 메트릭 구현이 포함되어 있어야 합니다.

## 메트릭과 라벨

| 메트릭 | 종류 | Prometheus에서 조회할 라벨 |
| --- | --- | --- |
| `nilm_pattern_detection_duration_seconds` | Histogram | `pattern` |
| `nilm_pattern_detection_total` | Counter | `pattern`, `result` |
| `nilm_pattern_events_total` | Counter | `event_type` |
| `nilm_daily_job_duration_seconds` | Histogram | `exported_job` |
| `nilm_daily_job_runs_total` | Counter | `exported_job`, `status` |

- `pattern`: `ROUTINE_MISSED`, `PROLONGED_INACTIVITY`, `PROLONGED_APPLIANCE_USE`, `ROUTINE_CHANGED`.
- `result`: `detected`, `not_detected`, `error`, `skipped`.
- `event_type`: Kafka 발행이 완료된 패턴 이벤트 유형.
- `exported_job`: `activity_index`, `routine_changed`, `baseline_update`.
- `status`: `success`, `error`.
- Histogram은 `_bucket`, `_sum`, `_count` 시계열로 조회합니다. 모든 시계열에 수집 대상의 `job="ai-analysis"`, `instance` 라벨이 붙습니다.

`honor_labels: false`를 명시하여 AI가 보낸 애플리케이션 `job`은 `exported_job`으로 저장합니다. `honor_labels: true`로 바꾸면 이 문서와 대시보드의 선택 조건이 깨집니다.

## 의미와 데이터 부재

실시간 `detected`는 cooldown·중복 제거 전 후보가 나온 평가 횟수이며, 이벤트 개수와 같지 않습니다. 최종 Kafka 발행 성공은 `nilm_pattern_events_total`로 봅니다. 단, 동일 가구·가전·날짜의 `ROUTINE_MISSED`가 이미 발행된 뒤에는 해당 후보를 다음 평가에서 조기에 제외하므로 반복 `detected`가 증가하지 않습니다. 다른 가전과 다음 날짜 후보는 계속 평가합니다. 일일 `ROUTINE_CHANGED`는 실행 전체당 결과 1건을 기록하므로 가구별 이벤트 건수와도 다릅니다. 현재 일일 구현은 발행/중복 제거를 수행한 결과로 detected 여부를 정합니다.

실시간 패턴은 공통 평가 주기와 데이터 유효성 조건을 통과한 호출만 계측합니다. `ROUTINE_CHANGED` 정책이 없으면 패턴 결과는 skipped지만 일일 작업은 success일 수 있습니다. 일일 작업 duration은 성공과 실패 모두 포함하며 완료/예외 이후 기록됩니다. 실행 중 경과 시간, 마지막 성공 시각, 실행 예정 시각 메트릭은 없습니다.

고정 라벨 Counter는 프로세스 시작 시 가능한 라벨 조합을 0으로 생성합니다. 따라서 Prometheus가 첫 증가 전에 한 번 이상 수집하면 최초 패턴 판정·이벤트도 `rate`와 `increase`에 반영됩니다. Histogram과 동적 오류 라벨은 최초 관측 전에는 시계열이 없을 수 있으므로 No data와 수집 실패를 구별해야 합니다. 수집 상태는 `up{job="ai-analysis"}`로 확인합니다.

Counter는 프로세스 재시작 시 초기화됩니다. `increase`는 수집 샘플 사이 증가량의 보간 추정치입니다. 서버 시작 직후 첫 scrape보다 먼저 끝나는 일일 작업처럼 0 상태를 Prometheus가 수집하지 못한 경우에는 최초 증가가 집계에서 빠질 수 있습니다. 이 때문에 대시보드는 조회 기간 증가량과 프로세스 누적값을 함께 표시합니다. 프로세스 누적값은 조회 기간 총계나 영구 누계가 아닙니다.

## 대시보드와 PromQL

Grafana `NILM Observability` → `NILM · AI 패턴 감지 및 일일 작업` (`nilm-ai-patterns`). 기본 기간은 일일 작업을 보기 위한 최근 48시간입니다. AI instance 선택기로 범위를 제한할 수 있습니다.

패턴 결과별 초당 판정:

```promql
sum by(pattern, result) (
  rate(nilm_pattern_detection_total{job="ai-analysis",instance=~"$instance"}[$__rate_interval])
)
```

패턴 알고리즘 지연 p95:

```promql
histogram_quantile(0.95, sum by(le, pattern) (
  rate(nilm_pattern_detection_duration_seconds_bucket{job="ai-analysis",instance=~"$instance"}[$__rate_interval])
))
```

패턴 평균 지연:

```promql
sum by(pattern) (rate(nilm_pattern_detection_duration_seconds_sum{job="ai-analysis",instance=~"$instance"}[$__rate_interval]))
/
sum by(pattern) (rate(nilm_pattern_detection_duration_seconds_count{job="ai-analysis",instance=~"$instance"}[$__rate_interval]))
```

최종 Kafka 발행 성공 누적값:

```promql
sum by(event_type) (nilm_pattern_events_total{job="ai-analysis",instance=~"$instance"})
```

일일 작업 조회 기간 실행 횟수:

```promql
sum by(exported_job, status) (
  increase(nilm_daily_job_runs_total{job="ai-analysis",instance=~"$instance"}[$__range])
)
```

일일 작업 조회 기간 p95 / 평균:

```promql
histogram_quantile(0.95, sum by(le, exported_job) (
  increase(nilm_daily_job_duration_seconds_bucket{job="ai-analysis",instance=~"$instance"}[$__range])
))

sum by(exported_job) (increase(nilm_daily_job_duration_seconds_sum{job="ai-analysis",instance=~"$instance"}[$__range]))
/
sum by(exported_job) (increase(nilm_daily_job_duration_seconds_count{job="ai-analysis",instance=~"$instance"}[$__range]))
```

일일 작업 최초 실행도 보이는 프로세스 누적값 / 평균:

```promql
sum by(exported_job, status) (nilm_daily_job_runs_total{job="ai-analysis",instance=~"$instance"})

sum by(exported_job) (nilm_daily_job_duration_seconds_sum{job="ai-analysis",instance=~"$instance"})
/
sum by(exported_job) (nilm_daily_job_duration_seconds_count{job="ai-analysis",instance=~"$instance"})
```

Grafana 전용 변수 `$instance`, `$__range`, `$__rate_interval`을 Prometheus 화면에서 직접 사용할 때는 각각 `.*`, `2d`, `1m` 등 실제 값으로 바꿉니다. 일일 작업은 짧은 rate 창 대신 전체 조회 기간으로 집계하며 관측 없는 평균/분위수는 비어 있을 수 있습니다.

## 1초 갱신 설정

- Grafana 최소 갱신 제한: `GF_DASHBOARDS_MIN_REFRESH_INTERVAL=1s`.
- 모든 제공 대시보드: `refresh=1s`, 갱신 메뉴에 1초 추가.
- AI scrape: `scrape_interval=1s`, `scrape_timeout=1s`.
- 기존 AI 대시보드와 신규 패턴 대시보드: 모든 Prometheus 쿼리의 Min step(`interval`)을 1초로 지정. 이 값이 `$__rate_interval` 계산에 사용됩니다.
- 공통 데이터소스는 다른 job과 일치하도록 `timeInterval=15s` 유지. HTTP/TCP probe는 5초 timeout을 보존하며 15초마다 수집. cAdvisor도 기존 15초 수집/내부 갱신 유지. 이 화면들은 1초마다 조회하지만 원본 데이터는 15초마다 바뀝니다.
- AI Consumer Lag은 AI 내부 계산 주기가 기본 5초이므로 1초마다 새 값이 생기지는 않습니다. 일일 작업도 실제 실행 시에만 바뀝니다. 이 주기를 바꾸는 AI 수정은 범위 밖입니다.
- AI 요청 응답이 1초를 넘으면 수집 실패가 됩니다. 샘플 수는 기존 15초 대비 약 15배 증가하며, 기존 5GB 보존 한도 때문에 실제 보존 기간이 짧아질 수 있습니다. 긴 조회 기간에서는 Grafana가 그래프 해상도를 자동 조정하므로 매초 점을 모두 그린다는 의미는 아닙니다.

## 적용

`infrastructure/observability`에서 기존 `.env`를 유지하여 실행합니다. 파일이 없으면 로컬은 `.env.local.example`, EC2-A는 `.env.ec2-a.example`을 복사하고 환경에 맞게 설정합니다.

```powershell
docker compose --env-file .env -f compose.yaml up -d prometheus blackbox-exporter grafana
docker compose --env-file .env -f compose.yaml restart prometheus grafana
```

첫 명령은 Grafana 환경변수를 반영하여 필요 시 재생성하고, 두 번째는 기존 컨테이너의 Prometheus 설정 및 Grafana provisioning도 확실히 다시 읽게 합니다. cAdvisor 추가 구성을 사용 중이면 두 명령 모두 `-f compose.resources.yaml`을 추가합니다. 열린 브라우저는 새로고침하고 1초 갱신을 선택합니다.

관측 컨테이너의 재빌드는 필요하지 않습니다. AI 구버전 이미지에 메트릭이 없다면 이 폴더만 수정해서 만들 수 없으므로 AI 이미지 배포 담당자가 최신 이미지를 배포해야 합니다.

## 공식 문서

- [Grafana 최소 갱신 간격](https://grafana.com/docs/grafana/latest/setup-grafana/configure-grafana/#min_refresh_interval)
- [Grafana Prometheus 데이터소스 간격](https://grafana.com/docs/grafana/latest/datasources/prometheus/configure/)
- [Prometheus scrape 간격과 라벨 충돌](https://prometheus.io/docs/prometheus/latest/configuration/configuration/)
