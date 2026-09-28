# Grafana 일별 보고서 운영

구현 범위는 일별 보고서 데이터 자동 생성·게시와 Grafana 화면 조회입니다. PDF 생성·메일 발송은 포함하지 않습니다. 운영자용이며 가구 선택 변수는 접근 권한 경계가 아닙니다.

## 데이터 경로

`run-gold-daily.sh → Gold daily 완료(0/12) → household-report daily → 활성 lake 버전 고정 → risk_assessments DB 스냅샷 추출 → Spark 보고서 생성 → reporting 테이블 원자적 게시 → Grafana`

- usage: `appliance_usage_daily`의 날짜별 활성 버전만 사용합니다. raw 품질과 세션 사용량은 기존 Silver 일별 집계에서 결합됩니다. Grafana에서 원시 1초 데이터를 다시 집계하지 않습니다.
- baseline/statistics: 보고일의 `routine_baseline_history`, `household_statistical_profile`이 같은 Gold 실행인지 확인합니다.
- assessments: monitoring DB의 `risk_assessments`를 KST 기간으로 읽습니다. REPEATABLE READ 스냅샷이며 `LIVE_RECORDED`로 표시합니다. 아직 DB에 도착하지 않은 이벤트까지 완료됐다는 의미는 아닙니다.
- 과거 평가: `import-assessments`가 생성한 lake manifest를 `--historical-manifest`로 전달합니다. 화면에는 `EVENT_TIME_REASSESSMENT`로 표시되며 알림은 발송하지 않습니다.
- 원본·집계·평가 내용이 같으면 같은 report_id입니다. 지연 기록이나 활성 입력 변경은 새 버전을 생성합니다. 실패한 DB 게시 트랜잭션은 전체 롤백합니다.
- 보고일 usage와 Gold는 필수입니다. 이전 기간의 누락 날짜는 manifest와 화면에 남기며 위험 점수·사용량을 0으로 보정하지 않습니다. 7일 평균은 7일 모두 유효할 때만 표시합니다. 날짜별 막대그래프를 사용해 과거 보고서가 Grafana 전역 시간 범위에 잘리지 않도록 했습니다.
- baseline과 statistics는 보고일 기준 프로필입니다. 과거 날짜별 기준선으로 오인하지 않도록 현재 대시보드에서는 이를 과거 기준선 곡선으로 그리지 않습니다.

## 1. DB 초기화

배포 전 관리자 계정으로 **monitoring_db**에 `reporting_schema.sql`을 실행합니다. 기존 reporting 테이블을 유지하면서 조회 뷰를 추가합니다.

```sh
psql "$MONITORING_ADMIN_DSN" -v ON_ERROR_STOP=1 -f batch/gold_profile_service/reporting_schema.sql
```

배치용 계정에는 risk_assessments SELECT, reporting 스키마 USAGE와 report_runs/report_rows SELECT·INSERT를 부여합니다. Grafana용 별도 계정 `grafana_reporting`을 만들고 비밀번호를 배포 비밀 설정으로 관리합니다. 다음은 해당 계정 생성 후 적용할 권한입니다.

```sql
GRANT CONNECT ON DATABASE monitoring_db TO grafana_reporting;
GRANT USAGE ON SCHEMA reporting TO grafana_reporting;
GRANT SELECT ON reporting.report_runs, reporting.v_report_index,
    reporting.v_household_daily, reporting.v_appliance_daily,
    reporting.v_report_summary, reporting.v_report_evidence TO grafana_reporting;
ALTER ROLE grafana_reporting SET statement_timeout = '15s';
ALTER ROLE grafana_reporting SET default_transaction_read_only = on;
```

이 계정은 다른 애플리케이션 역할을 상속하거나 쓰기 권한을 가져서는 안 됩니다. 대시보드는 가구 전체에 접근하는 내부 운영자 기준입니다.

## 2. 환경 설정

EC2-B Compose의 `.env`에 다음 DSN을 설정합니다. URL의 비밀번호 특수문자는 URL 인코딩합니다. `DATABASE_*`는 기존 analysis_db 카탈로그 연결로 유지합니다.

```dotenv
REPORT_DATABASE_URL=postgresql+psycopg://REPORT_WRITER:URL_ENCODED_PASSWORD@postgres:5432/monitoring_db
```

Grafana가 실행되는 EC2-A 또는 observability Compose `.env`:

```dotenv
GRAFANA_REPORTING_DB_HOST=EC2_B_PRIVATE_IP:5432
GRAFANA_REPORTING_DB_NAME=monitoring_db
GRAFANA_REPORTING_DB_USER=grafana_reporting
GRAFANA_REPORTING_DB_PASSWORD=DEPLOYMENT_SECRET
GRAFANA_REPORTING_DB_SSLMODE=require
```

호스트는 실제 사설 IP로 바꿉니다. 컨테이너 localhost는 PostgreSQL 서버가 아닙니다. PostgreSQL TLS 미설정 환경에서는 사설망 정책에 맞게 SSLMODE를 `disable`로 명시합니다. 기본값은 `require`입니다. Grafana에서 PostgreSQL 사설 포트로 연결 가능해야 합니다.

새 Gold 이미지와 Grafana provisioning 파일을 배포한 뒤 Grafana를 재시작합니다. 기존 Prometheus 데이터소스는 그대로 사용합니다. PostgreSQL UID는 `nilm-reporting-postgres`입니다.

## 3. 한 날짜 검증 후 자동 실행 활성화

EC2-B 배포 디렉터리에서:

```sh
docker compose --env-file .env -f compose.yaml run --rm --no-deps gold-profile \
  household-report daily --as-of 2026-09-23 --window-days 90
```

Gold가 완료된 실제 날짜로 바꿉니다. 보고서의 대상 가구는 Gold 이미지의 `OBSERVATION_TARGETS_FILE` 설정을 따릅니다. 운영 Gold와 동일한 대상 설정을 이미지에 포함하거나 같은 파일을 마운트해야 합니다.

성공 후 Grafana의 `NILM · 일별 보고서`, `NILM · 일별 추세`를 엽니다. 가구·출처·보고일/버전을 선택합니다. 기본 시간 선택기는 숨겨져 있으며 모든 패널은 선택한 report_id의 고정된 기간을 조회합니다. 화면을 새로고침하면 보고서 선택 목록을 다시 읽습니다. 최신 생성 보고서는 목록 최상단이며 과거 보고서를 선택한 URL은 그 버전을 유지합니다.

`gold-daily.env` (systemd 실행기가 읽는 환경 파일, Compose `.env`와 별도):

```dotenv
HOUSEHOLD_REPORT_ENABLED=true
HOUSEHOLD_REPORT_WINDOW_DAYS=90
```

기존 Gold daily timer가 날짜별 Gold 완료 직후 보고서를 생성합니다. Gold 입력 미완료(10)이면 해당 날짜 보고서를 건너뛰고 실패 상태를 유지합니다. 보고서 실패는 실행기 실패로 전달됩니다. 마지막 정상 보고서가 남아도 화면의 보고일/생성시각은 그대로이므로 오늘 보고서로 오인하면 안 됩니다. 초기값은 false로, 스키마·계정 설정 없이 기존 운영 배치를 깨뜨리지 않습니다.

## 과거 평가 보고서

```sh
household-report daily --as-of 2026-09-23 --window-days 90 \
  --historical-manifest /nilm/historical_assessments/run_id=RUN_ID/manifest.json
```

이는 local JSONL manifest가 아니라 이전 `import-assessments`가 반환한 lake manifest입니다. 해당 실행의 평가 범위에 맞는 보고일을 사용합니다. 직접 만든 입력 manifest는 기존 `build` → `publish` 경로도 지원합니다. 예전 보고서는 일별 projection이 없어 새 대시보드 목록에 나타나지 않으므로 새 report_rule_version으로 재생성합니다.

## 표시의 의미와 제한

- 최대 점수는 그 날짜에 **저장된 VALID 평가의 최대값**입니다. PARTIAL 최대값은 별도 막대로 표시합니다. 평가가 없으면 산출 불가입니다.
- 장시간 사용 이벤트/실제 알림 발송 이력은 M/I/A 평가 기록과 별개이며 이번 화면에 연결하지 않았습니다. 요약에도 미연결로 표시합니다.
- 요약 문장은 배치가 생성한 데이터이며 Grafana Table로 표시합니다. 추가 플러그인·HTML 실행이 필요 없습니다.
- typed SQL 뷰가 기존 report_rows를 조회하므로 동일 report_id의 모든 패널이 같은 스냅샷을 봅니다. 90일 평가 상세는 lake와 DB에 누적되므로 데이터량에 맞게 보존 기간을 정해야 합니다.
- 자동화 실패는 기존 systemd/journal에서 확인합니다. 별도 메일/PDF 또는 잡 상태 대시보드는 이번 범위에 포함하지 않습니다.
