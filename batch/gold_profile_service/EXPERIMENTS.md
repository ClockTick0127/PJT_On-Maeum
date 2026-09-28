# 실험 기록 실행

`gold-experiment`는 Gold 이미지 안에서 날짜별 명령을 실행한다. Docker 소켓이나
컨테이너 내부 Docker CLI는 필요하지 않다. 기존 `history-generator run-dates`와
그 JSONL 기록은 변경하지 않는다. 기존 CSV를 가져오는 기능은 포함하지 않는다.

## 저장 위치

| 환경 | 컨테이너 | 호스트 기본 경로 |
| --- | --- | --- |
| 로컬 Compose | `/app/experiments` | 저장소 루트의 `experiments/` |
| EC2-B Compose | `/app/experiments` | `/opt/nilm/experiments` |

Compose의 `EXPERIMENTS_HOST_DIR`로 호스트 경로를 변경할 수 있다. 로컬 기본 상대
경로는 `infrastructure/local/compose.yaml` 기준 `../../experiments`다.
EC2에서는 호스트 디렉터리를 만들고 컨테이너 실행 사용자에게 쓰기 권한을 부여한다.
디렉터리는 컨테이너 `--rm` 종료 후에도 남는다. 운영 DB 비밀번호 등 비밀값을
실험 ID, dataset ID 또는 명령 인자에 넣지 않는다. 환경변수 전체는 기록하지 않지만
하위 명령이 출력한 stdout/stderr는 로그에 그대로 보관한다.

```text
experiments/<experiment-id>/<execution-id>/
  config.json
  run-dates.csv
  result.json
  execution.log
```

experiment ID는 비교할 실험 이름이며 execution ID는 실행마다 UTC 시각+UUID로
자동 생성된다. `--execution-id`로 지정해도 기존 디렉터리는 절대 덮어쓰지 않는다.
각 실행에 설정을 따로 보존하므로 같은 실험의 반복 측정 조건을 비교할 수 있다.

## 로컬 (저장소 루트에서 실행)

수정된 entry point가 들어가도록 이미지를 먼저 빌드한다.

```sh
docker compose -f infrastructure/local/compose.yaml build gold-profile
docker compose -f infrastructure/local/compose.yaml run --rm --no-deps gold-profile gold-experiment --experiment-id demo-3h-91d --dataset-id demo-v1 --households 3 --sampling-seconds 1 --workers 1 --run-mode INITIAL_BUILD --image nilm-gold-profile:local --from 2026-06-25 --to 2026-09-23
```

## EC2 (/opt/nilm에서 실행)

수정된 Gold 이미지를 기존 배포 절차로 빌드·Docker Hub에 게시한 뒤,
`.env`의 `GOLD_PROFILE_IMAGE`를 그 태그/다이제스트로 지정한다.
아래 pull은 이미 게시된 이미지를 내려받을 뿐 새 코드를 게시하지 않는다.

```sh
mkdir -p /opt/nilm/experiments
docker compose --env-file .env -f compose.yaml pull gold-profile
docker compose --env-file .env -f compose.yaml run --rm --no-deps gold-profile gold-experiment --experiment-id demo-3h-91d --dataset-id demo-v1 --households 3 --sampling-seconds 1 --workers 1 --run-mode INITIAL_BUILD --from 2026-06-25 --to 2026-09-23 --image YOUR_PULLED_IMAGE --git-commit YOUR_COMMIT
```

Gold가 사용하는 서비스·DB·레이크와 관측 대상 설정은 미리 준비되어 있어야 한다.
실험 ID는 기록을 분리할 뿐 데이터 레이크나 DB를 격리하지 않는다.

## 보고서 측정

기본 하위 명령은 `gold-profile daily --as-of {date} --no-publish`다.
보고서까지 실행하는 명령은 아니며, 보고서는 다음처럼 별도로 측정한다.
`--command`는 반드시 마지막에 둔다. 날짜만 치환하며 셸 해석은 하지 않는다.

```sh
docker compose -f infrastructure/local/compose.yaml run --rm --no-deps gold-profile gold-experiment --experiment-id report-3h --dataset-id demo-v1 --households 3 --sampling-seconds 1 --workers 1 --run-mode DAILY_UPDATE --from 2026-09-23 --to 2026-09-23 --command household-report daily --as-of {date} --window-days 90
```

과거 재평가 보고서는 하위 명령에 `--historical-manifest`를 추가한다.
실시간 기록 보고서는 기존 `REPORT_DATABASE_URL` 및 보고서 스키마가 필요하다.

## 기록 의미

- config: 기간, 입력 ID, 선언한 가구/worker 수, 요청 실행 종류, 코드·이미지 식별자,
  명령 및 허용 목록의 Spark 환경설정. `--workers`는 실제 worker를 생성하지 않는다.
  지정하지 않은 코드·이미지·자원 설정은 null(미기록)이며 자동 검증값이 아니다.
- CSV: 실행 순서, 날짜, exit, 상태, 벽시계 소요시간, 단계별 원본 결과 JSON,
  report ID, 단계에서 보고된 reused run ID. 없는 단계별 시간은 추정하지 않는다.
- result: 전체 벽시계 시간, 날짜별 원본 결과, 종료 코드별 건수, 최종 상태.
  `correctness_verified=false`이며 정확성 검증을 자동으로 주장하지 않는다.
- log: 하위 명령의 stdout/stderr. 실행 중에도 호스트에서 확인할 수 있다.
- 10=입력 부족, 11=건너뜀, 12=발행 대기를 성공 0으로 바꾸지 않는다.
  날짜 순회는 이 코드들에서 계속하고, 그 외 실패는 기본 중단한다(`--keep-going`으로 변경).
- 결과 JSON이 없는 exit 0 실행은 UNVERIFIED/exit 1로 기록한다.
- 각 날짜가 끝날 때 result를 갱신한다. 강제 종료/SIGKILL이면 RUNNING 기록이 남을 수 있다.
- `--run-mode RECOMPUTE`는 기록용 선언이며 강제 재계산 옵션이 아니다. 실제 출력의
  재사용 여부를 확인하고 성능 비교에서 분리한다. 측정시간에는 프로세스 시작 비용이 포함된다.

기존 CPU 모니터링, Spark event log, 입력 건수 검증은 별도로 수집한다.
이번 기능은 실제 자원 사용량이나 분산 실행 여부를 자동 측정하지 않는다.
