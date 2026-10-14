# 실험 원본 데이터 (보고서 부록의 출처)

| 파일 | 내용 |
|---|---|
| `cluster-repeat-results.csv` | 1부 분산 실험 16회 (`time,config,repeat,exit,status,seconds,container_oom,host_oom,avail_before_mb`) |
| `bench-results.csv` | 1부 규모·튜닝·local 14회 (`...,households,workers,partitions,repeat,exit,status,seconds,...,date`) |
| `cross-results.csv` | 2부 전체 42행, 실패 포함 (`time,scenario,run,datanodes,b_workers,a_workers,node_distribution,exit,status,time_sec,alive_workers,live_datanodes,container_oom_b,host_oom_b,avail_before_mb,config`). 첫 실패 행은 열이 밀려 있음(WARN 문자열이 alive_workers 자리에 기록됨) |
| `cpu-by-run.csv` | 회차별 시간 창 안 EC2-B 컨테이너 CPU 평균 (`stats`에서 산출). A worker 는 미수집 |
| `run-dates/all-run-dates.csv` | 모든 실행 폴더의 날짜별 기록 115행 (`config,execution,date,exit_code,status,seconds,first_error`) |
| `stats-ec2b-15s.csv.gz` | EC2-B 컨테이너 CPU·메모리 15초 간격 (9/26 17:18 ~ 10/2 02:52 KST, 36만 행) |
| `bin/` | 1부 스크립트(`/opt/nilm/experiments/load/bin/`) 사본 |
| `cross-host/` | 2부 스크립트(`experiments/cross-host/`) 사본. 저장소 `.gitignore`가 `/experiments/`를 제외하므로 여기에도 둔다 |

드라이버 전체 로그(`execution.log` 97개), `config.json`, `result.json`, 구성별 `batch-*.env`는 백업 `C:\1_on_maum\backup\b\experiments.tgz`에 있다.
