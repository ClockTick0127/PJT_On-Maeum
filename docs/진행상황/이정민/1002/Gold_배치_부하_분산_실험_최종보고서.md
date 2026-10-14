# Gold 배치 부하·분산 실험 최종 보고서

- 기간: 2026-09-26 ~ 2026-10-02 (EC2-B 단독 → EC2-A 추가)
- 작성: 2026-10-02
- 대상: `gold-profile daily` 파이프라인(전력 Silver → 가전 사용 요약 → 28일 Gold 프로필)의 실행 시간을 가구 수, Spark worker 수·배치, HDFS datanode 수, 셔플 파티션, 드라이버 메모리에 따라 측정
- 성격: 성능 측정 결과와 트러블슈팅 기록을 함께 남긴다. 모든 회차의 원시 값은 부록에 있다.
- 선행 문서: [1001/부하_분산_실험_결과.md](../1001/부하_분산_실험_결과.md) (1부·2부 요약). 이 문서는 그 문서를 대체할 수 있도록 전체를 다시 적었다.

---

## 0. 한눈에 보기

| 질문 | 답 | 근거 |
|---|---|---|
| 처리 시간은 가구 수에 비례하나 | 거의 비례(1.7~1.9배/2배) | 500 → 1000가구, 5개 구성 전부 (6.3절) |
| 한 서버에서 worker를 늘리면 | 2개까지만 효과, 3·4개는 포화 | 1,179 → 808 → 802 → 822초 (5.2절) |
| 포화의 원인은 | 알고리즘 순차 구간보다 **한 서버 안 자원 경합** | 두 서버에서는 4코어까지 계속 빨라짐 (6.5절) |
| 같은 코어 수를 두 서버에 나누면 | **13% 빠르다** (500·1000가구 동일) | 808 → 702초, 1,466 → 1,265초 |
| datanode까지 나누고 입력을 복제하면 | **추가 4~5%** | 702 → 669초, 1,265 → 1,213초 |
| 코어 2배(두 서버 4코어)로 | 1.37배(500) / 1.46배(1000) | 669 → 490초, 1,213 → 828초 |
| 셔플 파티션 200 → 12 | 19~20% 단축 | 1,179 → 947초, 802 → 647초 (5.4절) |
| 1000가구 드라이버 메모리 | 4 GB 부족, 6 GB 필요 | 둘째 날부터 OOM → 6 GB로 4일 완료 (5.6절) |
| 재현성 | 회차 간 편차 1~3% | 전 구성 3~5회 반복 |
| OOM | 메모리 설정 확정 이후 0건 | 1부 16+14회, 2부 30+회 |

총 실행 폴더 68개, 날짜별 배치 실행 115회, 그중 성공 97회·실패 18회(원인은 7장).

---

## 1. 무엇을 쟀는가

### 1.1 측정 대상 파이프라인

```text
gold-profile daily --as-of D
  1. power-silver run          Bronze(1초 또는 10초 간격 전력 원본) → 중복 제거·검증 → Silver power_clean, 관측일 판정
  2. power-silver usage-daily  Silver + 실시간 분석 세션·영수증 manifest → 가전별 일별 사용 요약(appliance_usage_daily)
  3. 창 정렬                   창(기본 28일, 실험 3일) 안의 다른 날짜가 같은 세션 스냅샷인지 확인
  4. gold-profile run          창 안의 일별 요약 → 루틴 기준선 + 시간대 통계 (Spark)
  5. (publish)                 Kafka 발행. 실험에서는 --no-publish 로 제외. 10/1 이후 코드에서 제거됨
```

실험 드라이버는 `gold-experiment`(`batch/gold_profile_service/src/gold_profile/experiment.py`)다. 날짜 범위를 받아 날짜마다 `gold-profile daily`를 실행하고 exit code·상태·초를 `run-dates.csv`에 적는다.

### 1.2 한 회차가 실제로 처리한 양 (중요)

**모든 단일 날짜 회차는 "하루치 Silver + 사용 요약 + 그날까지 있는 날짜의 Gold"만 처리했다.** 창 정렬(3단계)은 창 안의 날짜 중 **이미 Silver가 있는 날짜만** 다시 집계하고, 없는 날짜는 `skipped_dates`로 보고하기 때문이다. 구성마다 새 출력 경로를 쓰는 실험에서는 다른 날짜의 Silver가 없으므로 창은 항상 비어 있고, 그래서 거의 모든 회차가 exit 10(`INPUT_INCOMPLETE`)이다. 이 사실은 10/1에 7일 창 실험(6.4절)이 효과가 없는 것을 보고 확인했다.

따라서 이 보고서의 시간 값은 **"가구 N곳의 하루치를 끝까지 처리하는 시간"**이다. 1부에서 9/14~9/23처럼 여러 날짜를 연속 처리한 회차만 뒤쪽 날짜에서 창이 차서 exit 12(완료)가 나왔다.

### 1.3 exit code

| exit | 상태 | 뜻 | 측정 포함 |
|---:|---|---|---|
| 0 | SUCCEEDED | 완료 | 포함 |
| 12 | PUBLISH_PENDING | 완료, 발행만 대기(10/1 이전 코드) | 포함 |
| 10 | INPUT_INCOMPLETE | 계산은 끝났으나 창이 안 참 | **포함** (1.2절) |
| 1 | FAILED / NO_REPORT | 실패 | 제외, 7장에 원인 |
| 11 | SKIPPED | 다른 실행이 날짜를 점유 | 없었음 |

### 1.4 시간 측정 지점

`run-dates.csv`의 `seconds` = `gold-experiment`가 `gold-profile daily` 프로세스를 띄워서 끝날 때까지 벽시계 시간. Spark 세션 생성, 세 단계, manifest 기록, DB 커밋을 모두 포함한다. 드라이버 컨테이너 생성 시간은 포함하지 않는다.

---

## 2. 환경

### 2.1 서버

| | EC2-B (172.26.12.162) | EC2-A (172.26.8.92) |
|---|---|---|
| 종류 | Lightsail, Ubuntu 24.04 | Lightsail, Ubuntu 24.04 |
| vCPU / 메모리 | 4 / 15 GB | 4 / 15 GB |
| 디스크 | 309 GB | 309 GB (여유 234 GB) |
| 실험 중 떠 있던 운영 컨테이너 | Kafka, PostgreSQL, HDFS namenode·datanode, 실시간 분석, 집계, bronze-loader, session-lake-loader, mqtt-kafka-bridge, node-exporter | 모니터링, API 게이트웨이, Keycloak, Redis, Mosquitto, 시뮬레이터, Grafana, Prometheus, blackbox, **Jenkins(1.2 GB)**, 프런트엔드 |
| 실험 전 가용 메모리 | 약 8.9 GB → 정리 후 10.9 GB | 약 10.9 GB |

운영 컨테이너를 내리지 않고 측정했다. 2부의 핵심 결론(한 서버 안 경합)은 이 조건에서 나온 것이다.

### 2.2 EC2-B 환경 정리 (9/26)

| 작업 | 명령 | 이유 |
|---|---|---|
| HDFS 쿼터 상향 | `hdfs dfsadmin -setSpaceQuota 250g /nilm`, `32g /nilm/gold` | 부하 데이터와 보고서가 기본 80 GB, 2 GB를 넘음 |
| 잔여 프로세스 정리 | Kafka 컨테이너 안 ConsoleConsumer 5개 종료 | 이틀째 남아 2 GB 점유 |

### 2.3 Spark 실행 방식

| 모드 | 어디서 계산 | 설정 |
|---|---|---|
| local | 드라이버 컨테이너 안 CPU | `SPARK_MASTER` 비움, `local[4]`(사전 실험) |
| 클러스터(1부) | EC2-B 도커 브리지 안 spark-master + spark-worker-N | `spark://spark-master:7077`, 컨테이너 이름으로 통신 |
| 클러스터(2부) | 양쪽 서버 host 네트워크의 master·worker | `spark://172.26.12.162:7077`, 사설 IP로 통신 (6.1절) |

### 2.4 메모리 설정 (OOM 사전 실험 뒤 확정, 9/27~28)

| 대상 | 설정 |
|---|---|
| 드라이버 | 1.5 GB (`--driver-memory 1536m`). local 비교 4 GB, 1000가구 Gold 6 GB |
| worker 1개 | 코어 1, `SPARK_WORKER_MEMORY=1200m`, `spark.executor.memory=1g`, 데몬 256 MB, 컨테이너 상한 2.5 GB(`--memory 2560m --memory-swap 2560m`) |
| master | 컨테이너 768 MB, 데몬 256 MB |
| 공통 | `spark.sql.autoBroadcastJoinThreshold=-1`, `spark.executor.cores=1`, `spark.cores.max=worker 수` |
| 안전장치 | Spark 컨테이너 `--oom-score-adj 500`(OOM 시 운영 컨테이너보다 먼저 죽음), 실행 전 가용 메모리 확인(`2900 + 1700×worker` MB 미만이면 SKIP), 회차당 `timeout 45~60m`(2부 120m) |

### 2.5 HDFS

- namenode 1개, datanode 1개(EC2-B). `dfs.replication=1`. 2부 hdfs-spark·scale 시나리오에서 EC2-A에 datanode 1개 추가.
- 블록 크기는 기본 128 MB이지만 Bronze 파일이 작아 파일당 블록 1개(평균 3~5 MB).
- 실험 경로: `/nilm/experiments/load-<N>/bronze`, `.../manifests`(공유 입력), `/nilm/experiments/load-<N>/<구성>/{silver,gold,manifests}`(구성별 출력, 회차 시작 때 삭제).

---

## 3. 데이터

`batch/history_generator`(규칙 기반 과거 데이터 생성기)로 가상 가구를 만들었다.

| 항목 | 값 |
|---|---|
| 시나리오 | `demo_3households.json` + `load_households`: 접두 `L`, 템플릿 H008/H009/H010(가중치 3:2:5), 비활동 가구 5%, 파형 풀 50 |
| 간격 | 10초 → 가구당 하루 8,640행 |
| 하루 행 수 | 10가구 86,400 / 100가구 864,000 / 500가구 432만 / 1000가구 864만 |
| 비교 | 1000가구 10초 간격 = 실제 1초 간격 가구 100곳 |
| Bronze 용량 | 500가구 4일 440 MB(147파일), 1000가구 7일 1.8 GB(343파일). 하루 약 110 MB / 260 MB |
| 함께 생성 | 분석 처리 영수증(행 수 = 전력 행 수), 가전 사용 세션(1000가구 3일 8,680건) |
| 적재 시간 | 1000가구 3일 193초 |
| 적재 날짜 | 500·1000가구: 9/20~9/23 (9/26 적재). 1000가구 9/17~9/19 추가(10/1). 10·100가구: 9/14~9/23 |

`load-write.sh`는 이미 있는 날짜를 건너뛰므로 재실행해도 Bronze가 중복되지 않는다.

---

## 4. 스크립트

### 4.1 1부 — `/opt/nilm/experiments/load/bin/` (EC2-B, root)

| 스크립트 | 역할 | 호출 |
|---|---|---|
| `common.sh` | 공통 변수: `BASE`, Gold 이미지(compose에서 읽음), 도커 네트워크, 저장소 경로, `SPARK_CLASS`, 날짜 범위 `LOAD_FROM/TO`(기본 9/20~9/23) | source |
| `load-setup.sh N` | 가구 N곳 시나리오 JSON, 관측 대상 `targets-N.json`, 레이크 경로 `lake-N.env` 생성 | `load-setup.sh 500` |
| `load-write.sh N` | history-generator로 Bronze·영수증·세션을 HDFS에 적재. 있는 날짜 건너뜀 | `LOAD_FROM=.. LOAD_TO=.. load-write.sh 1000` |
| `load-config.sh N CFG` | 구성별 DB(`load_N_CFG`, 컨테이너 `nilm-exp-pg`) 생성·스키마 적용, 출력 경로, Spark 설정을 `batch-N-CFG.env`에 기록. CFG가 `w<숫자>`로 시작하면 클러스터(`spark.cores.max=숫자`), `local*`이면 local. `SHUFFLE`, `DRIVER_MEM` 환경변수 반영 | `SHUFFLE=12 load-config.sh 500 w3p12...` |
| `load-run.sh N CFG W` | `docker compose run gold-profile gold-experiment`로 날짜별 실행. 결과는 `/opt/nilm/experiments/load-N-CFG/<실행ID>/` | `load-run.sh 500 w3r1 3` |
| `cluster.sh up W` / `down` | 브리지 네트워크에 spark-master + worker W개 기동·제거. master UI `172.26.12.162:18080` | |
| `cluster-repeat.sh "3 4" 5` | worker 수별 반복. 회차마다 클러스터 재기동, DB·출력 삭제, 메모리 확인, OOM 집계(cgroup `memory.events`, `dmesg`). 결과 `cluster-repeat-results.csv` | |
| `bench.sh "10 100 500 1000" "3" "200" 2` | 가구×worker×파티션 반복. 날짜 `BENCH_DATE`. 결과 `bench-results.csv` | |
| `local-repeat.sh N R` | local 모드(드라이버 4 GB) 반복, `bench-results.csv`에 `workers=local`로 기록 | |
| `gold1000.sh` | 1000가구 9/20~9/23 연속, 드라이버 6 GB | |
| `load-all.sh` | 10/100/1000가구 local4 연속 실행 (9/26 첫 실험) | |
| `cluster-all.sh`, `phase2.sh`, `phase3.sh`, `phase3b.sh`, `phase3c.sh` | 9/27 사전 실험(창 2일, worker 1·2·3·4). OOM·FetchFailed로 대부분 실패. 메모리 설정 확정의 근거 | |

결과 CSV 열:

- `cluster-repeat-results.csv`: `time,config,repeat,exit,status,seconds,container_oom,host_oom,avail_before_mb`
- `bench-results.csv`: `time,households,workers,partitions,repeat,exit,status,seconds,container_oom,host_oom,avail_before_mb,date`
- `stats.csv`: 15초 간격 `time,container,cpu%,mem` (9/26 17:18 ~ 10/1, 36만 행)

### 4.2 2부 — `experiments/cross-host/` (저장소) = `/home/ubuntu/cross-host/` (두 서버)

| 스크립트 | 서버 | 역할 |
|---|---|---|
| `common-cross.sh` | 공통 | IP, 포트(master 7077, UI 18080, 드라이버 7078/7079), worker 예산, A로 가는 ssh 명령, `alive_workers()`(master `/json/` 조회) |
| `ec2b/hdfs-ports.sh apply\|revert` | B | compose.yaml 백업 후 namenode 9000, datanode 9864·9866 공개 + `dfs.datanode.hostname=172.26.12.162`. namenode·datanode 재생성 |
| `ec2b/cluster-cross.sh up W\|down` | B | host 네트워크 master + B worker W개(`spark-worker-b<i>`) |
| `ec2b/load-config-cross.sh N CFG TOTAL` | B | `load-config.sh` 호출 후 env 수정: `SPARK_MASTER=spark://172.26.12.162:7077`, `spark.driver.host=172.26.12.162`, `driver.port=7078`, `blockManager.port=7079`, `spark.hadoop.dfs.client.use.datanode.hostname=true`, `spark.shuffle.readHostLocalDisk=false` |
| `ec2b/load-run-cross.sh N CFG TOTAL` | B | `load-run.sh`와 같되 드라이버 포트 2개를 `-p 172.26.12.162:7078:7078 -p ...7079` 로 공개 |
| `ec2b/experiment.sh 이름 dn B A [반복]` | B | 시나리오 1개 자동 반복. datanode 수 맞추기(2면 A datanode 기동 + 입력 `setrep 2`, 1이면 입력 복제 확인 → A 내림 → dead 대기 → `setrep 1`), master·worker 기동(A는 ssh), ALIVE worker 대기(최대 3분), 실행, 기록. `N`, `WINDOW_DAYS`, `SHUFFLE`, `LOAD_FROM/TO` 환경변수 |
| `ec2b/run-all.sh 반복 conf` | B | conf의 시나리오를 순서대로 `experiment.sh`로 실행, 끝에 평균 출력 |
| `ec2b/scenarios*.conf` | B | `이름 datanode수 Bworker Aworker` 한 줄씩 |
| `ec2b/setup-ssh-to-a.sh`, `print-a-hosts.sh` | B | B→A ssh 키 생성, A용 `a.env`(이미지 digest) 출력 |
| `ec2a/datanode-cross.sh up\|down` | A | host 네트워크 datanode(`hostname=172.26.8.92`, 볼륨 `cross_datanode_data`) |
| `ec2a/worker-cross.sh up W\|down` | A | host 네트워크 worker W개(`spark-worker-a<i>`, `--restart unless-stopped`) |
| `ec2a/check-cross.sh` | A | B로의 포트 연결 확인 |
| `stats-cross.sh start\|stop` | 공통 | 실험 컨테이너 CPU·메모리 15초 기록 → `/opt/nilm/experiments/cross/stats-<hostname>.csv` |

결과 CSV `cross-results.csv` 열: `time,scenario,run,datanodes,b_workers,a_workers,node_distribution,exit,status,time_sec,alive_workers,live_datanodes,container_oom_b,host_oom_b,avail_before_mb,config`

구성 이름 규칙: `w<총코어>x<시나리오>d<datanode>b<B worker>a<A worker>r<회차>`. 예 `w2xsparkdist1000d1b1a1r2`.

---

## 5. 1부 — 단일 서버(EC2-B) 실험 (9/26 ~ 9/30)

### 5.1 사전 실험과 메모리 확정 (9/26~27)

목적: 파이프라인이 실험 데이터로 끝까지 도는지, 어디서 메모리가 부족한지 확인.

| 일시 | 구성 | 결과 | 배운 것 |
|---|---|---|---|
| 9/26 16:51 | `load-10-local`, 9/14~9/23 10일 연속, local, 드라이버 3 GB | 10일 모두 완료. 창 미충족 6일 120~126초, 창 충족 4일 184~189초 | Gold 포함일은 약 60초 추가 |
| 9/26 17:21 | `load-100-local`, 9/14~9/17 | 4일 246~253초 (중단) | 가구 10배에 2배 |
| 9/26 17:41~ | `load-all`: 10/100/1000가구 local4, 9/20~9/23 | 10: 123/124/186/183초, 100: 245/251/332/327초, **1000: 첫날 1,272초 뒤 3일 NO_REPORT** | 1000가구 둘째 날부터 `java.lang.OutOfMemoryError: Java heap space`(executor, stage 41). 전날 데이터까지 입력에 잡혀 드라이버 3 GB 초과 |
| 9/27 00:14 | `load-1000-local6` 드라이버 6 GB | 기록 없음(중단) | phase2 재설계 |
| 9/27 00:26 | `local4b`(셔플 16, `maxPartitionBytes` 32 MB, 배치 1024) 10·100가구 | 10: 131/143초, 100: 335초 | 작은 입력에서는 파티션 축소가 오히려 느림 |
| 9/27 00:43 | `local4win2`(창 2일) 10/100/500가구, 9/22~23 | 10: 87/105초, 100: 183/213초, 500: 581/708초 | 창 2일에서 둘째 날 완료 |
| 9/27 02:55~ | `w1win2`~`w4win2`: 클러스터 worker 1·2·4(뒤에 3), 창 2일, 메모리 미조정 | w1: 868/1,013초 완료, w2: 756/913초 완료, **w3: 2,295초 실패, w4: 2,305초 후 3,039초 실패** | `FetchFailedException`(셔플 가져오기 실패) — 실행기 OOM으로 죽은 뒤 셔플 파일 소실. **worker 3~4개는 드라이버+실행기 합계가 가용 메모리 초과** |

이 결과로 2.4절의 메모리 설정을 확정했고, 이후 모든 회차에서 OOM은 0건이다.

### 5.2 분산 실험 — worker 수 (9/28~29, `cluster-repeat.sh`)

조건: 500가구, 9/23 하루, 파티션 200, 회차마다 새 DB·출력, 클러스터 재기동.

```bash
setsid nohup /opt/nilm/experiments/load/bin/cluster-repeat.sh "3 4" 5 > cluster-repeat.log 2>&1 &
setsid nohup /opt/nilm/experiments/load/bin/cluster-repeat.sh "1 2" 3 > cluster-repeat-12.log 2>&1 &
```

| worker | 회차 | 각 회차(초) | 평균 | 최소~최대 | Speedup | Efficiency | worker CPU 합(평균) | 드라이버 CPU |
|---:|---:|---|---|---|---|---|---|---|
| 1 | 3 | 1,179.9 / 1,179.4 / 1,177.8 | **1,179.0** | 1,177.8~1,179.9 | 1.00 | 100% | 110% (1개 110%) | 9% |
| 2 | 3 | 812.2 / 797.2 / 814.5 | **808.0** | 797.2~814.5 | 1.46 | 73% | 193% (각 96%) | 21% |
| 3 | 5 | 805.4 / 796.7 / 801.7 / 807.6 / 798.6 | **802.0** | 796.7~807.6 | 1.47 | 49% | 250% (각 80~93%) | 18% |
| 4 | 5 | 825.3 / 827.3 / 819.5 / 821.7 / 816.7 | **822.1** | 816.7~827.3 | 1.43 | 36% | 274% (각 66~74%) | 23% |

CPU는 `stats.csv`에서 회차 시간 창 안의 컨테이너 평균을 더한 값이다(`cpu-by-run.csv`). worker 4개에서 worker당 사용률이 70% 아래로 떨어지는 것이 포화의 직접 증거다. Amdahl 법칙으로 역산하면 순차 구간 비율은 약 37%다. 16회 모두 OOM 0.

### 5.3 규모 실험 — 가구 수 (9/29, `bench.sh`)

조건: worker 3개, 파티션 200, **9/20 첫날**(전날 데이터 없음, 모든 규모 동일 조건), 각 2회.

```bash
BENCH_DATE=2026-09-20 /opt/nilm/experiments/load/bin/bench.sh "10 100 500 1000" "3" "200" 2
```

| 가구 | 회차(초) | 평균 | 초당 처리 행 | worker CPU 합 | local 4코어 첫날(5.1·5.5절) | 차이 |
|---:|---|---|---|---|---|---|
| 10 | 223.5 / 227.1 | **225.3** | 383 | 224~252% | 123.0초 | +102초 |
| 100 | 389.7 / 391.8 | **390.7** | 2,211 | 234~242% | 245.1초 | +146초 |
| 500 | 820.0 / 828.3 | **824.1** | 5,242 | 249~259% | 653.2초(9/23) | +149초(9/23 클러스터 802초 대비) |
| 1000 | 1,402.2 / 1,414.9 | **1,408.6** | 6,134 | 267~270% | 1,272.3초(4 GB) / 1,215.9초(6 GB) | +137 / +193초 |

가구 1곳당 증가분은 클러스터·local 모두 약 1.13초로 같다. 클러스터는 실행기 기동·데이터 교환의 고정 비용이 100~150초 더 든다. 가구 수가 늘수록 worker CPU 합이 올라가 10가구에서는 CPU가 놀고(고정 비용 지배), 1000가구에서 비로소 3코어를 다 쓴다.

### 5.4 튜닝 실험 — 셔플 파티션 (9/29, `bench.sh`)

조건: 500가구, 9/23, 파티션 200 → 12, worker 1·3개, 각 2회.

```bash
/opt/nilm/experiments/load/bin/bench.sh "500" "1 3" "12" 2
```

| worker | 파티션 200 | 파티션 12 (회차) | 단축 |
|---:|---|---|---|
| 1 | 1,179.0초 | 947.0 / 947.0 → **947.0초** | 19.7% |
| 3 | 802.0초 | 646.2 / 647.8 → **647.0초** | 19.3% |

이 데이터 양(하루 432만 행)에 파티션 200은 태스크가 너무 잘게 쪼개져 스케줄링·셔플 파일 비용이 컸다. 처음 구성(worker 1, 파티션 200, 1,179초) 대비 worker 3·파티션 12(647초)는 **1.82배**다. 단, 5.1절 `local4b`처럼 입력이 작을 때는 파티션 축소가 효과 없거나 역효과였다.

### 5.5 local 500가구 재측정 (9/30, `local-repeat.sh`)

worker 수를 0으로 넘긴 스크립트 오류로 처음 실패해 1로 고쳐 재실행. 500가구, 9/23, 드라이버 4 GB, 파티션 200.

| 회차 | 시간 | 드라이버 CPU(local이므로 계산 전부) |
|---:|---|---|
| 1 | 651.0초 | 315% |
| 2 | 655.4초 | 319% |
| 평균 | **653.2초** | |

### 5.6 1000가구 Gold — 드라이버 메모리 (9/30, `gold1000.sh`)

local 4코어, 9/20~9/23 연속, 창 3일.

| 날짜 | 드라이버 4 GB (9/26) | 드라이버 6 GB (9/30) |
|---|---|---|
| 9/20 | 1,272.3초 | 1,215.9초 |
| 9/21 | 실패(NO_REPORT, 605.8초) | 1,228.1초 |
| 9/22 | 실패 | 1,733.4초 (창 충족, 완료) |
| 9/23 | 실패 | 1,715.9초 (완료) |

6 GB에서 4일 모두 끝났다. 창이 찬 날은 첫날보다 약 500초 더 걸린다(Gold가 3일치 요약을 실제로 계산하고 발행 준비까지). **1000가구 운영 기준은 드라이버 6 GB.**

### 5.7 1부 해석

1. 한 서버에서는 worker 2개(2코어)가 최적이고, 그 이상은 CPU 사용률이 떨어지며 시간이 늘어난다.
2. 가구 수에는 선형. 고정 비용은 클러스터 100~150초, local 수십 초.
3. 셔플 파티션은 데이터 양에 맞춰야 한다. 하루 400만 행 규모에서 12가 200보다 19% 빠르다.
4. 드라이버 메모리는 창이 차는 둘째 날부터 급증한다. 1000가구는 6 GB.
5. 이 단계의 "분산"은 노드 분산이 아니라 **단일 서버 안 실행기 분산**이다. 포화의 원인이 알고리즘인지 자원 경합인지는 2부에서 가렸다.

---

## 6. 2부 — 두 서버(EC2-B + EC2-A) 실험 (9/30 ~ 10/2)

### 6.1 구성 변경

1부 클러스터는 한 서버의 도커 브리지 안에서 컨테이너 이름으로 서로를 찾는다. 다른 서버에서는 그 이름을 풀 수 없어 다음과 같이 바꿨다.

```text
EC2-B 172.26.12.162                                    EC2-A 172.26.8.92
  spark-master     host 네트워크 :7077, UI :18080  <----  spark-worker-a1.. host 네트워크, --restart
  spark-worker-b1.. host 네트워크
  load-driver-*    compose 브리지, :7078 :7079 호스트 공개  <---- 실행기 접속
  namenode :9000 공개                               <----  datanode-a host 네트워크, hostname=172.26.8.92
  datanode hostname=172.26.12.162, :9864 :9866 공개
  nilm-exp-pg      브리지, 드라이버만 접속
```

| 항목 | 1부 | 2부 | 이유 |
|---|---|---|---|
| master·worker 네트워크 | 도커 브리지, 컨테이너 이름 | host, 사설 IP(`--host`, `SPARK_LOCAL_HOSTNAME`) | 서버 간 이름 해석 불가. Spark는 master→worker, 드라이버↔실행기, 실행기↔실행기(셔플) 연결을 양방향으로 새로 연다 |
| 드라이버 주소 | `spark.driver.host=<컨테이너명>` | `=172.26.12.162`, `driver.port=7078`, `blockManager.port=7079` 고정·공개 | 실행기가 드라이버에 붙어야 함 |
| HDFS datanode | 컨테이너 ID hostname, 브리지 IP | `dfs.datanode.hostname=사설 IP`, 9864·9866 공개, 클라이언트 `use.datanode.hostname=true` | 실행기가 어느 서버에서든 datanode에 붙고, 블록 위치(IP) = 실행기 주소라 로컬리티가 맞음 |
| 방화벽 | 불필요 | 두 서버 ufw에 상대 사설 IP TCP 허용 + B에 도커 브리지 대역(172.17/16, 172.18/16) 허용 | Lightsail은 보안그룹 없음, 사설 IP는 Lightsail 방화벽 안 거침, 차단은 ufw(기본 deny) |
| 셔플 | 기본 | `spark.shuffle.readHostLocalDisk=false` | 같은 서버 worker들이 같은 주소라 Spark가 로컬 디스크 직접 읽기를 시도, 컨테이너라 `/tmp`가 달라 실패 |
| 자동화 | 수동 또는 B 로컬 반복 | `experiment.sh`가 A를 ssh로 조작, ALIVE·live datanode 대기 | |

이 변경 때문에 같은 구성(B worker 1개)이 1부보다 약 8% 느리게 측정됐다(1,179 → 1,273초). 호스트 네트워크와 호스트 공개 포트를 거치는 오버헤드로 본다. **1부와 2부 값은 섞어 비교하지 않는다.**

### 6.2 500가구 5개 시나리오 (9/30 23:48 ~ 10/1 03:41 KST, 각 3회)

조건: 9/23 하루, 창 3일, 파티션 200, worker당 코어 1·실행기 1.2 GB.

| 시나리오 | datanode | worker (B+A) | 코어 | 회차(초) | 평균 | 최소~최대 | baseline 대비 | 코어당 효율 | B worker CPU 합 |
|---|---:|---|---:|---|---|---|---|---|---|
| baseline | 1 | 1+0 | 1 | 1,283.5 / 1,259.7 / 1,274.5 | **1,272.6** | 1,259.7~1,283.5 | 1.00 | 100% | 103~109% |
| worker-up | 1 | 2+0 | 2 | 808.8 / 812.5 / 803.2 | **808.1** | 803.2~812.5 | 1.57 | 79% | 209~211% |
| spark-dist | 1 | 1+1 | 2 | 687.2 / 719.2 / 698.8 | **701.7** | 687.2~719.2 | 1.81 | 91% | 95~109% (B 1개) |
| hdfs-spark | 2 | 1+1 | 2 | 674.4 / 668.1 / 664.9 | **669.1** | 664.9~674.4 | 1.90 | 95% | 95~107% (B 1개) |
| scale | 2 | 2+2 | 4 | 488.1 / 494.8 / 486.7 | **489.8** | 486.7~494.8 | 2.60 | 65% | 177~193% (B 2개) |

A worker의 CPU는 A에서 수집기를 켜지 않아 기록이 없다(11장). 스모크(spark-dist와 같은 구성) 1회 770.5초는 표에 넣지 않았다.

### 6.3 1000가구 5개 시나리오 (10/1 13:38 ~ 10/2 02:01 KST)

조건은 6.2절과 같고 가구만 1000. baseline·spark-dist는 2회 + 보강 1회, 나머지 3회.

| 시나리오 | datanode | worker (B+A) | 코어 | 회차(초) | 평균 | 최소~최대 | 500가구 | 배율 | 1000가구 코어당 효율 |
|---|---:|---|---:|---|---|---|---|---|---|
| baseline-1000 | 1 | 1+0 | 1 | 2,381.7 / 2,398.8 / 2,350.8 | **2,377.1** | 2,350.8~2,398.8 | 1,272.6 | 1.87 | 100% |
| worker-up-1000 | 1 | 2+0 | 2 | 1,455.0 / 1,476.1 / 1,465.9 | **1,465.7** | 1,455.0~1,476.1 | 808.1 | 1.81 | 81% |
| spark-dist-1000 | 1 | 1+1 | 2 | 1,272.9 / 1,272.1 / 1,248.8 | **1,264.6** | 1,248.8~1,272.9 | 701.7 | 1.80 | 94% |
| hdfs-spark-1000 | 2 | 1+1 | 2 | 1,199.2 / 1,198.7 / 1,240.4 | **1,212.7** | 1,198.7~1,240.4 | 669.1 | 1.81 | 98% |
| scale-1000 | 2 | 2+2 | 4 | 831.6 / 822.0 / 830.9 | **828.2** | 822.0~831.6 | 489.8 | 1.69 | 72% |

실패 회차(spark-dist-1000 3회)는 7장. hdfs-spark-1000 3회차(1,240초)는 다른 두 회차보다 3% 느린데, 그 시간대에 백그라운드 HDFS 복제(7장 F3)가 돌고 있었다.

### 6.4 7일 창 시도 (10/1 15:02 ~ 16:30 KST, 중단)

"창을 7일로 늘려 입력을 키운다"는 계획으로 1000가구 Bronze 9/17~9/19를 추가 적재하고 `PROFILE_WINDOW_DAYS=7`로 hdfs-spark-1000-w7 1회차를 돌렸다. 결과 1,230.1초로 3일 창(1,199초)과 같았다. 창 정렬이 Silver 없는 6일을 전부 `skipped_dates`(`INPUT_MISSING`)로 보고하고 하루치만 처리했기 때문이다(1.2절). 측정 의미가 없어 중단했다. 입력을 늘리려면 한 회차에서 날짜를 연속 처리해야 한다(`LOAD_FROM=9/17 LOAD_TO=9/23`, 회차당 약 7배 시간). 시간 관계로 하지 않았다.

### 6.6 7일 연속 처리 분산 실험 (10/2 15:53 KST 시작, 서버 종료로 중단)

6.4절의 교훈대로 입력을 키우기 위해 한 회차에서 9/17~9/23 **7일을 연속 처리**하는 실험을 걸었다.
1000가구, Gold 창 7일, spark-dist-1000-7d(datanode 1, B1+A1) ×2 → hdfs-spark-1000-7d(datanode 2) ×2,
`experiment.sh`는 다중 날짜 합계를 기록하도록 수정(`RUN_TIMEOUT`, `time_sec`=날짜 합).

| 시각(KST) | 진행 |
|---|---|
| 15:53 | 체인 시작. 입력 복제 확인 → A datanode 내림 → dead 판정 대기(22분) |
| 16:15 | spark-dist-1000-7d 1회차 시작 |
| 16:36 | 9/17 완료 1,244.8초 (하루치 측정 1,264.6초와 같은 수준) |
| 16:55 | 9/18 처리 중 (Stage 169) |
| 17:56 | **두 서버 모두 접속 불가** (모든 공개 포트 타임아웃). 이후 인스턴스 종료 확인 |

결과는 9/17 하루치 1건뿐이라 결론을 낼 수 없다. 창이 차는 9/19 이후의 날짜별 시간 증가와 두 시나리오의
7일 합계 비교는 측정하지 못했다. 다시 하려면 `experiments/cross-host/README.md` 절차로 서버를 구성한 뒤
`N=1000 WINDOW_DAYS=7 LOAD_FROM=2026-09-17 LOAD_TO=2026-09-23 RUN_TIMEOUT=360m run-all.sh 2 scenarios-1000-7d.conf`
(회차당 약 2.5~3시간, 4회 약 11시간).

### 6.5 2부 해석

**(1) 서버 분산 효과 — worker-up 대 spark-dist (총 코어 같음)**

| 가구 | 한 서버 2코어 | 두 서버 1+1코어 | 차이 |
|---|---|---|---|
| 500 | 808.1초 | 701.7초 | **−13.2%** |
| 1000 | 1,465.7초 | 1,264.6초 | **−13.7%** |

서버 사이 셔플 비용보다 EC2-B 한 대 안에서 worker 2개가 운영 컨테이너(Kafka, PostgreSQL, 실시간 분석)와 CPU·디스크를 다투는 비용이 더 컸다. 비율이 가구 수와 무관하게 같으므로 경합 비용은 입력에 비례한다.

**(2) 저장소 분산 효과 — spark-dist 대 hdfs-spark**

| 가구 | datanode 1 (B) | datanode 2 (B+A, 입력 복제) | 차이 |
|---|---|---|---|
| 500 | 701.7초 | 669.1초 | −4.6% (33초) |
| 1000 | 1,264.6초 | 1,212.7초 | −4.1% (52초) |

Spark가 블록이 있는 서버의 실행기에 태스크를 우선 배정해 A 실행기가 자기 서버 datanode에서 읽는 비율이 늘어난다. 효과는 읽기 시간의 일정 비율이라 입력을 키워도 비율은 그대로이고 절대값만 커진다. 입력(하루 110~260 MB)이 작아 비율 자체가 작다.

**(3) 자원 확장 — hdfs-spark 대 scale (코어 2배)**

| 가구 | 2코어 | 4코어 | 배율 | 1부 단일 서버 4개 |
|---|---|---|---|---|
| 500 | 669.1초 | 489.8초 | 1.37 | 822.1초 → 두 서버가 1.68배 빠름 |
| 1000 | 1,212.7초 | 828.2초 | 1.46 | 1,408.6초(worker 3) → 1.70배 |

두 서버에서는 4코어까지 계속 빨라진다. 1부에서 worker 3→4개가 오히려 느려졌던 것은 알고리즘 포화가 아니라 **한 서버 안의 자원 경합**이었다. 입력이 클수록 고정 비용 비중이 줄어 확장 효율이 좋아진다(1.37 → 1.46).

**(4) 가구 수 영향** 모든 구성이 1.7~1.9배로 늘어 처리 시간은 입력에 선형이다. 2배가 안 되는 것은 고정 비용(Spark 기동, 쓰기, DB) 때문이다.

---

## 7. 실패 회차와 원인 (전체)

| # | 일시(KST) | 구성 | 증상 | 원인 | 조치 | 재실행 |
|---|---|---|---|---|---|---|
| F1 | 9/26 18:45 | load-1000-local4 9/21~23 (3회) | `OutOfMemoryError: Java heap space`, NO_REPORT | 둘째 날부터 전날 데이터까지 입력에 잡혀 드라이버 3~4 GB 초과 | 드라이버 6 GB | 9/30 `gold1000.sh` 4일 완료 |
| F2 | 9/27 03:55~05:31 | w3win2(2,295초), w4win2 9/23(3,040초) | `FetchFailedException`, 매우 오래 걸린 뒤 실패 | 메모리 미조정 상태에서 실행기 OOM → 셔플 파일 소실 → 재시도 반복 | 2.4절 메모리 설정, 컨테이너 상한, `oom-score-adj` | 9/28~ cluster-repeat 16회 정상 |
| F3 | 9/30 00:12 | local-repeat 첫 시도 | 즉시 종료 | worker 수 0 전달(`--workers 0`은 양수 필수) | 1로 수정 | 당일 2회 정상 |
| F4 | 9/30 16:05 | smoke (B1+A1) 934초 | `All masters are unresponsive`, 실행기 없음 | EC2-B ufw가 도커 브리지(드라이버) → host 네트워크 master 7077 연결을 차단 | `ufw allow from 172.18.0.0/16`, `172.17.0.0/16` | 16:34 770.5초 정상 |
| F5 | 9/30 22:38~23:46 | worker-up r1~r3 (B2) 1,302~1,378초 | `FetchFailedException` ← `NoSuchFileException .../shuffle_0_1_0.index` | host 네트워크에서 같은 서버 worker 2개의 주소가 같아 Spark가 셔플 블록을 로컬 디스크에서 직접 읽으려 함(`readHostLocalDisk`). worker가 각자 컨테이너라 `/tmp`가 다름 | `spark.shuffle.readHostLocalDisk=false` | 10/1 00:26~ 3회 정상(808초) |
| F6 | 10/1 14:54, 14:57 | spark-dist-1000 r1, r2 (67초) | `HdfsError: No active nodes contain this block` | datanode 1개 시나리오가 A를 **내리기 전에** 입력 복제계수를 1로 낮춤 → namenode가 일부 블록의 B 복제본을 삭제하고 A 것만 남김 → A를 내리자 읽기 불가 | 순서를 "A 내림 → dead 판정 → setrep 1"로 변경 | 17:44~ 정상 |
| F7 | 10/1 16:42 | spark-dist-1000 r1 (97초) | usage-daily `ConnectionError 172.26.8.92:9864` | 7일 창 중단 중 수동 `setrep 1 → 2`로 manifest 블록 2개의 B 복제본이 지워져 A에만 남은 상태에서 A dead → WebHDFS가 죽은 A로 리다이렉트 | A를 내리기 전에 입력 전체 `setrep -w 2`로 B 복제본 보장 | 17:44 1,272.9초, 보강 1,248.8초 |
| F8 | 10/1 00:04~ | run-all이 첫 시나리오 뒤 종료 (실패 회차는 아님) | 나머지 시나리오 미실행 | `while read` 루프 중 `experiment.sh` 안의 ssh가 표준입력(conf)을 삼킴 | 목록을 배열로 읽고 `< /dev/null` | 재시작 |
| F9 | 10/1 15:25~15:57 | hdfs-spark-1000-w7 체인 꼬임 (실패 회차는 아님) | 엉뚱한 분기 실행, A datanode 내려감, 50분 대기 | **실행 중인 `experiment.sh`를 scp로 덮어씀.** bash는 스크립트를 조금씩 읽어 바뀐 바이트 위치를 실행 | 중단 후 재시작. 수정본은 `.new`로 올린 뒤 `mv` | |
| F10 | 10/1 10:10~10:55 | hdfs-spark 입력 복제 45분 정체 | `setrep -w` 미종료 | Spark가 쓴 파일이 클라이언트 기본 복제계수 3 → datanode 2개가 되자 4.8만 블록이 대기열에 들어가 입력 147블록이 차례를 못 받음 | `/nilm/experiments`, `/nilm/gold` 복제계수 1로(메타데이터만). `experiment.sh`가 시나리오 시작 때 출력(`w*`)을 1로 내림 | |

실패 18회 중 측정 결과에 반영되지 않은 회차는 전부 재실행으로 대체했다. F8~F10은 회차 실패가 아니라 자동화·HDFS 운영 문제다.

---

## 8. 겪은 문제와 해결 (운영·도구)

| 문제 | 해결 | 재발 방지 |
|---|---|---|
| Lightsail 보안그룹이 없음, 사설 IP 트래픽은 ufw가 막음 | 양쪽 ufw에 상대 IP TCP 허용 | README에 명시 |
| 도커 브리지 → 호스트 서비스 연결이 ufw에 막힘 | B에 172.17/16, 172.18/16 허용 | `experiment.sh` 시작 시 브리지 컨테이너에서 master 7077 연결 사전 확인 |
| Spark master JSON 경로 | `/json/`(끝 슬래시). `/json`은 리다이렉트만 | `alive_workers()` 수정 |
| 같은 서버 host 네트워크 worker 2개의 셔플 | `readHostLocalDisk=false` | `load-config-cross.sh` 기본 적용 |
| datanode hostname이 컨테이너 ID라 외부에서 해석 불가 | `dfs.datanode.hostname=사설 IP` + 9864·9866 공개 | `hdfs-ports.sh` |
| A datanode를 내리면 namenode가 10.5분 뒤 dead 판정 | 시나리오 순서를 datanode 1 → 2로, 1 시나리오는 dead 대기(최대 12분) | `scenarios.conf` 순서, `wait_for` |
| 복제계수 변경 순서로 B 복제본 삭제 | A 내리기 전 `setrep -w 2`(B 보장) → A 내림 → dead → `setrep 1` | `experiment.sh` |
| Spark 출력 복제계수 3으로 복제 대기열 폭주 | 출력 경로 복제계수 1 | 근본 해결은 Gold·Silver Spark 설정에 `spark.hadoop.dfs.replication=1` (미적용) |
| 실행 중 스크립트 덮어쓰기 | `.new` + `mv` | 운영 규칙 |
| `pkill -f` 패턴이 자기 ssh 셸과 일치해 셸이 죽음 | 패턴을 `^bash /path` 로 앵커 | 운영 규칙 |
| `while read` + ssh의 stdin 소비 | `mapfile` + `< /dev/null` | `run-all.sh` |
| 두 manifest 블록이 `setrep -w`에서 끝나지 않음 | 로컬리티와 무관해 대기 해제, 이후 25분 `timeout` | `experiment.sh` |
| 4 GB 드라이버에서 1000가구 둘째 날 OOM | 6 GB | 운영 기준 |
| 가용 메모리 부족(ConsoleConsumer 잔존) | 종료 | 실행 전 가용 메모리 확인 |
| **(10/2 백업 중 발견) `/nilm/gold` 보고서 파일 12블록 결손** — 10/1 `setrep 1 /nilm/gold`(datanode 2개 상태)로 namenode가 12블록의 B 복제본을 지우고 A 것만 남김 → 보강 회차가 A를 내리자 읽기 불가, HDFS `CORRUPT` | A datanode 재기동 → 해당 보고서 디렉터리 `setrep -w 2`로 B 복제 → 13,798블록 전부 B 보유 확인 | datanode 2개 상태에서 운영 경로의 복제계수를 내리지 않는다. 내려야 하면 먼저 `setrep -w 2`로 B 복제본을 보장하고 A를 내린 뒤에 내린다 |
| **(10/2 발견) `/nilm/gold` 쿼터가 2 GB로 회귀** — 9/26 32 GB로 올렸으나 9/28 배포의 hdfs-init가 `.env`의 `HDFS_GOLD_QUOTA=2g`를 재적용. 사용량 3.7 GB라 Gold 쓰기가 쿼터 초과로 실패하는 상태 | `hdfs dfsadmin -setSpaceQuota 32g /nilm/gold` | Jenkins `ec2-b-runtime-env`의 `HDFS_GOLD_QUOTA`를 32g로 수정해야 다음 배포에서 유지됨 |
| `/nilm/experiments`와 `/nilm/report_inputs` 복제계수 3 출력이 datanode 2개가 되자 대기열 점유 | `setrep 1` | Gold·Silver Spark 설정 `spark.hadoop.dfs.replication=1` (미적용) |

---

## 9. 재현 절차

### 9.1 1부 (EC2-B, root)

```bash
cd /opt/nilm/experiments/load/bin
./load-setup.sh 500 && ./load-write.sh 500                      # Bronze 9/20~9/23
setsid nohup ./cluster-repeat.sh "1 2 3 4" 3 > ../cluster-repeat.log 2>&1 &   # worker 수
BENCH_DATE=2026-09-20 ./bench.sh "10 100 500 1000" "3" "200" 2  # 규모
./bench.sh "500" "1 3" "12" 2                                   # 파티션
./local-repeat.sh 500 2 && ./gold1000.sh                        # local 비교, 1000가구 6 GB
```

### 9.2 2부 (저장소 `experiments/cross-host/README.md` 참고)

```bash
# 방화벽 (각 서버)
sudo ufw allow from <상대 사설 IP> proto tcp comment "cross-host spark temp"
sudo ufw allow from 172.18.0.0/16 comment "cross-host docker bridge"   # B
sudo ufw allow from 172.17.0.0/16 comment "cross-host docker bridge"   # B
# B
bash ~/cross-host/ec2b/setup-ssh-to-a.sh          # 공개키를 A authorized_keys 에 추가 후 ... test
sudo bash ~/cross-host/ec2b/hdfs-ports.sh apply --yes
sudo bash ~/cross-host/ec2b/print-a-hosts.sh > ~/cross-host/ec2a/a.env   # A 로 복사
sudo bash ~/cross-host/ec2b/cluster-cross.sh up 0
# A
bash ~/cross-host/ec2a/check-cross.sh             # 7077 18080 9000 9864 9866 open
sudo bash ~/cross-host/stats-cross.sh start       # (양쪽)
# B
sudo bash ~/cross-host/ec2b/experiment.sh smoke 1 1 1 1
setsid nohup sudo bash ~/cross-host/ec2b/run-all.sh 3 ~/cross-host/ec2b/scenarios.conf > /opt/nilm/experiments/load/run-all.log 2>&1 &
N=1000 setsid nohup sudo bash ~/cross-host/ec2b/run-all.sh 3 <1000 conf> ...
```

### 9.3 집계

```bash
# 1부
awk -F, 'NR>1 && ($4=="0"||$4=="10") {c=substr($2,1,index($2,"r")-1); n[c]++; s[c]+=$6} END{for(k in n) printf "%s runs=%d avg=%.1f\n",k,n[k],s[k]/n[k]}' cluster-repeat-results.csv | sort
awk -F, 'NR>1 && ($6=="0"||$6=="10") {k=$12","$2","$3","$4; n[k]++; s[k]+=$8} END{for(k in n){split(k,a,","); printf "%s N=%s W=%s P=%s runs=%d avg=%.1f\n",a[1],a[2],a[3],a[4],n[k],s[k]/n[k]}}' bench-results.csv | sort -V
# 2부
awk -F, 'NR>1 && ($8=="0"||$8=="10") {k=$2; n[k]++; s[k]+=$10} END{for(k in n) printf "%-20s runs=%d avg=%.1f\n",k,n[k],s[k]/n[k]}' cross-results.csv | sort
```

---

## 10. 결과 파일 위치

| 위치 | 내용 |
|---|---|
| EC2-B `/opt/nilm/experiments/load/cluster-repeat-results.csv` | 1부 분산 실험 16회 |
| EC2-B `/opt/nilm/experiments/load/bench-results.csv` | 1부 규모·튜닝·local 14회 |
| EC2-B `/opt/nilm/experiments/load/cross-results.csv` | 2부 전 회차(실패 포함 42행) |
| EC2-B `/opt/nilm/experiments/load/stats.csv` | 15초 간격 컨테이너 CPU·메모리 (9/26~) |
| EC2-B `/opt/nilm/experiments/cross/stats-ip-172-26-12-162.csv` | 2부 B 쪽 컨테이너 CPU·메모리 |
| EC2-B `/opt/nilm/experiments/load/*.log` | 실험 묶음별 로그(`cluster-repeat*.log`, `bench*.log`, `run-all*.log`, `chain-1000*.log`, `cross-experiment.log` 등) |
| EC2-B `/opt/nilm/experiments/load-<N>-<구성>/<실행ID>/` | `run-dates.csv`, `execution.log`(드라이버 전체 로그), `config.json`, `result.json` |
| EC2-B `/opt/nilm/experiments/load/batch-*.env` | 구성별 Spark·경로 설정 |
| EC2-B `/opt/nilm/experiments/load/bin/` | 1부 스크립트 |
| 두 서버 `/home/ubuntu/cross-host/` | 2부 스크립트 (저장소 `experiments/cross-host/`와 동일) |
| 저장소 `docs/진행상황/이정민/1001/` | 중간 정리(1부·2부 요약) |
| 저장소 `docs/진행상황/이정민/1002/` | 이 문서 |
| 저장소 `docs/진행상황/이정민/1002/data/` | **원본 CSV 사본**: `cluster-repeat-results.csv`, `bench-results.csv`, `cross-results.csv`, `cpu-by-run.csv`, `run-dates/all-run-dates.csv`(115행), `stats-ec2b-15s.csv.gz`, 1부 스크립트 `bin/`, 2부 스크립트 `cross-host/` |
| 로컬 `C:_on_maumackup\experiments.tgz` | 위 전부 + 드라이버 로그 97개, `config.json`, `result.json`, `batch-*.env` (서버 종료 전 10/2 백업) |

EC2-B 자료는 10/2 서버 종료 전에 전부 로컬로 받았다(위 표). 서버는 10/2 17:56 KST 이후 접속 불가.

---

## 11. 남은 과제와 한계

- **입력 확대 실험 미실시.** 하루치만 처리하는 구조라 7일·28일 창의 실제 운영 부하(Gold가 28일치를 읽는 상황)는 재지 않았다. 날짜 연속 처리(회차당 7배)로 가능하다.
- **A 쪽 CPU 미수집.** `stats-cross.sh start`를 A에서 켜지 않아 2부 A worker의 CPU 기록이 없다. B worker만 있다.
- **1부와 2부의 baseline 차이 8%**(브리지 대 host 네트워크·공개 포트). 네트워크 모드만 바꾼 대조 실험은 하지 않았다.
- **단일 노드 데이터 생성 조건.** 실험 데이터는 10초 간격 가상 가구다. 실제 1초 간격 데이터의 파일 수·크기 분포는 다를 수 있다.
- **반복 수.** 1000가구 baseline·spark-dist는 3회, 1부 규모·튜닝은 2회다. 편차가 1~3%라 결론에는 충분하지만 통계 검정을 할 수준은 아니다.
- **Spark 출력 복제계수.** Gold·Silver가 `dfs.replication=3`으로 쓰는 문제는 코드에 반영하지 않았다(`spark.hadoop.dfs.replication=1`). datanode를 2개 이상 운영하면 바로 영향이 있다.
- **서버 정리와 A datanode.** 보고서 시점에 A의 `datanode-a`, B의 HDFS 포트 공개, 양쪽 ufw 임시 규칙이 남아 있다. 정리 절차는 `experiments/cross-host/README.md`.
  단, **`/nilm/experiments`의 실험 출력 10,902블록은 A에만 복제본이 있다.** A datanode를 내리면 그 경로가 `CORRUPT`가 된다(운영 경로 `/nilm/bronze`, `/nilm/gold`, `/nilm/manifests`는 B에 전부 있어 영향 없음). 내리기 전에 해당 실험 출력 디렉터리를 지우거나, `setrep -w 2`로 B에 복제한 뒤 내린다.
- **백업(10/2).** `.env`·인증서·MQTT CA 키·Jenkins 홈·DB 덤프(10/2)·실험 결과·HDFS 운영 데이터(bronze/gold/manifests, 24,195파일)를 `C:_on_maumackup`에 받아 sha256으로 검증했다.

---

## 부록 A. 1부 전체 회차

### A.1 분산 실험 (`cluster-repeat-results.csv`, 500가구, 9/23)

| 시각(UTC) | 구성 | 회차 | exit | 초 | 가용 MB(시작 전) |
|---|---|---:|---:|---|---|
| 09-28 15:56 | w3 | 1 | 10 | 805.435 | 10,980 |
| 09-28 16:11 | w4 | 1 | 10 | 825.332 | 11,006 |
| 09-28 16:25 | w3 | 2 | 10 | 796.670 | 10,986 |
| 09-28 16:40 | w4 | 2 | 10 | 827.282 | 11,017 |
| 09-28 16:54 | w3 | 3 | 10 | 801.651 | 10,985 |
| 09-28 17:09 | w4 | 3 | 10 | 819.519 | 11,013 |
| 09-28 17:23 | w3 | 4 | 10 | 807.625 | 10,990 |
| 09-28 17:38 | w4 | 4 | 10 | 821.724 | 10,973 |
| 09-28 17:52 | w3 | 5 | 10 | 798.581 | 10,990 |
| 09-28 18:07 | w4 | 5 | 10 | 816.701 | 10,994 |
| 09-29 00:31 | w1 | 1 | 10 | 1,179.863 | 10,974 |
| 09-29 00:45 | w2 | 1 | 10 | 812.225 | 10,982 |
| 09-29 01:06 | w1 | 2 | 10 | 1,179.437 | 10,957 |
| 09-29 01:20 | w2 | 2 | 10 | 797.175 | 10,980 |
| 09-29 01:41 | w1 | 3 | 10 | 1,177.807 | 10,968 |
| 09-29 01:55 | w2 | 3 | 10 | 814.487 | 10,980 |

컨테이너·호스트 OOM 전부 0.

### A.2 규모·튜닝·local (`bench-results.csv`)

| 시각(UTC) | 가구 | worker | 파티션 | 회차 | 날짜 | exit | 초 |
|---|---:|---|---:|---:|---|---:|---|
| 09-29 06:55 | 10 | 3 | 200 | 1 | 09-20 | 10 | 223.522 |
| 09-29 07:03 | 100 | 3 | 200 | 1 | 09-20 | 10 | 389.692 |
| 09-29 07:17 | 500 | 3 | 200 | 1 | 09-20 | 10 | 819.987 |
| 09-29 07:42 | 1000 | 3 | 200 | 1 | 09-20 | 10 | 1,402.231 |
| 09-29 07:46 | 10 | 3 | 200 | 2 | 09-20 | 10 | 227.127 |
| 09-29 07:54 | 100 | 3 | 200 | 2 | 09-20 | 10 | 391.800 |
| 09-29 08:08 | 500 | 3 | 200 | 2 | 09-20 | 10 | 828.285 |
| 09-29 08:33 | 1000 | 3 | 200 | 2 | 09-20 | 10 | 1,414.926 |
| 09-29 08:50 | 500 | 1 | 12 | 1 | 09-23 | 10 | 947.049 |
| 09-29 09:01 | 500 | 3 | 12 | 1 | 09-23 | 10 | 646.240 |
| 09-29 09:18 | 500 | 1 | 12 | 2 | 09-23 | 10 | 947.005 |
| 09-29 09:30 | 500 | 3 | 12 | 2 | 09-23 | 10 | 647.828 |
| 09-30 00:22 | 500 | local | 200 | 1 | 09-23 | 10 | 651.033 |
| 09-30 00:34 | 500 | local | 200 | 2 | 09-23 | 10 | 655.409 |

### A.3 사전 실험·연속 실행 (`run-dates.csv`)

| 구성 | 실행 시작(UTC) | 날짜 | exit | 초 |
|---|---|---|---:|---|
| load-10-local (드라이버 3 GB, local[4]) | 09-26 16:51 | 09-14 ~ 09-19 | 10 | 119.8 / 120.3 / 123.6 / 122.3 / 123.6 / 125.5 |
| | | 09-20 ~ 09-23 | 12 | 183.7 / 188.7 / 188.8 / 187.9 |
| load-100-local | 09-26 17:21 | 09-14 ~ 09-17 | 10 | 245.9 / 249.6 / 250.8 / 253.5 |
| load-10-local4 | 09-26 17:41 | 09-20, 21 / 22, 23 | 10 / 12 | 123.0, 124.3 / 185.6, 183.0 |
| load-100-local4 | 09-26 17:51 | 09-20, 21 / 22, 23 | 10 / 12 | 245.1, 250.8 / 332.3, 326.7 |
| load-1000-local4 | 09-26 18:15 | 09-20 / 21, 22, 23 | 10 / **1** | 1,272.3 / 605.8, 607.4, 610.7 (OOM) |
| load-10-local4b (셔플 16) | 09-27 00:26 | 09-22, 23 | 10 | 131.5, 142.6 |
| load-100-local4b | 09-27 00:31 | 09-22 | 10 | 334.8 |
| load-10-local4win2 (창 2) | 09-27 00:43 | 09-22 / 23 | 10 / 12 | 87.1 / 105.3 |
| load-100-local4win2 | 09-27 00:46 | 09-22 / 23 | 10 / 12 | 182.9 / 212.9 |
| load-500-local4win2 | 09-27 00:53 | 09-22 / 23 | 10 / 12 | 581.3 / 707.8 |
| load-500-w1win2 | 09-27 02:55 | 09-22 / 23 | 10 / 12 | 868.3 / 1,013.4 |
| load-500-w2win2 | 09-27 03:27 | 09-22 / 23 | 10 / 12 | 756.4 / 912.7 |
| load-500-w4win2 | 09-27 03:55 | 09-22 / 23 | 10 / **1** | 2,305.1 / 3,039.8 (FetchFailed) |
| load-500-w3win2 | 09-27 05:31 | 09-22 | **1** | 2,295.4 (FetchFailed) |
| load-1000-local6, load-500-local4 (phase2) | 09-27 00:14, 00:28 | - | - | 기록 없음(중단) |
| load-1000-local6g (드라이버 6 GB) | 09-30 00:34 | 09-20, 21 / 22, 23 | 10 / 12 | 1,215.9, 1,228.1 / 1,733.4, 1,715.9 |

## 부록 B. 2부 전체 회차 (`cross-results.csv`)

| 시각(UTC) | 시나리오 | 회차 | dn | B | A | exit | 초 | ALIVE | live DN | 비고 |
|---|---|---:|---:|---:|---:|---:|---|---:|---:|---|
| 09-30 16:21 | smoke | 1 | 1 | 1 | 1 | 1 | 934.394 | 0 | 1 | F4 ufw |
| 09-30 16:47 | smoke | 1 | 1 | 1 | 1 | 10 | 770.542 | 2 | 1 | |
| 09-30 17:10 | baseline | 1 | 1 | 1 | 0 | 10 | 1,283.462 | 1 | 1 | |
| 09-30 17:32 | baseline | 2 | 1 | 1 | 0 | 10 | 1,259.702 | 1 | 1 | |
| 09-30 17:54 | baseline | 3 | 1 | 1 | 0 | 10 | 1,274.532 | 1 | 1 | |
| 09-30 22:59 | worker-up | 1 | 1 | 2 | 0 | 1 | 1,303.532 | 2 | 1 | F5 |
| 09-30 23:22 | worker-up | 2 | 1 | 2 | 0 | 1 | 1,301.661 | 2 | 1 | F5 |
| 09-30 23:46 | worker-up | 3 | 1 | 2 | 0 | 1 | 1,378.146 | 2 | 1 | F5 |
| 09-30 23:59 | spark-dist | 1 | 1 | 1 | 1 | 10 | 687.157 | 2 | 1 | |
| 10-01 00:12 | spark-dist | 2 | 1 | 1 | 1 | 10 | 719.201 | 2 | 1 | |
| 10-01 00:24 | spark-dist | 3 | 1 | 1 | 1 | 10 | 698.820 | 2 | 1 | |
| 10-01 00:40 | worker-up | 1 | 1 | 2 | 0 | 10 | 808.789 | 2 | 1 | 재실행 |
| 10-01 00:54 | worker-up | 2 | 1 | 2 | 0 | 10 | 812.460 | 2 | 1 | |
| 10-01 01:09 | worker-up | 3 | 1 | 2 | 0 | 10 | 803.200 | 2 | 1 | |
| 10-01 02:48 | hdfs-spark | 1 | 2 | 1 | 1 | 10 | 674.439 | 2 | 2 | 입력 복제 45분 대기 뒤(F10) |
| 10-01 03:00 | hdfs-spark | 2 | 2 | 1 | 1 | 10 | 668.068 | 2 | 2 | |
| 10-01 03:12 | hdfs-spark | 3 | 2 | 1 | 1 | 10 | 664.929 | 2 | 2 | |
| 10-01 03:22 | scale | 1 | 2 | 2 | 2 | 10 | 488.084 | 4 | 2 | |
| 10-01 03:32 | scale | 2 | 2 | 2 | 2 | 10 | 494.753 | 4 | 2 | |
| 10-01 03:41 | scale | 3 | 2 | 2 | 2 | 10 | 486.696 | 4 | 2 | |
| 10-01 05:21 | hdfs-spark-1000 | 1 | 2 | 1 | 1 | 10 | 1,199.180 | 2 | 2 | |
| 10-01 05:42 | hdfs-spark-1000 | 2 | 2 | 1 | 1 | 10 | 1,198.664 | 2 | 2 | |
| 10-01 05:55 | spark-dist-1000 | 1 | 1 | 1 | 1 | 1 | 66.532 | 2 | 1 | F6 |
| 10-01 05:57 | spark-dist-1000 | 2 | 1 | 1 | 1 | 1 | 67.298 | 2 | 1 | F6 |
| 10-01 07:19 | hdfs-spark-1000-w7 | 1 | 2 | 1 | 1 | 10 | 1,230.145 | 2 | 2 | 창 7일, 하루치만 처리(6.4절) |
| 10-01 07:43 | spark-dist-1000 | 1 | 1 | 1 | 1 | 1 | 97.327 | 2 | 1 | F7 |
| 10-01 08:06 | spark-dist-1000 | 2 | 1 | 1 | 1 | 10 | 1,272.904 | 2 | 1 | |
| 10-01 08:29 | spark-dist-1000 | 1 | 1 | 1 | 1 | 10 | 1,272.122 | 2 | 1 | |
| 10-01 08:55 | worker-up-1000 | 1 | 1 | 2 | 0 | 10 | 1,455.012 | 2 | 1 | |
| 10-01 09:20 | worker-up-1000 | 2 | 1 | 2 | 0 | 10 | 1,476.116 | 2 | 1 | |
| 10-01 09:46 | worker-up-1000 | 3 | 1 | 2 | 0 | 10 | 1,465.867 | 2 | 1 | |
| 10-01 10:27 | baseline-1000 | 1 | 1 | 1 | 0 | 10 | 2,381.677 | 1 | 1 | |
| 10-01 11:08 | baseline-1000 | 2 | 1 | 1 | 0 | 10 | 2,398.817 | 1 | 1 | |
| 10-01 11:56 | hdfs-spark-1000 | 1 | 2 | 1 | 1 | 10 | 1,240.362 | 2 | 2 | 3회차(보강) |
| 10-01 12:13 | scale-1000 | 1 | 2 | 2 | 2 | 10 | 831.579 | 4 | 2 | |
| 10-01 12:27 | scale-1000 | 2 | 2 | 2 | 2 | 10 | 822.046 | 4 | 2 | |
| 10-01 12:42 | scale-1000 | 3 | 2 | 2 | 2 | 10 | 830.863 | 4 | 2 | |
| 10-01 16:19 | spark-dist-1000 | 1 | 1 | 1 | 1 | 10 | 1,248.799 | 2 | 1 | 3회차(보강) |
| 10-01 17:01 | baseline-1000 | 1 | 1 | 1 | 0 | 10 | 2,350.835 | 1 | 1 | 3회차(보강) |

컨테이너·호스트 OOM 전부 0.

## 부록 C. 회차별 CPU 평균 (B 쪽 컨테이너, `stats.csv`에서 산출)

| 출처 | 구성 | 회차 | 초 | worker CPU 합 | 드라이버 CPU |
|---|---|---:|---|---|---|
| 1부 분산 | w1 r1/r2/r3 | | 1,179.9 / 1,179.4 / 1,177.8 | 111.7 / 108.4 / 108.9 | 8.2 / 6.5 / 12.0 |
| | w2 r1/r2/r3 | | 812.2 / 797.2 / 814.5 | 195.4 / 192.2 / 191.5 | 28.0 / 15.4 / 20.7 |
| | w3 r1~r5 | | 805.4 / 796.7 / 801.7 / 807.6 / 798.6 | 256.5 / 248.5 / 253.1 / 249.3 / 244.6 | 24.4 / 15.1 / 11.3 / 13.5 / 27.5 |
| | w4 r1~r5 | | 825.3 / 827.3 / 819.5 / 821.7 / 816.7 | 286.4 / 273.0 / 266.7 / 281.9 / 262.9 | 19.2 / 21.7 / 26.4 / 26.8 / 21.9 |
| 1부 규모 | N10/100/500/1000 W3 r1 | | 223.5 / 389.7 / 820.0 / 1,402.2 | 224.5 / 242.3 / 259.2 / 269.5 | 56.0 / 34.5 / 19.4 / 11.6 |
| | N10/100/500/1000 W3 r2 | | 227.1 / 391.8 / 828.3 / 1,414.9 | 252.1 / 233.7 / 248.5 / 266.7 | 50.8 / 44.5 / 8.4 / 7.4 |
| 1부 튜닝 | N500 W1 P12 r1/r2 | | 947.0 / 947.0 | 121.2 / 115.6 | 8.0 / 11.9 |
| | N500 W3 P12 r1/r2 | | 646.2 / 647.8 | 256.7 / 273.1 | 18.8 / 26.3 |
| 1부 local | N500 local r1/r2 | | 651.0 / 655.4 | - | 315.1 / 319.2 |
| 2부 500 | baseline r1~r3 | | 1,283.5 / 1,259.7 / 1,274.5 | 108.6 / 102.6 / 108.9 | 11.5 / 7.6 / 6.0 |
| | worker-up r1~r3 | | 808.8 / 812.5 / 803.2 | 210.5 / 210.0 / 209.4 | 27.0 / 20.4 / 17.2 |
| | spark-dist r1~r3 (B 1개) | | 687.2 / 719.2 / 698.8 | 108.8 / 97.5 / 94.5 | 12.5 / 23.8 / 15.5 |
| | hdfs-spark r1~r3 (B 1개) | | 674.4 / 668.1 / 664.9 | 106.8 / 95.1 / 103.3 | 28.8 / 28.1 / 17.7 |
| | scale r1~r3 (B 2개) | | 488.1 / 494.8 / 486.7 | 176.6 / 193.1 / 184.9 | 33.3 / 27.3 / 34.6 |
| 2부 1000 | baseline-1000 r1~r3 | | 2,381.7 / 2,398.8 / 2,350.8 | 108.2 / 106.7 / 111.5 | 2.3 / 7.8 / 7.1 |
| | worker-up-1000 r1~r3 | | 1,455.0 / 1,476.1 / 1,465.9 | 217.0 / 209.0 / 212.8 | 10.6 / 10.1 / 11.3 |
| | spark-dist-1000 (성공 3회, B 1개) | | 1,272.9 / 1,272.1 / 1,248.8 | 98.9 / 101.1 / 106.7 | 8.6 / 8.1 / 15.3 |
| | hdfs-spark-1000 r1~r3 (B 1개) | | 1,199.2 / 1,198.7 / 1,240.4 | 110.8 / 115.4 / 108.8 | 13.8 / 8.8 / 14.7 |
| | scale-1000 r1~r3 (B 2개) | | 831.6 / 822.0 / 830.9 | 204.1 / 188.2 / 211.6 | 10.6 / 18.2 / 13.9 |

2부 교차 구성에서 A worker CPU는 수집되지 않았다. B worker 1개 기준 약 100%(코어 1개를 거의 다 씀), scale의 B 2개는 각 90~105%다. 1부 w4의 worker당 66~74%와 비교하면, 두 서버 구성에서는 worker가 코어를 온전히 쓴다.
