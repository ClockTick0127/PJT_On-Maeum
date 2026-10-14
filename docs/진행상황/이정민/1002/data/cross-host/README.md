# 교차 호스트 분산 실험 (EC2-A + EC2-B)

기존 부하 실험(`/opt/nilm/experiments/load/bin/`)은 EC2-B 한 대의 도커 브리지 안에서
master·worker·드라이버가 컨테이너 이름으로 서로를 찾는다. 이 스크립트들은 EC2-A 에 Spark
worker 와 두 번째 HDFS datanode 를 추가해 **노드가 실제로 나뉜 분산**을 시나리오별로
자동 반복 측정한다. 기존 스크립트는 건드리지 않는다.

## 실험 변수와 기본 시나리오 (`ec2b/scenarios.conf`)

| 이름 | datanode | B worker | A worker | 총 코어 | 보는 것 |
|---|---:|---:|---:|---:|---|
| baseline | 1 | 1 | 0 | 1 | 단일 서버 기준 |
| worker-up | 1 | 2 | 0 | 2 | 같은 서버에서 병렬성 증가 |
| spark-dist | 1 | 1 | 1 | 2 | **총 자원 같음, 배치만 두 서버** (worker-up 과 비교) |
| hdfs-spark | 2 | 1 | 1 | 2 | 저장소까지 두 서버. 입력 Bronze 를 두 노드에 복제해 로컬리티 비교 |
| scale | 2 | 2 | 2 | 4 | 자원 확장 |

기본 3회 반복이면 5 × 3 = 15회. 조건은 기존 분산 실험과 같다(500가구, 9/23 하루, worker 당
코어 1·실행기 1.2 GB, 셔플 파티션 200). `N`, `LOAD_FROM`, `LOAD_TO`, `SHUFFLE` 환경변수로 바꾼다.

## 연결 구조

```text
EC2-B 172.26.12.162                                  EC2-A 172.26.8.92
  spark-master     (host net :7077, ui :18080)  <----  spark-worker-a1.. (host net, --restart)
  spark-worker-b1..(host net)
  load-driver-*    (compose 브리지, :7078 :7079 호스트 공개)  <---- 실행기 접속
  namenode :9000 공개                             <----  datanode-a (host net, hostname=172.26.8.92)
  datanode  hostname=172.26.12.162, :9864 :9866 공개
  nilm-exp-pg      (브리지, 드라이버만 접속)
```

- master·worker 는 양쪽 다 `--network host`, 사설 IP 로 광고. 브리지 컨테이너 이름은 다른
  서버에서 풀 수 없기 때문이다.
- 드라이버는 `spark.driver.host=172.26.12.162`, 포트 7078/7079 고정.
- datanode 는 `dfs.datanode.hostname` 을 자기 사설 IP 로 둔다. 실행기가
  `dfs.client.use.datanode.hostname=true` 로 IP 에 바로 붙고, Spark 로컬리티(블록 위치 IP =
  실행기 IP)도 맞는다.

## 준비

1. **방화벽**: 두 서버 사이 TCP 전체 허용(실행기·셔플 포트가 임의). 이 서버들은 Lightsail 이라
   EC2 보안그룹이 없고 사설 IP 트래픽은 Lightsail 방화벽을 거치지 않는다. 실제 차단은 각 서버의
   ufw(기본 deny incoming)이므로 양쪽에서 상대 사설 IP 를 허용한다.
   ```bash
   # EC2-B
   sudo ufw allow from 172.26.8.92 proto tcp comment "cross-host spark temp"
   # EC2-A
   sudo ufw allow from 172.26.12.162 proto tcp comment "cross-host spark temp"
   # EC2-B 추가: 드라이버(compose 브리지)가 host 네트워크의 master·HDFS 에 붙도록 도커 브리지 대역 허용
   sudo ufw allow from 172.18.0.0/16 comment "cross-host docker bridge"
   sudo ufw allow from 172.17.0.0/16 comment "cross-host docker bridge"
   # 되돌리기: sudo ufw status numbered → sudo ufw delete <번호>
   ```
   도커 브리지 규칙이 없으면 드라이버가 "All masters are unresponsive" 로 실패하고, datanode
   hostname 을 사설 IP 로 바꾼 뒤에는 브리지 안 WebHDFS 클라이언트(bronze-loader)도 datanode 에
   못 붙는다. `experiment.sh` 가 시작 전에 이 연결을 확인한다.
2. **파일 배치**: 저장소 `experiments/cross-host/` 를 두 서버 `/home/ubuntu/cross-host/` 로.
   ```bash
   scp -r experiments/cross-host ec2-b:/home/ubuntu/ && scp -r experiments/cross-host ec2-a:/home/ubuntu/
   ```
3. **EC2-B → EC2-A ssh 키** (오케스트레이터가 A 를 조작한다)
   ```bash
   # EC2-B, ubuntu 계정
   bash /home/ubuntu/cross-host/ec2b/setup-ssh-to-a.sh        # 공개키 출력
   # 출력된 한 줄을 EC2-A 의 /home/ubuntu/.ssh/authorized_keys 에 추가
   bash /home/ubuntu/cross-host/ec2b/setup-ssh-to-a.sh test   # "ssh to EC2-A ok" + "sudo ok"
   ```
4. **HDFS 포트 공개** (EC2-B, Gold daily·다른 실험이 없을 때. namenode/datanode 재생성)
   ```bash
   sudo bash /home/ubuntu/cross-host/ec2b/hdfs-ports.sh apply --yes
   ```
5. **EC2-A 가 쓸 값** (이미지 digest)
   ```bash
   sudo bash /home/ubuntu/cross-host/ec2b/print-a-hosts.sh > /home/ubuntu/cross-host/ec2a/a.env
   scp ec2-b:/home/ubuntu/cross-host/ec2a/a.env /tmp/a.env && scp /tmp/a.env ec2-a:/home/ubuntu/cross-host/ec2a/a.env
   ```
6. **연결 확인** (EC2-A)
   ```bash
   sudo bash /home/ubuntu/cross-host/ec2b/cluster-cross.sh up 0     # EC2-B 에서 master 만
   bash /home/ubuntu/cross-host/ec2a/check-cross.sh                  # 7077 18080 9000 9864 9866 open
   ```

## 실행

```bash
# 자원 기록 시작 (양쪽 서버 각각)
sudo bash /home/ubuntu/cross-host/stats-cross.sh start

# 연결 검증용 1회 (EC2-B)
sudo bash /home/ubuntu/cross-host/ec2b/experiment.sh smoke 1 1 1 1

# 전체 시나리오 × 3회 (EC2-B, 백그라운드)
setsid nohup sudo bash /home/ubuntu/cross-host/ec2b/run-all.sh 3 > /opt/nilm/experiments/load/run-all.log 2>&1 &
tail -f /opt/nilm/experiments/load/cross-experiment.log

# 시나리오 하나만
sudo bash /home/ubuntu/cross-host/ec2b/experiment.sh spark-dist 1 1 1 3
```

`experiment.sh` 가 회차마다 하는 일: 이전 컨테이너 정리 → datanode 수 맞추기(2개면 입력을
setrep 2 로 복제) → master·B worker 기동, A worker 는 ssh 로 기동 → 기대 worker 가 모두 ALIVE
될 때까지 대기(최대 3분) → 새 DB·HDFS 출력 경로로 `gold-profile daily` 실행 → 시간·OOM·ALIVE
worker·live datanode 기록 → 정리.

## 결과

| 파일 | 내용 |
|---|---|
| `/opt/nilm/experiments/load/cross-results.csv` | `time,scenario,run,datanodes,b_workers,a_workers,node_distribution,exit,status,time_sec,alive_workers,live_datanodes,container_oom_b,host_oom_b,avail_before_mb,config` |
| `/opt/nilm/experiments/load/cross-experiment.log` | 회차별 시작·종료 로그와 드라이버 출력 |
| `/opt/nilm/experiments/load-500-<config>/…/run-dates.csv` | 날짜별 실행 기록. config = `w<총>x<이름>d<dn>b<B>a<A>r<회차>` |
| `/opt/nilm/experiments/cross/stats-<hostname>.csv` (양쪽) | 실험 컨테이너 CPU·메모리 15초 간격 |

`run-all.sh` 끝에 시나리오별 평균·최소·최대가 출력된다. 직접 보려면:

```bash
awk -F, 'NR>1 && ($8=="0"||$8=="10") {k=$2; n[k]++; s[k]+=$10} END{for(k in n) printf "%s avg=%.1fs (n=%d)\n", k, s[k]/n[k], n[k]}' /opt/nilm/experiments/load/cross-results.csv
```

exit 10 은 실험 창(3일)이 아직 안 찬 날짜, 0 은 완료. 둘 다 계산은 끝까지 돌았다.

## 정리

```bash
sudo bash /home/ubuntu/cross-host/stats-cross.sh stop                 # 양쪽
sudo bash /home/ubuntu/cross-host/ec2a/worker-cross.sh down           # EC2-A
sudo bash /home/ubuntu/cross-host/ec2a/datanode-cross.sh down         # EC2-A (볼륨은 남음)
sudo bash /home/ubuntu/cross-host/ec2b/cluster-cross.sh down          # EC2-B
sudo bash /home/ubuntu/cross-host/ec2b/hdfs-ports.sh revert --yes     # EC2-B, 선택
```

## 주의

- **datanode 1개 시나리오를 먼저, 2개를 나중에.** A datanode 를 내리면 namenode 가 dead 로
  판정하기까지 약 10.5분 걸리고, 그 노드에만 있던 블록(실험 출력)은 그동안 못 읽는다.
  `experiment.sh` 는 1개 시나리오에서 live datanode 가 1이 될 때까지 최대 12분 기다린다.
- `hdfs-ports.sh apply` 는 namenode·datanode 를 다시 만든다(HDFS 수십 초 중단). 다음 Jenkins
  배포가 compose.yaml 을 덮어쓰면 원래대로 돌아간다.
- EC2-A 는 모니터링·Keycloak·Jenkins 가 떠 있다(가용 약 10 GB). worker 2개 + datanode 1개까지.
- 공정 비교: `worker-up`(B 2) 과 `spark-dist`(B 1 + A 1) 는 총 코어가 같다. 이 둘의 차이가
  순수한 서버 분산 비용이다. `scale` 은 자원이 2배라 속도 향상에 자원 효과가 섞인다.
- 이 스크립트가 지우는 것은 자기가 만든 컨테이너와 회차별 실험 DB·출력 경로뿐이다.
  결과 CSV, 입력 Bronze, 운영 데이터는 지우지 않는다.
