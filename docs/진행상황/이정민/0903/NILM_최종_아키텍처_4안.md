# NILM 최종 아키텍처 (축소 3안): Edge / Stream·Data 분리 + EC2 Spark 최소화 + 노트북 HDFS 클러스터

> 선행 문서: [NILM_최종_아키텍처_3안.md](NILM_최종_아키텍처_3안.md), [NILM_1-3안_비교_요약.md](NILM_1-3안_비교_요약.md), [NILM_AIHub_전처리_저장소_전략.md](NILM_AIHub_전처리_저장소_전략.md)
> 자원: EC2 2대(4 vCPU·15GiB·309GB, 비버스터블 계열) + 온프레미스 GPU 서버 + **노트북 6대(외장 SSD 포함)** + 보조 EC2(1GB). 장기 저장소는 S3. AWS 콘솔 없이 SSH만 가능.
> 3안 대비 변경 요지: AI Hub 학습 전처리를 EC2에서 노트북 6대 Spark + HDFS 클러스터로 이동. EC2 Spark는 운영 수집 데이터 전처리 파이프라인만 맡는 최소 구성(A 단일 노드)으로 축소. B에서 Spark 완전 제거.

## 1. 결론

> **EC2-A는 Edge·서비스 계층 + 소규모 Spark(운영 데이터 전처리 전용), EC2-B는 스트림·데이터 계층(Spark 없음), S3는 운영 데이터 레이크·모델 저장소, 노트북 6대는 AI Hub 학습 전처리 클러스터(Spark Standalone + HDFS), 온프레미스 GPU는 학습 계층으로 분리한다.**

3안이 "Spark가 클라우드 위에서 물리 2노드로 분산됨"을 EC2로 보이려던 것을, 이 문서는 두 갈래로 나눈다.

- **대용량 일회성 학습 전처리(AI Hub 300GB)** 는 노트북 6대 클러스터가 맡는다. 물리 다노드 분산·셔플·워커 장애 복구 시연은 여기서 한다.
- **운영 중 수집되는 센서 데이터의 전처리(S3 Bronze → Silver → 재학습용 Gold)** 는 EC2-A의 소규모 Spark가 맡는다. "클라우드 위 Spark 전처리 파이프라인"은 이것으로 보인다.

Spark의 역할 범위는 3안과 같다(학습 전처리만. 실시간 판정은 Flink, 학습은 GPU). 바뀐 것은 **어디서 돌리느냐**이고, 결과적으로 EC2 위 Spark는 3안(A Worker 2 + B Master·Worker)에서 A 단일 노드(Master + Worker 1)로 줄어든다.

핵심 원칙 다섯 가지:

1. 실시간 경로(`MQTT → Kafka → Flink → Monitoring`)가 도는 EC2-B에는 Spark 프로세스가 없다. 배치 간섭은 A 한 대 안에서만 관리한다.
2. EC2 Spark는 운영 수집 데이터만 입력으로 받는다. AI Hub 데이터는 EC2를 거치지 않는다.
3. Kafka는 6~24시간 버퍼이고, 운영 데이터 재처리 기준은 S3 Bronze다. AI Hub 학습 데이터의 기준 저장소는 노트북 HDFS다.
4. AI Hub 원천은 노트북 외장 디스크에서 불필요 채널·기간을 제거해 약 300GB로 줄인 뒤 HDFS에 적재한다. 제거 기준은 §5.3에 기록한다.
5. HA(고가용성)는 아니다. Kafka·PostgreSQL·Flink·HDFS NameNode·Spark Master가 각각 단일 인스턴스임을 명시하고 로드맵 4단계로 이연한다.

## 2. 전체 구조

```text
[가구별 ESP32 + 클램프 센서]
  └─ 10Hz 측정, 1초 단위 마이크로배치
          │ MQTT/TLS 8883, QoS 1
          ▼
┌──────────────────────────────────────────────────────────┐
│ EC2-A: Edge / Service (+ 소규모 Spark)                    │
│  Nginx + React Dashboard                                  │
│  API Gateway (JWT 검증) · Keycloak (인증)                  │
│  IoT Device Service · Monitoring Service (SSE)            │
│  Mosquitto (MQTT Broker) · Redis (최신 상태 Projection)    │
│  ── Spark Master + Worker ×1 (단일 노드, 야간 Job만) ──    │
│  ── Bronze Compaction cron (Python, MVP 규모) ──          │
└───────────────┬───────────────────────────┬──────────────┘
                │ MQTT 구독                   │ analysis.* 소비, PostgreSQL 쓰기
                ▼                            ▲
┌──────────────────────────────────────────────────────────┐
│ EC2-B: Stream / Data (Spark 없음)                          │
│  MQTT–Kafka Bridge (Spring)                               │
│  Kafka (KRaft 단일 브로커, 파티션 24, key=household_id)    │
│  Flink JobManager + TaskManager (슬롯 2)                   │
│    변화점 탐지 · 가전 상태 판정 · 루틴 비교 · 부재 타이머     │
│  PostgreSQL (keycloak·device·analysis·monitoring DB)      │
│  Raw Archive Sink → S3 Bronze                             │
└──────┬──────────────────────────────────────┬─────────────┘
       │ analysis.snapshot.v1 / analysis.event.v1│ 원천 Parquet (1~5분 묶음)
       ▼                                        ▼
  Monitoring(A) → Redis · PG · SSE      ┌──────────────────────────────────┐
  → React Dashboard · 알림              │ S3 (서울): 운영 데이터 레이크      │
                                        │  bronze/  power_raw (센서 원천)   │
                                        │  silver/  power_samples           │
                                        │  gold/    retrain_windows         │
                                        │  artifacts/ models, flink ckpt    │
                                        └───────┬────────────────┬──────────┘
                                                │ s3a://          │ 모델 등록 / Flink 로드
                                                ▼                 ▲
                              ┌────────────────────────┐          │
                              │ Spark on EC2-A (단일)   │          │
                              │ Bronze→Silver Compaction│          │
                              │ 운영 Gold · 기준선 갱신  │          │
                              └────────────────────────┘          │
                                                                  │
┌─────────────────────────────────────────────┐   ┌──────────────┴─────────────┐
│ 노트북 클러스터 ×6 (온프레미스 LAN)           │   │ 온프레미스 GPU 서버          │
│  N1: HDFS NameNode + Spark Master (+Worker)  │──▶│ PyTorch 학습·검증            │
│  N1~N6: HDFS DataNode + Spark Worker         │   │ leave-house-out 평가         │
│  hdfs://silver/aihub_10hz (≈300GB, 복제 2)   │   │ 모델 아티팩트 → S3 artifacts │
│  hdfs://gold/training_windows                │   └────────────────────────────┘
│  1차 변환: 외장 SSD ZIP → 채널·기간 필터      │
│            → 10Hz Parquet → HDFS put          │
└─────────────────────────────────────────────┘
```

## 3. 서버별 구성요소와 역할

| 위치 | 구성요소 | 맡는 이유 | 두지 않는 것 |
| --- | --- | --- | --- |
| **EC2-A** | Nginx·React, API Gateway, Keycloak, IoT Device, Monitoring(SSE), Mosquitto, Redis, **Spark Master + Worker ×1(단일 노드)**, Compaction cron | 외부 진입점. 평시 여유 8GB 이상으로 야간 Spark를 받을 수 있고, 페이지 캐시·fsync에 민감한 프로세스가 없어 배치 간섭이 B보다 작다 | Kafka, PostgreSQL, Flink, AI Hub 데이터 |
| **EC2-B** | Bridge, Kafka, Flink JM·TM, PostgreSQL, Archive Sink | 메시지 버퍼·판정·영구 저장을 한 장애 도메인으로. **Spark 제거로 실시간 경로가 배치와 자원을 나누지 않는다** | Spark 일체, 외부 공개 포트 |
| **S3** | bronze / silver / gold / artifacts | 운영 데이터 재처리 기준. Flink Checkpoint, 학습된 모델. EC2와 같은 리전이라 전송 무료 | AI Hub Silver 전량(HDFS에 둔다), 실시간 화면 직접 조회 |
| **노트북 클러스터 ×6** | HDFS(NameNode 1 + DataNode 6), Spark Standalone(Master 1 + Worker 6), 1차 변환 Python 풀, 리플레이 Producer·채점기 | AI Hub 원천 521GB ZIP은 EC2 디스크에 들어가지 않고, 6대 합산 코어가 EC2 4 vCPU보다 압도적. 데이터 옆에서 Spark가 돈다(지역성) | 상시 서비스, 실시간 경로 |
| **온프레미스 GPU** | PyTorch 학습·평가, 모델 export | GPU가 필요한 유일한 작업. HDFS Gold를 로컬로 받아 학습 | 실시간 추론 경로, 원천 DB |
| **보조 EC2 (1GB)** | WireGuard·점프 호스트 | 메모리 1GB로 다른 것은 불가 | Kafka 브로커, DB, Spark, Redis |

## 4. 데이터 흐름

### 4.1 실시간 전력 이벤트 (초~수 초)

```text
ESP32 → Mosquitto(A) → Bridge(B) → Kafka power.raw.v1 (key=household_id)
  → Flink(B): keyBy(household) → 스키마 검증·중복 제거 → 윈도우 → 변화점 탐지
             → 가전 상태 판정(히스테리시스) → 기준선 비교 → 부재 타이머(ROUTINE_MISSED)
  → Kafka analysis.snapshot.v1 / analysis.event.v1
  → Monitoring(A): Redis 최신 상태 · PostgreSQL(B) 이벤트·사건 · Outbox → SSE → React
```

3안과 동일. Flink Checkpoint 30초, `s3a://nilm-artifacts-dev/flink/checkpoints`. Monitoring은 `read_committed` + `event_id` Unique + Outbox로 멱등.

### 4.2 운영 수집 데이터 전처리 (EC2 Spark, 분~일)

```text
Kafka power.raw.v1 → Archive Sink(B) → S3 bronze/power_raw/dt=/hour=/bucket=   (1~5분 묶음)
S3 Bronze → Spark(A, 야간) → S3 silver/power_samples (Compaction 128~512MB)
S3 Silver → Spark(A) → S3 gold/retrain_windows (운영 가구 재학습용), PostgreSQL routine_baseline 갱신
S3 Bronze → Replay Producer(노트북) → Kafka power.replay.v1 → 같은 Flink Job → analysis_version 분리
```

- EC2 Spark의 입력은 S3 Bronze/Silver만이다. 이 경로가 "클라우드 위 Spark 전처리 파이프라인"이다.
- MVP 11가구 규모에서 Bronze→Silver Compaction은 A의 Python cron(pyarrow)으로 충분하다. Spark Job은 가구 수 증가 시, 그리고 완료 기준 4·5번 시연 시 사용한다.

### 4.3 AI Hub 학습 전처리 (노트북 클러스터, 시간~일)

```text
외장 SSD: TS.z01~z05 + TS.zip (521GB) + VS.zip (68GB)
  → N1~N6 분담: 7-Zip 스트리밍 → 채널·기간 필터(§5.3) → 10Hz Parquet zstd
  → hdfs://silver/aihub_10hz/house_id=/channel=/dt=      (≈300GB, 복제 2 → 디스크 600GB)
  → hdfs://silver/aihub_events (JSON 라벨)
  → Spark(클러스터) job2: 라벨 조인 · is_active 타임라인 · 세션 병합 → silver/aihub_labeled
  → Spark(클러스터) job3: 학습 윈도우 슬라이싱 · leave-house-out 분할 · Compaction → hdfs://gold/training_windows/dataset_version=
  → Spark(클러스터) job4: 앞 20일 라벨 → 루틴 기준선 → PostgreSQL(B) routine_baseline, Kafka baseline.v1 (VPN 경유)
  → GPU 서버: hdfs dfs -get gold/training_windows → 학습 → 모델 → S3 artifacts/models/
  → Flink Job이 S3에서 버전 지정 로드
```

- Silver·Gold의 기준 저장소는 HDFS다. S3에는 올리지 않는다(300GB 업로드 10~14시간과 보관비를 피한다). 필요 시 Gold만 S3 `gold/training_windows_snapshot/`에 사본을 둔다.
- 1차 변환은 파일 `(house, channel, date)` 단위 독립 작업이므로 Spark 없이 노트북 6대의 Python 풀로 분담한다. 6대 합산 60~80코어 기준 1시간 안팎.

### 4.4 서비스·대응 피드백 (요청 즉시)

```text
React → API Gateway(A) → Keycloak(JWT) / IoT Device(기기 등록·MQTT ACL) / Monitoring(조회·확인·조치)
  → PostgreSQL(B) device_db · monitoring_db, Redis(A) 최신 상태
```

### 4.5 Kafka 토픽

| 토픽 | 파티션 | Producer | Consumer |
| --- | ---: | --- | --- |
| `power.raw.v1` | 24 | Bridge | Flink, Archive Sink |
| `analysis.snapshot.v1` | 12 | Flink | Monitoring |
| `analysis.event.v1` | 12 | Flink | Monitoring, (선택) Archive Sink |
| `power.replay.v1` | 24 | Replay Producer(노트북) | Flink |
| `baseline.v1` | 3 | Spark job4(노트북 클러스터 또는 A) | Flink (Broadcast State) |
| `dlq.*` | 3 | 각 Consumer | 운영 도구 |

## 5. Spark 배치 전략

### 5.1 EC2-A 단일 노드 Spark (운영 데이터 전처리)

| 역할 | 위치 | 크기 | 상시 여부 |
| --- | --- | --- | --- |
| Master | EC2-A | 0.5GB | 상시 (제어면만) |
| Worker ×1 | EC2-A | Executor 2g + overhead → 2.4GB, 1코어 × 최대 2 Executor | 데몬 상시(0.1GB), Executor는 Job 때만 |
| Driver | A (`--deploy-mode client`) | 1.0GB | Job 때만 |

```bash
# A 단일 노드. Master·Worker·Driver 모두 A. B와의 네트워크 의존 없음
spark-submit --master spark://<A 사설 IP>:7077 --deploy-mode client \
  --executor-memory 2g --executor-cores 1 --conf spark.cores.max=2 \
  --driver-memory 1g \
  --packages org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.hadoop.fs.s3a.committer.name=magic \
  /opt/nilm/jobs/job7_bronze_compaction.py --date 2026-09-02
```

- Worker 1개에 코어 2개를 주면 Spark UI Executors 탭에 논리 Executor 2개가 뜬다. 물리 다노드 시연은 노트북 클러스터가 맡으므로 EC2에서는 이것으로 충분하다.
- `spark.local.dir=/data/spark-scratch`, Worker 컨테이너 `--cpus=2`, `ionice -c3`. 3안의 A 규칙을 그대로 적용한다.
- 3안의 B Worker, A+B 시연 배치, `spreadOut` 조정, A↔B Spark 포트 개방 확인은 모두 삭제한다.

### 5.2 노트북 6대 클러스터 (AI Hub 학습 전처리)

| 역할 | 위치 | 크기(노트북당) | 비고 |
| --- | --- | --- | --- |
| HDFS NameNode | N1 | 힙 2GB | 단일. 메타데이터 백업은 외장 SSD에 매일 `hdfs dfsadmin -fetchImage` |
| HDFS DataNode ×6 | N1~N6 | 힙 1GB + 외장 SSD 데이터 디렉터리 | 복제 계수 2(가정). 300GB × 2 ÷ 6 ≈ 노트북당 100GB |
| Spark Master | N1 | 1GB | NameNode와 동거 |
| Spark Worker ×6 | N1~N6 | Executor 4g × 2~3, 코어 6~8 | 노트북 사양에 따라 상이. 최소 사양 노트북 기준으로 Executor 크기를 통일 |
| Driver | N1 (`client` 모드) | 2GB | |

```bash
# 노트북 클러스터. 입력·출력 모두 HDFS
spark-submit --master spark://N1:7077 --deploy-mode client \
  --executor-memory 4g --executor-cores 2 --conf spark.cores.max=36 \
  --driver-memory 2g \
  hdfs://N1:9000/jobs/job3_training_windows.py \
  --input hdfs://N1:9000/silver/aihub_labeled --output hdfs://N1:9000/gold/training_windows --dataset-version v1
```

- 6대는 같은 유선 LAN(기가비트 이상)에 둔다. Wi-Fi는 HDFS 복제 쓰기와 Shuffle에서 병목이 되므로 시연 리허설 전에 유선을 확보한다.
- 워커 장애 복구 시연: Job 중 N4 Worker를 종료 → 태스크가 다른 5대로 재스케줄 → 완료. HDFS 복제 2라 DataNode 1대 손실은 읽기에 영향 없다.
- Spark UI Executors 탭에 호스트 6개, Stages 탭에 Shuffle Read Remote > 0이 찍힌다. 완료 기준 4번의 물리 분산 증거는 이 화면이다.

### 5.3 AI Hub 데이터 축소 기준 (521GB ZIP → 300GB Silver)

전처리 전략 문서 기준 10Hz 11컬럼 전량은 400~600GB다. 300GB로 줄이기 위해 다음 순서로 제거한다. 실제 적용 기준은 변환 manifest에 남긴다.

| 단계 | 제거 대상 | 근거 |
| --- | --- | --- |
| 1 | MVP 판정 대상이 아닌 가전 채널 | 시그니처 모델 학습에 쓰지 않는 채널. `ch01`(메인)과 대상 가전 채널만 유지 |
| 2 | 라벨 이벤트가 0건인 가구·날짜 | `is_active` 타임라인이 전부 0이라 학습 표본 가치가 낮음 |
| 3 | 결측률 상위 날짜 | manifest의 NaN 카운트 기준 |
| 4 | 컬럼 축소 | 11컬럼 중 `ts, active_power` 외 학습에 쓰지 않는 컬럼은 Gold 단계에서 제외 |

원천 ZIP은 외장 SSD에 그대로 보존한다(Bronze 역할). 제거한 채널·기간이 나중에 필요하면 ZIP에서 다시 뽑는다.

### 5.4 Spark가 맡는 Job

| Job | 입력 | 출력 | 주기 | 실행 위치 |
| --- | --- | --- | --- | --- |
| job2 라벨 조인·`is_active` | HDFS Silver 10Hz + AI Hub 이벤트 | HDFS Silver labeled | Silver 적재 후 1회, 증분 | **노트북 클러스터** |
| job3 학습 윈도우·leave-house-out·Compaction | HDFS Silver labeled | HDFS Gold `training_windows/dataset_version=` | 모델 실험마다 | **노트북 클러스터** |
| job4 루틴 기준선 | 앞 20일 라벨 | PostgreSQL(B) `routine_baseline`, Kafka `baseline.v1` | MVP 1회 | 노트북 클러스터(VPN 경유) 또는 A |
| job7 Bronze Compaction | S3 bronze | S3 silver/power_samples | 야간 매일 | **EC2-A**. MVP 규모는 Python cron, 시연·가구 증가 시 Spark |
| job8 운영 재학습 윈도우 | S3 silver/power_samples + 운영 라벨(사용자 확인 이벤트) | S3 gold/retrain_windows | 주 1회 | **EC2-A** |

Spark를 쓰지 않는 것: ZIP 해제·JSON 파싱(노트북 Python 풀), 리플레이 Producer·채점기(노트북), 실시간 판정(Flink), 학습(GPU).

### 5.5 실행 시간대

| 시간 | EC2-A Spark | 노트북 클러스터 |
| --- | --- | --- |
| 02:00~05:00 | job7·job8 | 제약 없음 |
| 주간 | Executor 없음. 완료 기준 1·3번 측정 | 제약 없음 (EC2와 무관) |
| 시연 슬롯 | job7 소규모 실행으로 클라우드 Spark UI 표시 | job3 실행 + N4 종료 재스케줄 시연 |

노트북 클러스터는 EC2 실시간 경로와 자원을 전혀 나누지 않으므로 시간대 제약이 없다. 이것이 3안 대비 가장 큰 운영상 이득이다.

## 6. 메모리 예산

### EC2-A (15GiB)

| 컴포넌트 | 할당 | 근거 |
| --- | ---: | --- |
| OS·Docker | 1.5GB | |
| Nginx + React | 0.3GB | |
| API Gateway | 0.8GB | 힙 512MB × 1.6 |
| Keycloak | 1.2GB | 실측 우선 항목 |
| IoT Device | 0.6GB | |
| Monitoring | 0.8GB | 힙 512MB + Kafka fetch + SSE |
| Mosquitto | 0.2GB | |
| Redis | 0.5GB | maxmemory 400MB |
| Compaction cron | 0.3GB | |
| Spark Master | 0.5GB | 상시 (3안에서는 B에 있었음) |
| Spark Worker 데몬 | 0.1GB | 상시 |
| **평시 소계** | **6.8GB** | 여유 8.2GB |
| Spark Executor ×2 (2g + 384MB) | 4.8GB | Job 중 |
| Spark Driver | 1.0GB | Job 중 |
| **배치 중 합계** | **12.6GB** | **여유 2.4GB** |

### EC2-B (15GiB)

| 컴포넌트 | 할당 | 근거 |
| --- | ---: | --- |
| OS·Docker | 1.5GB | |
| Bridge | 0.6GB | |
| Kafka (KRaft) | 2.0GB | 힙 1GB + 페이지 캐시 1GB |
| Flink JobManager | 1.0GB | |
| Flink TaskManager | 2.5GB | 슬롯 2, 1,000가구 상태 약 530MB |
| PostgreSQL | 1.5GB | |
| Archive Sink | 0.5GB | |
| **합계** | **9.6GB** | **여유 5.4GB, 상시 동일** (3안 평시 10.2GB → Spark 0.6GB 제거) |

B는 배치 시간대에도 변동이 없다. 여유 5.4GB는 Flink TaskManager 실측 초과분과 향후 Kafka 페이지 캐시 확대에 쓴다.

## 7. 운영 규칙

| 항목 | 규칙 |
| --- | --- |
| CPU 격리 (A) | Spark Worker 컨테이너 `--cpus=2`. Gateway·SSE에 2 vCPU 보장 |
| 디스크 (A) | Executor `ionice -c3`, `spark.local.dir=/data/spark-scratch` |
| 포트·방화벽 | A의 Spark 7077·8080·8081은 localhost 또는 사설 IP 바인딩 + UFW. A 공개 포트는 443·8883만. **A↔B 사이 Spark 포트 불필요** |
| 노트북 클러스터 네트워크 | 유선 LAN, 고정 IP 또는 hosts 파일. HDFS 9000·9870, Spark 7077·8080·4040은 LAN 내부만 |
| 노트북 ↔ EC2 | WireGuard(보조 EC2 경유)로 job4의 PostgreSQL·Kafka 쓰기, 리플레이 Producer의 Kafka 쓰기만 허용 |
| HDFS 보호 | 복제 2, NameNode fsimage 매일 외장 SSD 백업, 원천 ZIP 보존 |
| 컨테이너 상한 | 모든 컨테이너 `mem_limit`, JVM `-XX:MaxRAMPercentage=65` |
| 측정 | A 배치 없는 시간과 배치 중 두 번 측정: 완료 기준 1번 p95, Consumer Lag, Checkpoint 시간. **B는 배치 영향이 없어야 하므로 두 값이 같음을 확인** |
| S3 자격 | Bronze 쓰기(B Sink), Silver/Gold 읽기·쓰기(A Spark), artifacts 쓰기(GPU 서버)·읽기(Flink) |
| 배포 단위 | A Compose(edge + spark-master·worker), B Compose(data). 노트북은 Ansible 또는 공통 스크립트로 HDFS·Spark 배포. Flink Job은 Application 모드 이미지 |

## 8. 완료 기준 매핑

| 완료 기준 | 충족 방법 |
| --- | --- |
| 1. 포트 ON 후 5초 이내 표시 | Flink → Kafka → Monitoring → SSE. B에 Spark가 없어 배치 시간대 영향이 3안보다 작다 |
| 2. 30회 ON/OFF 80% | Flink 판정 규칙·시그니처. 인프라 무관 |
| 3. 110msg/s 유실 0 | Kafka 단일 브로커 + Flink Checkpoint + Archive Sink 재시도. 리플레이 Producer는 노트북 |
| 4. Spark UI 2워커 분산 | **주: 노트북 클러스터 6워커(물리), Remote Shuffle > 0, N4 종료 후 재스케줄.** 보조: EC2-A 논리 Executor 2개로 클라우드 Spark 파이프라인 표시 |
| 5. Parquet → Spark → 학습용 Parquet | job2·job3(HDFS Silver → HDFS Gold). 운영 경로는 job7·job8(S3 Silver → S3 Gold) |
| 6. 기준 시각까지 미사용 알림 | Flink 부재 타이머 → `ROUTINE_MISSED` → Monitoring 알림 |
| 7. 처리량·지연·정확도·오탐 기록 | 채점기(노트북) + §7 측정 |

## 9. 장애 시 영향

| 장애 | 영향 | 복구 |
| --- | --- | --- |
| A 다운 | MQTT 입구·화면·운영 Spark 중단. Kafka 잔여 데이터는 Flink가 계속 처리, PG 저장 계속 | Compose 재기동. 센서는 Ring Buffer로 재전송 |
| B 다운 | Bridge 이후 전부 중단(Kafka·Flink·PG). **A Spark는 영향 없음**(3안에서는 Master 소실로 중단) | Compose 재기동. Flink는 S3 Checkpoint에서 복원 |
| S3 불가 | Archive Sink 재시도(Kafka 보관 기간 내), Checkpoint 실패(처리는 계속), A Spark 입력 불가 | 자격·네트워크 복구 후 자동 재시도 |
| A Spark Executor OOM | 해당 컨테이너만 종료 | 예산 준수 시 다른 서비스 무영향 |
| 노트북 N1(NameNode·Master) 다운 | AI Hub 전처리 전체 중단. **EC2 실시간·운영 경로 무영향** | 재부팅 후 fsimage 로드. 손상 시 외장 SSD 백업 fsimage + 원천 ZIP에서 재적재 |
| 노트북 N2~N6 중 1대 다운 | 실행 중 태스크 재스케줄. 복제 2로 데이터 손실 없음 | 복귀 시 DataNode 재합류, 복제 자동 복구 |
| 노트북 2대 동시 다운 | 복제 2라 일부 블록 손실 가능 | 원천 ZIP에서 해당 파티션 재변환 |
| GPU 다운 | 학습만 중단 | 기존 모델로 판정 지속 |

## 10. 확장 경로

1. **Kafka 2브로커**: A에 Broker 2(2GB) 추가. A 배치 중 여유 2.4GB이므로 Executor를 1개로 줄이거나 Spark를 야간에만 두는 조건. 컨트롤러 3노드 쿼럼은 보조 EC2 포함 시에만.
2. **운영 Spark 분리**: 가구 수 증가로 job7·job8이 무거워지면 EMR Serverless로 이동. A Spark 제거.
3. **관리형 전환**: Kafka → MSK Serverless, PostgreSQL → RDS, 이후 EC2-B는 Flink 전용.
4. **HA**: Kafka 3브로커, PG Primary/Standby, API 2대 + LB, HDFS NameNode HA(JournalNode 3). 실서비스 전 필수.

## 11. 3안 대비 변경 요약

| 항목 | 3안 | 축소 3안 |
| --- | --- | --- |
| Spark Master | EC2-B | EC2-A |
| EC2 Spark Worker | A ×2 + B ×1 | A ×1 (논리 Executor 2) |
| B의 Spark | Master·Worker 상시 | 없음 |
| A+B 시연 배치 | 있음 | 없음 |
| AI Hub Silver·Gold 저장소 | S3 | 노트북 HDFS |
| AI Hub 전처리 Spark | EC2 | 노트북 6대 클러스터 |
| AI Hub Silver 크기 | 400~600GB | 약 300GB (채널·기간 필터) |
| 노트북 역할 | 1대, 1차 변환·S3 업로드 | 6대, 1차 변환 + HDFS + Spark 클러스터 |
| EC2 Spark 입력 | S3 Silver(AI Hub) + Bronze | S3 Bronze/Silver(운영 데이터)만 |
| 완료 기준 4번 증거 | EC2 A+B 물리 2노드 | 노트북 6노드(주) + EC2-A 논리 2 Executor(보조) |
| B 평시 메모리 | 10.2GB | 9.6GB |
| A↔B Spark 포트 확인 | 필요 | 불필요 |
| S3 보관 용량 | Bronze + Silver·Gold 400~600GB | Bronze + 운영 Silver·Gold + artifacts (수십 GB) |

## 12. 남은 확인 사항

1. **HDFS 복제 계수**: 이 문서는 2로 가정(노트북당 약 100GB). 3이면 노트북당 150GB. 각 노트북 외장 SSD 여유 용량 확인.
2. **NameNode·Spark Master 노트북(N1) 선정**: 가장 안정적으로 켜져 있을 수 있는 1대. 절전·재부팅 정책 해제 필요.
3. **노트북 6대 사양 편차**: 최소 사양 기준으로 Executor 크기를 정한다. 각 대의 코어·메모리·SSD 용량 목록.
4. **유선 LAN 확보 여부**: 시연 장소에서 6대 + GPU 서버가 같은 스위치에 붙을 수 있는지.
5. **300GB 축소 기준 확정**: §5.3의 단계 중 어디까지 적용할지. 제거 채널 목록과 기간.
6. **job4 실행 위치**: 노트북 클러스터에서 VPN 경유로 PostgreSQL(B)에 쓸지, Gold 사본을 S3에 올려 A Spark가 쓸지.
7. **완료 기준 4번의 해석**: "클라우드 위" 조건이 명시돼 있는지. 명시돼 있다면 EC2-A 논리 2 Executor로 충족되는지 평가자에게 확인.
8. **S3 접근 자격** 발급 주체와 형태(3안과 동일).
9. **Keycloak·Flink TaskManager 실측 메모리**(3안과 동일).

> 정리 기준일: 2026-09-03
