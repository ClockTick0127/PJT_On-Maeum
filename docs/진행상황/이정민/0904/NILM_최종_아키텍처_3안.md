# NILM 최종 아키텍처 (3안 확정): Edge / Stream·Data 분리 + Spark 2노드

> 검토 문서: [NILM_1-3안_비교_요약.md](NILM_1-3안_비교_요약.md), [NILM_1안_2안_비교.md](NILM_1안_2안_비교.md), [NILM_서버역할_분리_3안_비교.md](NILM_서버역할_분리_3안_비교.md)
> 자원: EC2 2대(4 vCPU·15GiB·309GB, 비버스터블 계열) + 온프레미스 GPU 서버 + 노트북(14코어·32GB·외장 SSD) + 보조 EC2(1GB). 장기 저장소는 S3. AWS 콘솔 없이 SSH만 가능.

## 1. 결론

> **EC2-A는 Edge·서비스 계층, EC2-B는 스트림·데이터 계층, S3는 데이터 레이크, 온프레미스 GPU는 학습 계층으로 분리한다. Spark는 EC2 위에 Standalone 클러스터로 두되, Master는 B, 상시 연산은 A, B의 Worker는 물리 2노드 시연·스케일 실험 때만 쓴다.**

3안을 택한 이유는 "Spark가 클라우드 인프라 위에서 분산 실행된다"는 것을 실제 노드 2대로 보이기 위해서다. 그 대가로 배치 시간대에 실시간 경로가 캐시·CPU·디스크를 나눠 쓰게 되므로, 이 문서는 **어느 시간에, 어느 크기로, 어느 노드에서 Spark를 돌릴지**를 운영 규칙으로 고정한다.

핵심 원칙 다섯 가지:

1. 실시간 경로(`MQTT → Kafka → Flink → Monitoring`)는 Spark가 돌지 않는 시간에 완료 기준을 측정하고, 배치 시간대의 지연은 별도로 기록한다.
2. Spark 연산의 기본 위치는 A다. B의 Worker는 시연·실험 때만 Executor를 받는다.
3. Kafka는 6~24시간 버퍼이고, 재처리 기준은 S3 Bronze다.
4. 학습 데이터 1차 변환(ZIP → 10Hz Parquet)은 노트북이 하고, EC2 Spark는 S3 Silver 이후 단계만 맡는다.
5. HA(고가용성)는 아니다. Kafka·PostgreSQL·Flink가 각각 단일 인스턴스임을 문서에 명시하고 로드맵 4단계로 이연한다.

## 2. 전체 구조

```text
[가구별 ESP32 + 클램프 센서]
  └─ 10Hz 측정, 1초 단위 마이크로배치
          │ MQTT/TLS 8883, QoS 1
          ▼
┌──────────────────────────────────────────────────────────┐
│ EC2-A: Edge / Service                                     │
│  Nginx + React Dashboard                                  │
│  API Gateway (JWT 검증) · Keycloak (인증)                  │
│  IoT Device Service · Monitoring Service (SSE)            │
│  Mosquitto (MQTT Broker) · Redis (최신 상태 Projection)    │
│  ── Spark Worker ×2 (상시 데몬, Executor는 Job 때만) ──     │
│  ── Spark Driver (cluster 모드) · Bronze Compaction cron ──│
└───────────────┬───────────────────────────┬──────────────┘
                │ MQTT 구독                   │ analysis.* 소비, PostgreSQL 쓰기
                ▼                            ▲
┌──────────────────────────────────────────────────────────┐
│ EC2-B: Stream / Data                                      │
│  MQTT–Kafka Bridge (Spring)                               │
│  Kafka (KRaft 단일 브로커, 파티션 24, key=household_id)    │
│  Flink JobManager + TaskManager (슬롯 2)                   │
│    변화점 탐지 · 가전 상태 판정 · 루틴 비교 · 부재 타이머     │
│  PostgreSQL (keycloak·device·analysis·monitoring DB)      │
│  Raw Archive Sink → S3 Bronze                             │
│  ── Spark Master · Spark Worker ×1 (시연·실험 시 Executor) ─│
└──────┬──────────────────────────────────────┬─────────────┘
       │ analysis.snapshot.v1 / analysis.event.v1│ 원천 Parquet (1~5분 묶음)
       ▼                                        ▼
  Monitoring(A) → Redis · PG · SSE      ┌──────────────────────────────────┐
  → React Dashboard · 알림              │ S3 Data Lake (서울)               │
                                        │  bronze/  power_raw               │
                                        │  silver/  power_samples, aihub_10hz│
                                        │  gold/    training_windows, routine│
                                        │  artifacts/ models, flink ckpt    │
                                        └───────┬────────────────┬──────────┘
                                                │ s3a://          │ Gold 내려받기 / 모델 등록
                                                ▼                 ▼
                              ┌────────────────────────┐  ┌────────────────────────┐
                              │ Spark on EC2 (A 상시 +  │  │ 온프레미스 GPU 서버      │
                              │ B 시연)                 │  │ PyTorch 학습·검증        │
                              │ 라벨 조인 · 학습 윈도우  │  │ leave-house-out 평가     │
                              │ 기준선 · Compaction     │  │ 모델 아티팩트 → S3       │
                              └────────────────────────┘  └────────────────────────┘
                                                ▲
                              ┌────────────────────────┐
                              │ 노트북 (1차 변환)        │
                              │ AI Hub ZIP → 10Hz Silver│
                              │ Python 프로세스 풀       │
                              │ → aws s3 sync silver/   │
                              └────────────────────────┘
```

## 3. 서버별 구성요소와 역할

| 위치 | 구성요소 | 맡는 이유 | 두지 않는 것 |
| --- | --- | --- | --- |
| **EC2-A** | Nginx·React, API Gateway, Keycloak, IoT Device, Monitoring(SSE), Mosquitto, Redis, **Spark Worker ×2·Driver**, Compaction cron | 외부 진입점. 메모리·CPU 여유가 커서 Spark 연산을 받을 수 있고, 페이지 캐시·디스크 fsync에 민감한 프로세스가 없어 배치 간섭이 B보다 작다 | Kafka, PostgreSQL, Flink |
| **EC2-B** | Bridge, Kafka, Flink JM·TM, PostgreSQL, Archive Sink, **Spark Master·Worker ×1** | 메시지 버퍼·판정·영구 저장을 한 장애 도메인으로. Master는 제어면(0.5GB)이라 부담이 없고, 향후 Worker 추가 시 이동이 없다 | 상시 Spark Executor, 외부 공개 포트 |
| **S3** | bronze / silver / gold / artifacts 버킷 | 재처리 기준 저장소. EC2와 같은 리전이라 전송 무료. Flink Checkpoint와 모델 파일도 여기 | 실시간 화면 직접 조회 |
| **온프레미스 GPU** | PyTorch 학습·평가, 모델 export | GPU가 필요한 유일한 작업. Gold만 내려받아 로컬 디스크에서 학습 | 실시간 추론 경로, 원천 DB |
| **노트북** | ZIP → 10Hz Silver 1차 변환, S3 업로드, 리플레이 Producer·채점기 | 14코어·외장 SSD가 있어 40,641파일 변환이 3~4시간. EC2 4 vCPU로는 8시간 이상이고 Training ZIP 521GB가 EC2 디스크에 안 들어감 | 상시 서비스 |
| **보조 EC2 (1GB)** | WireGuard·점프 호스트. 선택: Kafka 컨트롤러 전용 노드(§10) | 메모리 1GB로 다른 것은 불가 | Kafka 브로커, DB, Spark, Redis |

## 4. 데이터 흐름

### 4.1 실시간 전력 이벤트 (초~수 초)

```text
ESP32 → Mosquitto(A) → Bridge(B) → Kafka power.raw.v1 (key=household_id)
  → Flink(B): keyBy(household) → 스키마 검증·중복 제거 → 윈도우 → 변화점 탐지
             → 가전 상태 판정(히스테리시스) → 기준선 비교 → 부재 타이머(ROUTINE_MISSED)
  → Kafka analysis.snapshot.v1 / analysis.event.v1
  → Monitoring(A): Redis 최신 상태 · PostgreSQL(B) 이벤트·사건 · Outbox → SSE → React
```

- Flink는 판정 엔진이고 Monitoring은 서빙 계층이다. Flink는 SSE·권한·사건 상태를 다루지 않는다.
- Flink Checkpoint는 30초 간격, `s3a://nilm-artifacts-dev/flink/checkpoints`. 재시작 시 Kafka Offset과 가구별 상태가 함께 복원된다.
- Monitoring은 `isolation.level=read_committed`로 소비하고, `event_id` Unique 제약과 Outbox로 멱등 처리한다.

### 4.2 이력 보존·학습·재처리 (분~일)

```text
Kafka power.raw.v1 → Archive Sink(B) → S3 bronze/power_raw/dt=/hour=/bucket=   (1~5분 묶음)
AI Hub ZIP → 노트북 Python 풀 → 10Hz Silver → S3 silver/aihub_10hz/house_id=/channel=/dt=
S3 Silver → Spark(EC2) → S3 gold/training_windows, PostgreSQL routine_baseline
S3 Gold → GPU 서버 → 모델 → S3 artifacts → Flink Job이 버전 지정 로드
S3 Bronze → Replay Producer → Kafka power.replay.v1 → 같은 Flink Job → analysis_version 분리
```

### 4.3 서비스·대응 피드백 (요청 즉시)

```text
React → API Gateway(A) → Keycloak(JWT) / IoT Device(기기 등록·MQTT ACL) / Monitoring(조회·확인·조치)
  → PostgreSQL(B) device_db · monitoring_db, Redis(A) 최신 상태
```

### 4.4 Kafka 토픽

| 토픽 | 파티션 | Producer | Consumer |
| --- | ---: | --- | --- |
| `power.raw.v1` | 24 | Bridge | Flink, Archive Sink |
| `analysis.snapshot.v1` | 12 | Flink | Monitoring |
| `analysis.event.v1` | 12 | Flink | Monitoring, (선택) Archive Sink |
| `power.replay.v1` | 24 | Replay Producer | Flink |
| `baseline.v1` | 3 | Spark job4 | Flink (Broadcast State) |
| `dlq.*` | 3 | 각 Consumer | 운영 도구 |

## 5. Spark 배치 전략

### 5.1 클러스터 형태

| 역할 | 위치 | 크기 | 상시 여부 |
| --- | --- | --- | --- |
| Master | EC2-B | 0.5GB | 상시 (제어면만) |
| Worker A ×2 | EC2-A | 각 Executor 2g + overhead → 2.5GB, 1코어 | 데몬 상시(각 0.1GB), Executor는 Job 때만 |
| Worker B ×1 | EC2-B | Executor 2g → 2.5GB, 1코어 | 데몬 상시, **Executor는 시연·실험 Job에만 배정** |
| Driver | Job을 받은 Worker 위 (`--deploy-mode cluster`) | 1.0GB | Job 때만 |

Executor 크기는 Standalone 규칙상 한 애플리케이션 안에서 균일하므로 전부 2g로 통일한다. 힙 2g × `spark.memory.fraction 0.6` = 1.2GB 실행 메모리에, Parquet 입력 스플릿 128MB의 압축 해제 팽창(약 500MB) × 1코어가 들어간다.

### 5.2 A만 쓰는 일상 배치와 A+B를 쓰는 시연 배치

```bash
# 일상 배치: A의 Executor 2개만. spreadOut=false로 가장 적은 Worker에 몰아 A를 먼저 채움
spark-submit --master spark://<B 사설 IP>:7077 --deploy-mode cluster \
  --executor-memory 2g --executor-cores 1 --conf spark.cores.max=2 \
  --conf spark.deploy.spreadOut=false \
  --driver-memory 1g \
  --packages org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.hadoop.fs.s3a.committer.name=magic \
  s3a://nilm-artifacts-dev/jobs/job3_training_windows.py --dataset-version v1

# 시연·스케일 실험: cores.max=3으로 B Worker까지 사용 → Executors 탭에 호스트 2개, Remote Shuffle > 0
spark-submit ... --conf spark.cores.max=3 --conf spark.deploy.spreadOut=true ...
```

- `spark.cores.max=2` + `spreadOut=false`이면 코어 2개를 가진 A Worker 쪽에 Executor 2개가 먼저 배정되어 B는 비어 있다. 이것이 일상 모드다.
- 시연 모드에서 A Worker를 `docker stop`하면 태스크가 B Executor로 재스케줄되어 완료된다. 워커 장애 복구 시연은 이것으로 한다.
- `spark.local.dir`은 A·B 모두 전용 디렉터리(`/data/spark-scratch`). 콘솔이 없어 EBS를 추가할 수 없으므로 같은 볼륨이지만, B에서는 Executor를 1코어로 제한해 spill을 최소화한다.

### 5.3 Spark가 맡는 Job

| Job | 입력 | 출력 | 주기 | 모드 |
| --- | --- | --- | --- | --- |
| job2 라벨 조인·`is_active` | S3 Silver 10Hz + AI Hub 이벤트 테이블 | S3 Silver labeled | Silver 업로드 후 1회, 증분 | 일상(A) |
| job3 학습 윈도우·leave-house-out·Compaction | Silver labeled | S3 Gold `training_windows/dataset_version=` | 모델 실험마다 | 일상(A) / 시연(A+B) |
| job4 루틴 기준선 | 앞 20일 라벨 | PostgreSQL(B) `routine_baseline`, Kafka `baseline.v1` | MVP 1회 | 일상(A) |
| job7 Bronze Compaction | S3 bronze 작은 파일 | S3 silver/power_samples | 야간 매일 | MVP 규모는 A의 Python cron. 가구 수 증가 시 Spark로 |

Spark를 쓰지 않는 것: ZIP 해제·JSON 파싱(노트북 Python), 리플레이 Producer·채점기(노트북 또는 보조 EC2), 실시간 판정(Flink), 학습(GPU).

### 5.4 실행 시간대

| 시간 | 허용 작업 | 이유 |
| --- | --- | --- |
| 02:00~05:00 | 일상 배치(job2·3·4·7) | 센서 유입은 계속되지만 대시보드 접속과 알림이 가장 적다 |
| 주간 | Spark Executor 없음 | 완료 기준 1·3번 측정, 시연 리허설 |
| 시연 슬롯 (사전 공지) | A+B 시연 배치 | p95 지연을 함께 기록해 간섭 크기를 남긴다 |

## 6. 메모리 예산

### EC2-A (15GiB)

| 컴포넌트 | 할당 | 근거 |
| --- | ---: | --- |
| OS·Docker | 1.5GB | 커널·systemd·dockerd·shim |
| Nginx + React | 0.3GB | 정적 파일 |
| API Gateway | 0.8GB | 힙 512MB × 1.6 |
| Keycloak | 1.2GB | 힙 768MB + non-heap. 실측 우선 항목 |
| IoT Device | 0.6GB | 힙 384MB × 1.6 |
| Monitoring | 0.8GB | 힙 512MB + Kafka fetch + SSE 세션 |
| Mosquitto | 0.2GB | 연결당 수 KB |
| Redis | 0.5GB | maxmemory 400MB, 가구당 약 105KB |
| Compaction cron | 0.3GB | pyarrow 파일 단위 스트리밍 |
| **평시 소계** | **6.2GB** | 여유 8.8GB |
| Spark Worker 데몬 ×2 | 0.2GB | 상시 |
| Spark Executor ×2 (2g + 384MB) | 4.8GB | Job 중 |
| Spark Driver | 1.0GB | Job 중 |
| **배치 중 합계** | **12.2GB** | **여유 2.8GB** |

### EC2-B (15GiB)

| 컴포넌트 | 할당 | 근거 |
| --- | ---: | --- |
| OS·Docker | 1.5GB | |
| Bridge | 0.6GB | 힙 384MB × 1.6 |
| Kafka (KRaft) | 2.0GB | 힙 1GB + 페이지 캐시 최소 1GB |
| Flink JobManager | 1.0GB | process.size 1024m |
| Flink TaskManager | 2.5GB | process.size 2560m, 슬롯 2. 1,000가구 상태 약 530MB를 Managed Memory에 수용 |
| PostgreSQL | 1.5GB | shared_buffers 384MB + 백엔드 40개 + work_mem |
| Archive Sink | 0.5GB | 5분 버퍼 + S3 멀티파트 |
| Spark Master | 0.5GB | 제어면 |
| Spark Worker 데몬 | 0.1GB | 상시 |
| **평시 합계** | **10.2GB** | **여유 4.8GB** (일상 배치 중에도 동일. Executor는 A에만) |
| Spark Executor ×1 (시연 시) | 2.4GB | 시연 Job 중 |
| **시연 배치 중 합계** | **12.6GB** | **여유 2.4GB** |

두 서버 모두 최악 시점에 안전선 2GB를 지킨다. 다만 §7의 소프트 간섭(페이지 캐시 회수, CPU·디스크 경쟁)은 예산과 별개로 배치 시간대에 발생한다.

## 7. 운영 규칙

| 항목 | 규칙 |
| --- | --- |
| CPU 격리 | A Worker 컨테이너 `--cpus=2`(Gateway·SSE에 2 vCPU 보장), B Worker `--cpus=1`(Flink 슬롯 2 + Kafka에 3 vCPU 보장) |
| 디스크 | Executor 프로세스 `ionice -c3`. B에서는 시연 Job만, 입력이 작은 job2 우선 |
| 포트·방화벽 | Spark 7077·8080·8081·7001·7002는 사설 IP 바인딩 + UFW로 VPC 내부만. A의 공개 포트는 443·8883만. 콘솔이 없으므로 현재 SG가 이 포트를 A↔B 사이에 열어 두고 있는지 `nc -zv`로 선행 확인 |
| 컨테이너 상한 | 모든 컨테이너에 `mem_limit`, JVM은 `-XX:MaxRAMPercentage=65` |
| 측정 | 배치 없는 시간과 배치 중 두 번 측정: 완료 기준 1번 p95, Consumer Lag, Flink Checkpoint 시간, Kafka fetch 지연. 차이를 시연 자료에 남긴다 |
| S3 자격 | Bronze 쓰기(B Sink), Silver/Gold 읽기·쓰기(Spark A·B), Gold 읽기(GPU) 세 경로. IAM 키 또는 인스턴스 역할. 버킷 생성 권한 포함 |
| 배포 단위 | A Compose(edge + spark-worker), B Compose(data + spark-master·worker). Flink Job은 Application 모드 이미지 |

## 8. 완료 기준 매핑

| 완료 기준 | 충족 방법 |
| --- | --- |
| 1. 포트 ON 후 5초 이내 표시 | Flink 판정 → Kafka → Monitoring → SSE. 배치 없는 시간에 측정, 배치 중 값은 별도 기록 |
| 2. 30회 ON/OFF 80% | Flink 판정 규칙·시그니처. 인프라 무관 |
| 3. 110msg/s 유실 0 | Kafka 단일 브로커 + Flink Checkpoint + Archive Sink 재시도. 리플레이 Producer는 노트북에서 |
| 4. Spark UI 2워커 분산 | 일상: A Worker 2개(논리). 시연: A+B(물리), Remote Shuffle > 0, A Worker 종료 후 재스케줄 |
| 5. Parquet → Spark → 학습용 Parquet | job2·job3, S3 Silver → S3 Gold |
| 6. 기준 시각까지 미사용 알림 | Flink 부재 타이머 → `ROUTINE_MISSED` → Monitoring 알림 |
| 7. 처리량·지연·정확도·오탐 기록 | 채점기(노트북) + §7 측정 |

## 9. 장애 시 영향

| 장애 | 영향 | 복구 |
| --- | --- | --- |
| A 다운 | MQTT 입구·화면·Spark 연산 중단. Kafka에 남은 데이터는 Flink가 계속 처리, PG 저장 계속 | Compose 재기동. 센서는 Ring Buffer로 재전송 |
| B 다운 | Bridge 이후 전부 중단(Kafka·Flink·PG). Spark Master 소실로 A Worker가 Job 못 받음 | Compose 재기동. Flink는 S3 Checkpoint에서 복원, Kafka Offset 이어서 소비 |
| S3 불가 | Archive Sink 재시도(Kafka 보관 기간 내), Flink Checkpoint 실패(처리는 계속), Spark 입력 불가 | 자격·네트워크 복구 후 자동 재시도 |
| Spark Executor OOM | 해당 컨테이너만 종료, 태스크 재시도 | 예산 준수 시 다른 서비스 무영향 |
| 배치 중 실시간 지연 | 소프트 간섭. 죽지 않지만 p95 상승 | 시간대 규칙, CPU·I/O 제한, Executor 축소 |
| GPU 다운 | 학습만 중단 | 기존 모델로 판정 지속 |

## 10. 확장 경로

1. **Kafka 2브로커**: A에 Broker 2(2GB) 추가. A 여유로 흡수 가능. 컨트롤러는 보조 EC2를 세 번째로 넣어 3노드 쿼럼을 만들 때만 어느 한 대의 장애를 견딤. RF=2·`min.insync.replicas=1`은 HA가 아니라 복제·리더 이동 시연임을 명시.
2. **Spark 분리**: 가구 수 증가로 Compaction이 무거워지면 EMR Serverless 또는 GPU 서버 Spark로 이동. A·B Worker 제거.
3. **관리형 전환**: Kafka → MSK Serverless, PostgreSQL → RDS, 이후 EC2-B는 Flink 전용.
4. **HA**: Kafka 3브로커, PG Primary/Standby, API 2대 + LB, 다른 AZ. 실서비스 전 필수.

## 11. 남은 확인 사항

1. A↔B 사이 Spark 포트(7077·8080·7001·7002·Worker 포트)가 현재 보안 그룹에서 열려 있는지. 막혀 있으면 B Worker와 Master-B 구성을 접고 Spark 전부를 A에 둔다.
2. S3 접근 자격 발급 주체와 형태.
3. Keycloak·Flink TaskManager 실측 메모리. 예산표에서 편차가 가장 큰 두 항목.
4. A·B의 AZ 동일 여부.
5. 노트북 Silver 업로드 회선 속도. Validation 45~60GB 약 1.5시간, 전체 400~600GB 10~14시간.
6. 시연 슬롯 일정. 이 시간에만 B Executor를 허용한다.

> 정리 기준일: 2026-09-03
