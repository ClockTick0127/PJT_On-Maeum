## 결론

현재 자원에서는 다음 구성이 가장 현실적입니다.

> **EC2-A는 서비스 계층, EC2-B는 실시간 데이터 계층, S3는 원천 데이터 저장소, GPU 서버는 오프라인 학습·배치 계층**으로 분리합니다.

두 대의 EC2에 Kafka·HDFS·Spark 클러스터를 억지로 분산 배치하는 것은 권장하지 않습니다. 논리적 파티셔닝은 가능하지만 Kafka 복제나 Spark/HDFS의 물리적 고가용성은 확보되지 않기 때문입니다.

또한 `t3.xlarge`는 4 vCPU지만 지속 성능형 인스턴스가 아닙니다. CPU 기준선은 40%이며, 장시간 초과 사용 시 Unlimited 추가 요금이 발생할 수 있습니다. 즉 EC2-B가 Kafka, 분석, DB, Spark를 동시에 계속 실행하기에는 불리합니다. [AWS T3 CPU 크레딧 문서](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/burstable-credits-baseline-concepts.html)

## 1. 권장 전체 구조

```text
[가구별 ESP32]
  └─ 10~30Hz 측정
  └─ 1초 단위 마이크로배치
          │ MQTT/TLS
          ▼
┌──────────────────────────────────────┐
│ EC2-A: Edge / Service                │
│ MQTT Broker, API, 인증, Monitoring,  │
│ Redis, React Dashboard               │
└──────────────────┬───────────────────┘
                   │
          MQTT–Kafka Bridge
                   │
                   ▼
┌──────────────────────────────────────┐
│ EC2-B: Stream / Data                 │
│ Kafka                                │
│ Realtime Analysis Consumer           │
│ Raw Archive Sink                     │
│ PostgreSQL                           │
└──────┬───────────────────────┬───────┘
       │                       │
       │ 실시간 분석 결과       │ 원천 데이터
       ▼                       ▼
 대시보드·알림             S3 Data Lake
                              │
                    Parquet / Iceberg
                              │
                ┌─────────────┴─────────────┐
                ▼                           ▼
       제한적 Spark 배치            온프레미스 GPU
       또는 EMR Serverless          학습·검증·재추론
```

핵심은 Kafka가 장기 저장소가 아니라 **짧은 버퍼와 재처리 입구** 역할만 하고, 모든 원천 데이터는 빠르게 S3로 내보내는 것입니다.

## 2. 가구 수에 따른 데이터 규모

다음과 같이 가정합니다.

- 센서 측정: 가구당 초당 10~30회
- ESP32가 측정값을 1초 단위로 묶어서 전송
- 압축된 메시지 크기: 가구당 초당 약 0.5~1KB
- Kafka 입력: 가구당 초당 1개 메시지

계산식은 다음과 같습니다.

```text
일일 데이터 = 가구 수 × 초당 메시지 수 × 메시지 크기 × 86,400초
```

| 가구 수 | 메시지 처리량 | 0.5KB 기준 | 1KB 기준 |
|---:|---:|---:|---:|
| 100가구 | 100 msg/s | 약 4.3GB/일 | 약 8.6GB/일 |
| 1,000가구 | 1,000 msg/s | 약 43GB/일 | 약 86GB/일 |
| 10,000가구 | 10,000 msg/s | 약 432GB/일 | 약 864GB/일 |

30Hz 샘플을 각각 JSON 메시지로 전송하면 1,000가구만으로도 하루 수백 GB가 생성될 수 있습니다. 따라서 다음 최적화가 필수입니다.

- 센서에서 1초 단위 마이크로배치
- JSON 대신 Avro·Protobuf 등 바이너리 포맷 사용
- Kafka 압축은 `lz4` 또는 `zstd`
- 전력 원본은 PostgreSQL에 넣지 않고 S3 Parquet으로 저장
- Kafka 보관 기간은 6~24시간으로 제한

EC2-B의 309GB 디스크는 1,000가구 기준으로 며칠 안에 소진될 수 있으므로 장기 저장 용도로 사용할 수 없습니다.

## 3. Kafka 분산 전략

### 토픽 설계

가구마다 토픽을 만들면 안 됩니다. 전체 가구가 공유하는 토픽을 만들고 `household_id`를 메시지 키로 사용합니다.

| 토픽 | 파티션 권장값 | 용도 |
|---|---:|---|
| `power.raw.v1` | 24 | 가구 원천 전력 |
| `analysis.snapshot.v1` | 12 | 최신 가전 상태 |
| `analysis.event.v1` | 12 | 가전 변화·이상 이벤트 |
| `power.replay.v1` | 24 | 과거 원천 재처리 |
| `dlq.power.v1` | 3 | 처리 실패 메시지 |

```text
key = household_id

가구 001 → partition 07
가구 002 → partition 13
가구 003 → partition 02
```

이 방식의 장점은 다음과 같습니다.

- 동일 가구 데이터의 순서를 유지한다.
- 서로 다른 가구를 병렬 처리할 수 있다.
- 분석 Worker 수를 늘리면 파티션이 자동으로 분배된다.
- 특정 가구의 데이터만 다시 재생할 수 있다.

현재 EC2-B에서는 실시간 Consumer를 2~3개 프로세스로 제한하고, 각 Consumer가 여러 파티션을 처리하는 구성이 적절합니다. 파티션 24개는 현재보다 많은 병렬도를 제공하지만 향후 Worker 확장 여유를 남깁니다.

### 전달 보장

완전한 Exactly-once 구현보다 다음 방식이 현실적입니다.

- MQTT QoS 1
- Kafka Producer `acks=all`
- 분석은 At-least-once
- `(household_id, device_id, sequence)`로 중복 제거
- 분석 이벤트에 고유 `event_id` 부여
- PostgreSQL 저장 시 `event_id` Unique Constraint 적용
- ESP32에 짧은 로컬 Ring Buffer를 두어 네트워크 복구 후 재전송

현재 Kafka는 단일 브로커이므로 `replication.factor=1`이며 장애 시 데이터 손실 가능성이 있습니다. 두 대 또는 `t3.micro`까지 동원해 불완전한 Kafka 클러스터를 만드는 것보다 S3 아카이빙과 센서 재전송을 안전망으로 두는 편이 낫습니다.

## 4. S3 분산 저장 전략

가구 수가 많더라도 다음과 같이 `household_id` 자체를 디렉터리 파티션으로 사용하지 않는 것이 좋습니다.

```text
s3://nilm-data/
  bronze/
    dt=2026-09-02/
      hour=10/
        bucket=037/
          part-....parquet
```

`bucket`은 다음처럼 계산합니다.

```text
bucket = hash(household_id) % 64
```

권장 원칙:

- Bronze: 정규화된 원천 데이터
- Silver: 결측·중복·시간 정렬이 끝난 데이터
- Gold: 모델 학습 Window, 가전 이벤트, 가구별 루틴
- Parquet 파일은 약 128~512MB가 되도록 묶어서 저장
- 가구마다 수초·수분 단위 파일을 만들지 않음
- 작은 파일은 일 단위 Compaction
- 모델·전처리 버전과 데이터 스키마 버전을 함께 저장
- 오래된 Bronze는 S3 Lifecycle로 저비용 스토리지 이동

S3는 접두사별로 높은 요청 처리량을 제공하고 자동 확장되므로, 현재와 같은 소규모 EC2 환경에서 HDFS보다 운영 기준 저장소로 적합합니다. [AWS S3 성능 지침](https://docs.aws.amazon.com/pdfs/whitepapers/latest/s3-optimizing-performance-best-practices/s3-optimizing-performance-best-practices.pdf)

## 5. EC2별 자원 배분

### EC2-A

| 구성요소 | 메모리 상한 예시 |
|---|---:|
| OS·Docker | 1.5~2GB |
| MQTT Broker | 0.5~1GB |
| API Gateway·Backend | 2~3GB |
| Keycloak | 1.5~2GB |
| Monitoring Service | 1~2GB |
| Redis | 1~2GB |
| React·Nginx | 0.5GB |
| 장애·버스트 여유 | 최소 3GB |

EC2-A에는 Spark, Kafka, 모델 학습을 올리지 않습니다.

### EC2-B

| 구성요소 | 메모리 상한 예시 |
|---|---:|
| OS·Docker | 1.5~2GB |
| Kafka JVM Heap | 2~3GB |
| Kafka 파일 Page Cache | 3~4GB 확보 |
| 실시간 분석 Worker | 3~4GB |
| PostgreSQL | 2~3GB |
| Archive Sink·Bridge | 0.5~1GB |
| 여유 공간 | 1~2GB |

이 구성만으로 메모리가 거의 소진됩니다. 따라서 EC2-B에서 Spark를 상시 실행하면 안 됩니다.

Spark가 반드시 필요하다면 다음 조건으로만 실행합니다.

- 야간 또는 낮은 트래픽 시간
- 최대 2개 Core
- Driver 1GB, Executor 2~4GB 수준
- 입력과 결과는 S3 사용
- 실시간 Consumer와 Kafka에 CPU·메모리 우선권 부여
- 배치 시간이 길어지면 즉시 GPU 서버 또는 관리형 서비스로 이동

## 6. 현재 자원이 부족할 때의 대안

### 우선순위 1: Spark 배치 분리

가장 먼저 Spark를 EC2-B에서 제거합니다.

- GPU 서버의 CPU·RAM이 충분하면 GPU 서버에서 단일 노드 배치 수행
- 단, EC2와 GPU 서버를 WAN으로 묶어 하나의 Spark 클러스터로 만들지는 않음
- 입력·출력을 S3로 전달해 비동기 배치 수행
- 데이터가 더 커지면 EMR Serverless 사용

EMR Serverless는 작업에 필요한 Worker를 자동으로 생성·확장하고 작업 종료 후 해제하므로, 간헐적인 대규모 Spark 작업에 적합합니다. [AWS EMR Serverless 문서](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/emr-serverless.html)

### 우선순위 2: Kafka 관리형 전환

Kafka 디스크 사용량이나 Consumer Lag이 계속 증가하면 EC2-B의 Kafka를 이전합니다.

Kafka API를 유지해야 한다면:

- **Amazon MSK Serverless**
- 기존 Producer·Consumer 변경이 적음
- 자동 용량 확장
- 클러스터 기준 최대 200MB/s 입력, 400MB/s 출력
- 파티션당 최대 5MB/s 입력

[AWS MSK Serverless 할당량](https://docs.aws.amazon.com/msk/latest/developerguide/limits.html)

Kafka 호환성이 중요하지 않고 AWS 네이티브 구성이 가능하다면:

- **Kinesis Data Streams On-demand**
- Shard 용량 계획 없이 자동 확장
- Consumer 코드를 Kinesis Client Library 기준으로 변경해야 함
- Kafka Connect·Kafka Streams 생태계 활용은 줄어듦

[AWS Kinesis On-demand 문서](https://docs.aws.amazon.com/streams/latest/dev/how-do-i-size-a-stream.html)

### 우선순위 3: PostgreSQL 분리

DB 쿼리 지연과 Kafka I/O가 서로 영향을 주기 시작하면 PostgreSQL을 RDS로 이동합니다.

- 초기: RDS PostgreSQL Single-AZ
- 실제 돌봄 서비스 운영: Multi-AZ
- 원본 전력 데이터는 계속 S3에 저장
- PostgreSQL에는 기기·이벤트·알림·대응 이력만 저장

RDS Multi-AZ는 별도 Standby로 장애 조치를 지원합니다. [AWS RDS Multi-AZ 문서](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.MultiAZ.html)

### 우선순위 4: EC2-B 인스턴스 변경

실시간 분석 CPU가 지속적으로 높다면 T 계열을 계속 확장하기보다 지속 성능형 인스턴스로 변경합니다.

- Kafka·DB·분석 혼합: M 계열
- CPU 추론 중심: C 계열
- 큰 Window·Redis·캐시 중심: R 계열
- Spark Worker가 필요하면 분석 서버와 별도 인스턴스로 추가

단순하게 `t3.xlarge → t3.2xlarge`로 변경해도 T 계열의 CPU 크레딧 특성은 남습니다.

## 7. 전환 판단 기준

다음 조건 중 하나가 반복되면 자원 분리를 시작합니다.

| 지표 | 전환 기준 예시 | 조치 |
|---|---|---|
| `CPUCreditBalance` | 지속 하락 | T 계열 탈피 또는 배치 분리 |
| EC2-B CPU | 15분 이상 70% 초과 | 분석 Worker 또는 Spark 분리 |
| 메모리 | 지속 80% 초과 | DB·Spark·Redis 중 하나 이동 |
| Kafka Consumer Lag | 10분 이상 계속 증가 | Consumer 추가 또는 MSK/Kinesis 전환 |
| Kafka 디스크 | 70% 초과 | Retention 축소, S3 Sink 확인 |
| 처리 지연 | p95가 목표 5초 초과 | 분석 병렬도·모델 최적화 |
| Spark 배치 | 다음 실행 주기 전에 완료되지 않음 | EMR Serverless 이동 |
| PostgreSQL | p95 쿼리 지연 지속 증가 | RDS 이전 |
| S3 작은 파일 | 파일 수 급증, 평균 수 MB 이하 | Archive Sink 묶음 크기 확대·Compaction |

## 최종 추천안

현재 단계에서는 다음 순서가 가장 적합합니다.

1. 센서 데이터를 1초 단위로 묶어 메시지 수를 줄인다.
2. Kafka는 EC2-B 단일 브로커로 시작하되 `household_id` 기준 24개 파티션을 사용한다.
3. 실시간 분석은 Spark가 아닌 가벼운 Python Consumer Group으로 수행한다.
4. 원천 데이터는 즉시 S3 Bronze Parquet으로 내보내고 Kafka 보관 기간을 짧게 둔다.
5. Spark는 배치 전용으로 제한하며 EC2-B의 온라인 부하와 겹치지 않게 한다.
6. 가장 먼저 부족해지는 Spark를 GPU 서버나 EMR Serverless로 이동한다.
7. 다음으로 Kafka를 MSK Serverless, PostgreSQL을 RDS로 이전한다.
8. 실제 운영에서 HA가 필요해지는 시점에는 두 EC2로 버티지 말고 Kafka·DB를 관리형 서비스 또는 3개 이상 노드로 전환한다.

현재 구성은 **1~수백 가구 MVP와 가상 부하 시험**에는 적합하지만, 실제 수천 가구 운영 용량은 메시지 크기와 NILM 추론 시간에 따라 크게 달라집니다. 최종 수용 가구 수는 100 → 500 → 1,000 → 5,000 → 10,000가구 리플레이 테스트에서 Consumer Lag, CPU 크레딧, 디스크 I/O, p95 처리 지연을 측정해 결정해야 합니다.