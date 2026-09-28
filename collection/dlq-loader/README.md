# DLQ Loader 설계

작성일: 2026-09-18. 설계 단계이며 실행 코드·Compose 서비스는 아직 추가하지 않았다.
수집 대상은 `dlq.analysis`, `dlq.monitoring`이다.

## 1. 책임과 기존 quarantine의 관계

`dlq-loader`는 서비스가 Kafka에 발행한 실패 메시지를 HDFS에 보관하는 독립 Consumer다.
기존 `bronze-loader`의 quarantine은 원천 적재 중 발견한 검증 실패이므로 그대로 유지한다.
DLQ Loader가 기존 quarantine 파일을 다시 소비하거나 Kafka로 재발행하지 않는다.

```text
power.raw.v1 ─┬─ bronze-loader ─ 정상 ─ /nilm/bronze/power
             │               └ 오류 ─ /nilm/quarantine/power
             └─ analysis ─ 입력 검증 실패 ─ dlq.analysis ─┐
                                                        ├─ dlq-loader ─ /nilm/dlq/...
analysis.event.v1 / analysis.snapshot.v1                  │
             └─ monitoring ─ 실패 ─ dlq.monitoring ───────┘
                            (DLQ 발행은 별도 구현 필요)
```

| 데이터 | 저장 주체 | 의미 | 처리 방식 |
|---|---|---|---|
| Bronze | bronze-loader | 적재기의 필드 변환을 통과한 원천 | 기존 경로 유지 |
| Bronze quarantine | bronze-loader | JSON/필드 변환 실패 원천 | 기존 경로·스키마·manifest 유지 |
| 서비스 DLQ | dlq-loader | 서비스가 발행한 실패 기록 | 토픽별 DLQ 경로에 보관 |
| 해석 불가능한 DLQ | dlq-loader | DLQ envelope 자체가 잘못되거나 미지원 | 같은 DLQ 경로에 원본과 해석 상태 보관 |

Bronze와 analysis는 같은 원천을 각각 소비하는 병렬 경로다. 예를 들어 깨진 JSON 하나를
각각 읽으면 Bronze는 quarantine에 원천을 저장하고 analysis는 DLQ에 실패 기록을 발행한다.
이 경우 원천은 한 건이고 실패를 관측한 주체는 둘이다. 저장 단계에서 어느 기록도 삭제하지
않고, 조회 시 원천 위치로 연결한다. quarantine이 DLQ로 전달되는 순차 흐름은 아니다.

### 선행 수정: Bronze의 계측 계약 갱신

Bronze의 추출 필드와 Parquet 스키마는 analysis의 현행 계약인
`message_id/household_id/device_id/measured_at/active_power/reactive_power/power_factor/current`로 갱신했다.
원본 바이트와 Kafka 메타데이터는 계속 보존하고, 추가 계측값은 실제 producer 계약에 맞춰
컬럼화한다. 기존 Parquet과 신규 스키마의 읽기 호환성·버전 분리도 함께 결정한다.
구 필드 유무로 유효성을 판단하지 않으며 신규 필드만 있는 정상 입력이 Bronze로 저장되는지
검증한다. 기존 quarantine은 원본으로 다시 검증해 실제 불량과 계약 불일치에 의한 오격리를
분리하고, 재적재 이력을 남긴다. 과거 실패 기록을 곧바로 삭제하거나 전부 불량으로 집계하지 않는다.

## 2. 현재 코드에서 확인한 계약

- analysis의 `DlqMessage`: `source_topic`, `source_partition`, `source_offset`,
  `error_code`, `error_message`, `failed_at`, `payload`.
- analysis는 JSON/UTF-8 오류를 `INVALID_JSON`, 모델 검증 오류를 `VALIDATION_ERROR`로
  발행한다. handler 실행 오류 전체가 DLQ로 전송되는 것은 아니다.
- analysis의 잘못된 UTF-8 입력은 `errors="replace"`로 변환되어 DLQ payload에 담긴다.
  DLQ에서 원천 바이트를 완전히 복원할 수 없으므로 Bronze/quarantine 원본과 연결해야 한다.
- monitoring은 현재 `CommonContainerStoppingErrorHandler`로 실패 시 처리를 중단한다.
  Compose에 토픽 설정은 있지만 확인한 backend 코드에는 DLQ 발행 구현이 없다.
  Loader 추가만으로 monitoring 실패가 보관되지는 않는다.
- 기존 quarantine 필드는 `error_code`, `topic`, `partition`, `kafka_offset`,
  `kafka_ts`, `ingested_at`, `raw_payload`다. 여기서 Kafka 위치는 원천 위치다.

monitoring 발행 구현 시 analysis와 같은 envelope를 권장한다. 발행 ACK 후 원천 offset을
커밋하고, 발행 실패 시 원천 offset을 커밋하지 않는 계약이 필요하다. 기존 중단 정책의
변경과 재시도 대상 오류 분류는 monitoring 서비스 작업으로 분리한다.

## 3. 저장 스키마 v1

두 토픽 모두 명시적인 하나의 Parquet 스키마를 사용한다. 원본 보관을 먼저 보장하고,
해석 가능한 필드만 추출한다. envelope 검증 실패도 저장 가능한 정상적인 보관 결과다.

| 컬럼 | 타입 | 의미 |
|---|---|---|
| `schema_version` | int32 | 보관 스키마 버전, 초기값 1 |
| `failure_stage` | string | 구독 토픽에서 결정하는 `analysis` / `monitoring` |
| `topic`, `partition`, `kafka_offset` | string, int32, int64 | **DLQ 레코드 자체**의 Kafka 위치 |
| `kafka_ts`, `ingested_at` | UTC timestamp(ms), UTC timestamp(ms) | DLQ Kafka 시각, 최초 버퍼 수신 시각 |
| `raw_payload` | nullable binary | DLQ value 원본. null과 빈 바이트를 구분 |
| `raw_key` | nullable binary | DLQ Kafka key 원본 |
| `headers` | list of struct(key: string, value: nullable binary) | 중복 이름·순서를 보존하는 Kafka headers |
| `source_topic`, `source_partition`, `source_offset` | nullable string, int32, int64 | envelope가 가리키는 원천 위치 |
| `error_code`, `error_message` | nullable string | 서비스가 기록한 실패 사유 |
| `failed_at` | nullable UTC timestamp(ms) | 서비스 실패 시각 |
| `payload_json` | nullable string | envelope payload의 JSON 표현. 원천 바이트와 구별 |
| `parse_status` | string | `PARSED`, `MALFORMED_JSON`, `INVALID_ENVELOPE`, `UNSUPPORTED_FORMAT`, `NULL_VALUE` |
| `parse_error` | nullable string | Loader의 해석 실패 사유. 서비스 오류와 구분 |

analysis adapter는 현재 계약을 검증한다. monitoring adapter는 발행 계약 확정 전에는
`UNSUPPORTED_FORMAT`으로 원본만 보관하고, 계약 확정 후 활성화한다. 단, JSON 자체가
깨졌으면 `MALFORMED_JSON`, tombstone은 `NULL_VALUE`로 기록한다.
계약상 정수 위치·필수 필드·시각을 검증하며 실패 필드를 억지로 형변환하지 않는다.
미확인 source 위치를 DLQ 위치나 기본값 0으로 채우지 않는다.

새 형식의 해석 기능을 추가하면 저장된 raw payload를 별도 배치에서 다시 해석할 수 있다.
DLQ 해석 실패를 다시 DLQ로 발행하는 순환 경로는 만들지 않는다.

## 4. 경로와 배치

```text
/nilm/quarantine/power/ingest_date=YYYY-MM-DD/partition=P/...       # 기존 유지
/nilm/dlq/topic=dlq.analysis/ingest_date=YYYY-MM-DD/hour=HH/partition=P/part-P-START-END-BATCH_ID.parquet
/nilm/dlq/topic=dlq.monitoring/ingest_date=YYYY-MM-DD/hour=HH/partition=P/part-P-START-END-BATCH_ID.parquet
/nilm/manifests/job=dlq-loader/topic=TOPIC/date=YYYY-MM-DD/manifest-P-START-END-BATCH_ID.json
```

- 날짜·시간은 UTC 수신 시각 기준이다. 버퍼는 `(topic, partition)` 단위이며 시간 경계에서
  이전 버퍼를 마감한 뒤 새 시간 버퍼를 연다. 재시도 중 배치 경로와 ID는 고정한다.
- 기본 Consumer group은 `hdfs-dlq-loader`. 다른 서비스 및 bronze-loader와 공유하지 않는다.
- 초기 flush 조건: 300초 또는 파티션별 5,000건 또는 16MiB 원본 바이트 중 먼저 도달.
  전체 버퍼 원본 바이트 64MiB 도달 시 오래된 버퍼부터 마감한다. 실제 메모리는 Arrow 변환과
  출력 버퍼 때문에 더 크므로 컨테이너 메모리 제한은 실측 후 별도로 설정한다.
- 단일 메시지가 임계값을 넘으면 단독 배치로 저장한다. 저유량의 빈 파일은 생성하지 않는다.
- 원천 식별자·가구·오류 코드별 디렉터리는 만들지 않는다. 토픽 설정은 명시적 허용 목록으로
  검증하며 메시지 본문 값을 경로에 넣지 않는다.

## 5. 저장 완료와 offset 커밋

보장은 **at-least-once**다. 다중 토픽에서 partition 번호만으로 버퍼·경로·커밋을 관리하면
두 토픽의 partition 0이 섞이므로 반드시 `(topic, partition)`을 함께 사용한다.

1. `enable.auto.commit=false`, `enable.auto.offset.store=false`로 설정한다.
2. 읽은 순서대로 버퍼를 유지한다. Kafka offset 숫자 사이에 빈 번호가 있을 수 있으므로
   모든 정수가 존재해야 한다고 가정하지 않고, 소비한 레코드를 건너뛰지 않았는지 확인한다.
3. 배치를 고정하고 고유 임시 경로에 Parquet을 쓴다. close·크기·내용 checksum 검증 후
   최종 경로로 rename한다. checksum은 원격 파일을 읽어 검증하며 rename 반환값도 확인한다.
4. 파일 경로·checksum·크기·행 수·토픽·파티션·offset 범위·parse_status별 건수·스키마 버전을
   담은 manifest를 임시 쓰기 후 rename으로 확정한다. 기존 최종 파일을 삭제 후 교체하지 않는다.
5. 해당 파티션의 `end_offset + 1`만 동기 커밋하고 파티션별 결과 오류까지 검사한다.
6. 성공 후 버퍼를 제거한다. 실패 시 해당 파티션의 후속 구간을 먼저 커밋하지 않는다.

파일/manifest 업로드의 응답이 불확실하면 동일 배치 ID의 최종 파일 존재와 checksum을
검증한다. 일치하면 단계를 이어가고 불일치하면 실패 처리한다. 새 프로세스에서 재소비하는
경우 새로운 배치 ID를 써서 이전 파일을 보존한다. 경계가 달라진 중복은 조회 단계에서 제거한다.

HDFS 실패는 레코드 해석 실패가 아닌 인프라 오류다. bounded retry 후 실패 종료하고
재시작으로 복구한다. 실패 버퍼를 버리고 이후 offset을 커밋하는 복구는 금지한다.
동기 flush·재시도 총 시간은 `max.poll.interval.ms`보다 짧게 제한하고 초기값은 부하 검증한다.
정상 종료·revoke 시 소유 중인 파티션만 같은 절차로 마감한다. 소유권을 이미 잃은 경우
커밋하지 않고 새 Consumer가 재소비하도록 한다.

manifest가 확정된 파일만 조회 입력으로 삼는다. orphan 파일과 임시 파일은 즉시 읽거나
삭제하지 않고, 실행 중 작업 참조와 유예 기간을 확인하는 별도 정리 대상으로 둔다.

## 6. quarantine과 통합 조회·중복 기준

물리적으로 다른 데이터셋을 조회용 공통 모델로 매핑한다.

| 공통 필드 | Bronze quarantine | DLQ archive |
|---|---|---|
| `failure_stage` | `bronze_ingest` | 저장된 `analysis` / `monitoring` |
| `source_topic/partition/offset` | `topic/partition/kafka_offset` | envelope의 `source_*` |
| `archive_topic/partition/offset` | 해당 없음 | DLQ의 `topic/partition/kafka_offset` |
| `error_code` | 적재 검증 오류 | 서비스 오류 |
| `parse_status` | 해당 없음 | DLQ 해석 상태 |
| `raw_payload_kind` | `SOURCE_BYTES` | `DLQ_ENVELOPE_BYTES` |

- 물리 재적재 중복: quarantine은 원천 `(topic, partition, offset)`, DLQ는 **DLQ 위치**로
  제거한다. 같은 원천이 여러 번 DLQ 발행된 경우 DLQ 위치가 다르므로 원래 이력은 유지한다.
- 영향받은 원천 건수: 유효한 `source_*` 기준으로 별도 distinct 집계한다. 실패 단계별
  발생 건수와 구별하고, source가 불명인 행은 미확인 건수로 별도 집계한다.
- quarantine과 analysis DLQ의 같은 원천은 연결하되 실패 단계는 유지한다. monitoring의
  source는 분석 이벤트/스냅샷 위치이므로 power 원천 offset과 직접 조인하지 않는다.
- 같은 Kafka 위치의 내용이 다르면 정상 중복으로 취급하지 않고 무결성 오류로 보고한다.
- 일일 학습/베이스라인 입력에는 격리 데이터와 DLQ를 직접 포함하지 않는다.

초기 구현은 한 Kafka 클러스터 기준이다. 여러 클러스터를 합치거나 토픽을 삭제 후 재생성하면
위치가 재사용될 수 있으므로 namespace에 cluster ID/topic 세대를 추가한 뒤 통합한다.

## 7. 재처리와 운영

Loader의 역할은 보관까지다. 재처리는 별도 작업으로 원천 위치·실패 단계·대상 서비스·
수정된 계약을 확인하고 실행 이력을 남긴다. DLQ envelope 전체를 원천 토픽에 넣지 않는다.
손상된 원천 바이트 복구에는 Bronze/quarantine을 우선 사용하며 monitoring의 원천은
별도 원천 보관 여부를 확인한다. 이미 처리된 이벤트의 부작용 중복 방지도 재처리 조건이다.

주요 관측 항목은 토픽/파티션별 lag, 마지막 성공 flush 시각, 저장 행 수, parse_status별
건수, 저장/커밋 실패, 재시도, 버퍼 바이트, HDFS 잔여 공간이다. 메시지 payload나 offset을
메트릭 label에 넣지 않는다. 입력이 없는 토픽의 무수신과 적재 장애를 구분한다.

Kafka retention은 최대 저장 장애 시간과 따라잡기 시간보다 길게 잡는다. HDFS DLQ 및
quarantine 보존 기간은 복구 요구·원천 보존 기간·용량을 함께 검토해 확정한다.

## 8. 구현 순서와 검증 기준

1. `loader.py`, `requirements.txt`, `Dockerfile`을 이 디렉터리에 추가하고,
   `infrastructure/hdfs/docker-compose.yml`에 독립 서비스를 연결한다.
   환경변수는 `KAFKA_TOPICS`, `GROUP_ID`, `KAFKA_BOOTSTRAP`, `HDFS_URL`, `DLQ_BASE`,
   `MANIFEST_BASE`, `FLUSH_SECS`, `MAX_BUFFER`, `MAX_BUFFER_BYTES`, `MAX_TOTAL_BUFFER_BYTES`다.
2. 두 토픽의 동일 partition/offset을 입력해 파일·버퍼·커밋 분리를 검증한다.
3. 정상 analysis envelope, monitoring 미지원 형식, 잘못된 JSON/UTF-8, 필드 오류,
   null/빈 value, 중복 header, 확장 필드를 넣어 원본 보존과 해석 상태를 확인한다.
4. 업로드·검증·rename·manifest·커밋 각각에 실패를 주입해 조기 커밋이 없음을 확인한다.
5. manifest 이후 commit 전 종료, 시간 경계, 재시작, revoke/소유권 상실을 시험하고
   재소비 중복 제거 후 입력과 출력 건수를 대사한다.
6. 같은 power 원천의 quarantine/analysis DLQ가 조회에서는 연결되면서 단계별 이력으로
   보존되는지 확인한다. monitoring 발행 구현 후 실제 producer와 통합 검증한다.

기존 Bronze Loader의 파티션 번호만 사용하는 버퍼, flush 시점의 날짜 선택,
저장 전 버퍼 pop, 최종 파일 delete 후 rename 방식은 신규 Loader에 그대로 복사하지 않는다.
공통 저장 모듈 추출은 신규 저장 절차 검증 후 Bronze의 회귀 검증을 포함한 후속 작업으로 한다.

## 코드 근거

- [Bronze Loader](../bronze-loader/loader.py)
- [analysis DLQ publisher](../../ai/realtime-analysis-service/src/realtime_analysis/dlq.py)
- [analysis consumer](../../ai/realtime-analysis-service/src/realtime_analysis/consumer.py)
- [analysis schema](../../ai/realtime-analysis-service/src/realtime_analysis/schemas.py)
- [monitoring 오류 처리](../../backend/monitoring-service/src/main/java/com/nilm/monitoring/config/KafkaConsumerConfig.java)
- [HDFS Compose](../../infrastructure/hdfs/docker-compose.yml)
