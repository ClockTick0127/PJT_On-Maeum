# HDFS 수집·일일배치 설계안

작성일: 2026-09-14

## 1. 목적과 적용 범위

시뮬레이터 원천 전력을 HDFS에 보관하고, 매일 수집 데이터를 마감한 뒤 가구별 가전 사용 베이스라인을 계산한다. 실시간 패턴 감지와 일일 배치는 독립적으로 실행한다.

이 문서는 운영 원천 저장소를 HDFS로 구성하는 **신규 제안**이다. 기존 최종 아키텍처의 운영 S3·노트북 HDFS 구성을 이미 변경·배포했다고 의미하지 않는다. 코드 변경과 서버 배포는 이 문서의 범위에 포함하지 않는다. 서버 이름은 대화에서 사용하는 A/B를 유지하며, 현재 인스턴스 상품·vCPU 수는 별도 확인한다.

- HDFS: 원천, 격리 데이터, 마감 파일, 배치 산출물 보관.
- PostgreSQL `analysis_db`: 일일 품질·가전 사용 요약, 현재 적용 기준, 배치 실행 메타데이터.
- 실시간 패턴 감지: 원천 전력으로 가전 상태를 판단하고 사용 이력을 기록.
- 일일 배치: 원천의 완전성과 사용 이력을 검증하고 다음 날 적용할 기준을 계산.
- S3: 기존 버킷을 유지하고 모델·Manifest 및 선택적 백업 용도로 활용. 모든 HDFS 원천을 S3에도 이중 적재하는 것은 기본 범위에서 제외.

## 2. 현재 코드에서 확인한 사실

| 항목 | 현재 구현 | 본 설계에서 필요한 변경 |
|---|---|---|
| CLI 시뮬레이터 | 기본 10가구, 가구당 1초 1건. `--houses`, `--interval`, `--hz`로 변경 | 실행 조건과 기간을 실험 기록에 보존 |
| 웹 시뮬레이터 | 선택한 한 가구, 1초 주기 | 다가구 지속 수집은 CLI로 수행 |
| 원천 토픽 | B Compose 기본 `power.raw.v1` | Loader의 기본 `power-raw`와 통일 |
| HDFS Compose | 로컬 NameNode 1개, DataNode 1개, 복제 수 1 | A/B DataNode 2개, 복제 수 2로 별도 배포 구성 |
| HDFS Loader | 60초 또는 50,000건에 flush, 가구/날짜별 Snappy Parquet | 전체 원천 필드·Kafka 위치 보존, 파일 병합, 오류 격리 |
| Loader 보존 필드 | `house`, `device`, `ts`, `power_w`만 저장 | 4개 모델 입력 특징 및 원본 payload 보존 |
| Loader 오류 처리 | JSON/필수값 오류를 로그 출력 후 skip | 격리 저장 성공 전에는 해당 입력을 커밋하지 않음 |
| 일일 관측 | `household_observation_daily`에 수집 건수 기록 | HDFS 마감 결과로 중복·지연·누락을 정산 |
| 기존 마감 | 새 날짜 메시지를 받으면 이전 `COLLECTING` 행의 수집률로 마감 | 메시지가 오지 않아도 동작하는 예약 배치 추가 |
| 가전 사용 이력 | `household_activity_daily`, `appliance_usage_session` 기록 | 일일 배치에서 정합성·미종료 세션·추론 누락 검증 |
| 베이스라인 | `routine_baseline` ORM 존재. 실행 경로는 `BaselineRepository.from_json_file()` | 계산 결과 저장과 실행 서비스 적용 경로 연결 |
| 초기 기준 | 현재 JSON은 H001~H003 전자레인지, 08:10, 12/14일 값 | 통계로 계산된 결과와 초기 설정을 구별 |

기존 문서의 `baseline.v1` Broadcast State는 과거 아키텍처 계획이다. 현재 코드가 해당 토픽을 구독하여 기준을 자동 갱신한다고 가정하지 않는다.

## 3. 처리 흐름

```text
시뮬레이터 → Mosquitto → MQTT-Kafka Bridge → Kafka power.raw.v1
                                               ├─ 실시간 패턴 감지
                                               │    └─ analysis_db 사용 이력·관측 요약
                                               └─ 원천 보관 Consumer
                                                    └─ HDFS Bronze / Quarantine

매일 예약 실행
  → 입력 파일·Kafka offset 범위 확정
  → 원천 검증·중복 제거·일일 품질 정산
  → HDFS Silver와 analysis_db 일일 결과 확정
  → 유효한 과거 일별 사용 이력으로 베이스라인 계산
  → 버전별 결과 보관 + 현재 적용 버전 전환
  → 실시간 패턴 감지 서비스가 새 버전 로드
```

원천의 전체 전력만 합산해서 가전별 사용 여부를 직접 얻을 수는 없다. 가전 루틴 베이스라인에는 **패턴 감지로 확정한 가전 사용 이력**이 필요하다. `analysis.event.v1`의 위험 이벤트만으로 정상 사용일을 복원하지 않는다.

## 4. 서버 배치와 초기 자원 예산

| 항목 | A | B |
|---|---:|---:|
| 현재 디스크 여유 | 약 291GiB | 약 303GiB |
| 메모리 전체 / available | 약 15GiB / 11GiB | 약 15GiB / 12GiB |
| HDFS 제안 배치 | DataNode, 예약 배치 | NameNode, DataNode, 원천 보관 Consumer |
| HDFS 데이터 초기 예산 | 150GiB | 150GiB |

- 기존 서비스를 유지한 채 HDFS 프로세스를 추가하는 제안이다. 메모리·CPU·디스크 I/O 부하 테스트 후 배치를 조정한다.
- B에 NameNode 힙 1GiB, 각 DataNode 힙 512MiB~1GiB, A 배치 동시 실행 1개·메모리 2~4GiB를 초기 시험값으로 사용한다. JVM 힙과 프로세스 전체 메모리는 다르므로 컨테이너 제한에는 여유를 둔다.
- 초기 배치는 단일 Python/PyArrow 작업으로 구현한다. 분산 저장과 분산 연산은 별개이며, Spark A/B 워커 비교 실험은 기본 파이프라인 검증 후 추가한다.
- A/B의 DataNode 저장 경로는 명시적 영속 볼륨으로 지정한다. 두 Compose의 같은 네트워크 이름만으로 서버 간 통신이 연결되지는 않는다.
- NameNode RPC와 DataNode 데이터 전송/WebHDFS 주소가 두 서버·배치 클라이언트에서 모두 사설망으로 도달해야 한다. WebHDFS의 DataNode 리다이렉트도 검증한다. 실제 포트는 배포 이미지 설정에 맞춰 확정하고 외부 공개를 막는다.
- 로컬 Compose의 `dfs.permissions.enabled=false`, `root` 접근을 그대로 운영 보안 구성으로 간주하지 않는다. 저장 경로 권한과 서비스 계정을 분리한다.

복제 수 2이면 파일 150GiB를 A와 B에 각각 보관하여 총 300GiB를 사용한다. 150GiB는 Bronze뿐 아니라 Silver·Gold·Quarantine을 모두 포함하는 논리 용량 예산이다. HDFS 디렉터리 space quota는 복제본 공간까지 계산하므로 150GiB 논리 예산을 그대로 space quota 값으로 넣지 않는다. 디스크별 예약 공간·사용량 경보도 별도 설정한다.

DataNode 하나가 중단되어도 다른 복제본을 읽을 수 있는 조건은 NameNode가 정상이고 해당 블록이 두 서버에 실제 복제되어 있는 경우다. B 서버 전체가 중단되면 단일 NameNode도 중단된다. **이 구성은 HA가 아니다.** 기존 문서의 “서버 두 대에서는 복제가 불가능하다”는 설명은 복제 수 2 구성에는 적용되지 않는다.

## 5. 용량과 실험 규모

계산식: `가구 수 × 초당 메시지 수 × 86,400 × 보관 일수 × 평균 저장 바이트`.

| 가구 수, 1Hz | 하루 건수 | 14일 건수 | 14일 원본 용량, 500~1,000B/건 |
|---|---:|---:|---:|
| 10 | 864,000 | 12,096,000 | 약 5.6~11.3GiB |
| 100 | 8,640,000 | 120,960,000 | 약 56~113GiB |
| 1,000 | 86,400,000 | 1,209,600,000 | 약 563~1,127GiB |

500~1,000B는 아직 실측하지 않은 예시다. JSON 크기와 압축 Parquet의 행당 크기는 다르며, 원본 payload와 정규화 컬럼을 함께 저장하면 크기도 달라진다. “1.6억~3.2억 건”은 150GiB를 원천에만 사용하는 산술 상한이지 전체 파이프라인 보장 용량이 아니다.

초기에는 기본 10가구로 24시간 검증 후 100가구·14일 목표의 적합성을 다시 계산한다. 1,000가구는 우선 1시간 부하 테스트(360만 건)로 제한한다. 보관 기간을 14일로 확정한 것은 아니다.

실측 항목:

1. MQTT payload, Kafka 보관 데이터, Bronze Parquet의 하루 증가량.
2. Silver·격리 데이터·일일 배치 임시 파일의 최대 사용량.
3. HDFS 복제 포함 사용량과 A/B 루트 디스크의 전체 사용량.
4. Consumer lag, 추론 지연, 배치 소요 시간, 메모리·CPU 최고치.

서버 디스크 70% 사용 시 경고, 80% 사용 시 부하 실험 중단을 초기 운영 기준으로 제안한다. 보관 Consumer를 무작정 중지하면 Kafka에 적체되므로 먼저 시뮬레이터 유입량을 줄인다. 확정 처리량은 측정 결과에만 근거한다.

## 6. 원천 보관 설계

### 6.1 보존 데이터

| 필드 | 목적 |
|---|---|
| `message_id`, `household_id`, `device_id`, `measured_at` | 메시지 식별, 가구·장치·측정시각 |
| `active_power`, `reactive_power`, `power_factor`, `current` | 현재 모델 입력 4개 특징 보존 |
| `voltage`, `apparent_power` | 추가 계측값 보존 |
| `raw_payload` (binary) | UTF-8/JSON 해석 실패도 포함하여 수신 바이트 복구 |
| `topic`, `partition`, `offset`, `kafka_timestamp`, `ingested_at` | 재처리 위치, 지연 추적 |
| `schema_version`, `validation_status`, `error_code` | 해석 규격과 격리 사유 |

Bronze에서는 수신 데이터를 삭제·덮어쓰지 않는다. `house/device/ts/power_w` 호환 필드도 raw payload에서 복구할 수 있다. 같은 의미의 컬럼을 Silver에 중복 유지할 필요는 없다.

### 6.2 파일 구성과 안전한 커밋

```text
/nilm/bronze/power/ingest_date=YYYY-MM-DD/hour=HH/partition=P/...
/nilm/quarantine/power/ingest_date=YYYY-MM-DD/partition=P/...
/nilm/silver/power/event_date=YYYY-MM-DD/run_id=.../...
/nilm/gold/daily/event_date=YYYY-MM-DD/run_id=.../...
/nilm/gold/baseline/version=.../...
/nilm/manifests/job=.../date=YYYY-MM-DD/run_id=.../...
```

Bronzeは到着日、Silver・일별 결과는 `Asia/Seoul` 측정일로 나눈다. 파일 내부 시각은 UTC로 보존한다. 가구별 디렉터리는 초기 Bronze에서 사용하지 않고 가구 ID는 컬럼으로 둔다.

기존 60초×가구별 flush 방식이면 100가구에서 하루 약 144,000개 파일이 생긴다. 따라서 파티션 단위로 여러 가구를 묶고, 초기 flush 조건은 5분 또는 50,000건 중 먼저 도달하는 조건으로 제안한다. 바이트 기준 메모리 상한도 함께 둔다. 일일 배치에서는 작은 파일을 병합하고 64~128MiB 파일 크기를 목표로 시험한다. 저유량에서 이 크기를 채우려고 무한 대기하지 않는다.

저장 순서:

1. Kafka 파티션별 offset 구간을 추적하고 정상·오류 레코드를 각각 파일에 기록한다.
2. HDFS 임시 경로에 업로드하고 close 성공·파일 크기·checksum을 확인한다.
3. 최종 경로로 rename한 뒤 구간·파일·건수를 담은 완료 manifest를 확정한다.
4. 정상 또는 격리 저장이 완료된 **연속 offset 구간의 다음 offset**만 파티션별 커밋한다.
5. 재시작 시 manifest와 구간을 확인한다. 재처리 구간 경계가 바뀌어도 Silver에서 중복 제거한다.

자동 offset commit을 끄고, rebalance·종료 시에도 완료된 offset만 커밋한다. `message_id`로 논리 중복을 제거하며 동일 ID의 내용이 다르면 충돌로 격리한다. ID 없는 잘못된 원천은 `(topic, partition, offset)`으로 식별한다. 전 구간 exactly-once를 주장하지 않는다.

현재 Loader는 초 단위 파일명과 `overwrite=True`를 사용하므로 충돌·재처리 안전성을 보완해야 한다. 코드 주석의 “유실 없음”을 실제 보장으로 간주하지 않는다. Kafka 보관 만료 전에 적재하지 못하면 복구 불가능할 수 있다.

DLQ 토픽 보관 Consumer는 별도 consumer group으로 운영할 수 있다. 토픽명·오류 envelope는 실제 producer 계약을 확인해 연결하며, 거주자 모니터링 화면에 인프라 오류 처리를 맡기지 않는다.

## 7. 일일 마감

### 7.1 실행과 마감의 의미

초기 제안은 매일 **00:30 KST**, 전날 D의 `[00:00, 다음 날 00:00)` 데이터를 대상으로 실행하는 것이다. 30분은 지연 허용의 초기값이며 측정 후 조정한다.

마감은 파일을 닫는 것만을 의미하지 않는다. 입력 범위를 고정하고 중복·누락·품질을 계산한 뒤, 그 날짜의 결과 버전을 확정하는 작업이다.

1. 실행 시작 시 Kafka 파티션별 목표 offset을 기록한다.
2. Archive Sink가 해당 범위를 보존했는지 확인한다. lag가 남아 있으면 마감을 대기·실패 처리한다.
3. manifest 목록으로 입력 스냅샷을 고정한다. 도착일 파티션은 측정일과 다를 수 있어 manifest 시각 범위를 이용해 관련 파일을 찾는다.
4. 측정시각 KST 기준 D 데이터만 추출하고 검증·중복 제거한다.
5. Silver·품질 결과를 새 run 경로에 기록하고 완료 manifest를 확정한다.
6. DB 일일 결과를 트랜잭션으로 갱신한다. DB 반영 실패 시 같은 확정 파일로 재시도한다.
7. 필요한 품질 조건이 충족된 결과만 베이스라인 계산에 제공한다.

벽시계 00:30이 됐다는 이유만으로 입력이 완전하다고 판정하지 않는다. 실험 가상 날짜 데이터는 자동 운영 마감과 분리하고 `--date`로 명시적으로 마감한다.

### 7.2 품질과 가전 이력

- 하루 1Hz 전체 참여 가구의 기대 건수는 86,400건이다. 중간 등록·중지·주기 변경은 실제 관측 예정 구간에 따라 기대 건수를 계산한다.
- 원천 품질은 유효하고 중복되지 않은 시간 슬롯 수/기대 슬롯 수로 계산한다. 중복 메시지로 수집률을 높이지 않는다.
- 등록된 수집 대상 목록을 기준으로 행을 생성해 **아예 메시지가 없는 가구**도 `SENSOR_GAP`으로 기록한다. 대상 목록의 원본과 관측 예정 시간을 별도 계약으로 확정해야 한다.
- 기존 `household_observation_daily` 상태를 재사용하되 95% 수집률을 초기 유효 기준으로 제안한다. 평균 수집률 외에 최대 연속 누락과 해당 가전의 판단 시간대 누락도 검사한다.
- 원천이 있어도 추론 실패·미처리 구간이 있으면 가전 미사용으로 해석하지 않는다. 추론 처리 범위와 오류 집계 계약을 추가하고, 확인할 수 없으면 기준 계산에서 제외한다.
- `appliance_usage_session`과 `household_activity_daily`를 정산한다. 확정 ON이 자정을 넘어 유지되는 세션은 다음 날 사용에도 반영한다. 미종료 세션을 무기한 사용으로 간주하지 않고 마지막 정상 관측·상태 및 재처리로 검증한다.
- 현재의 메시지 도착 기반 마감과 신규 배치가 충돌하지 않도록, 이전 날짜의 최종 결과는 배치가 소유하도록 변경한다. 마감 후 지연 데이터는 dirty date로 기록하고 새 revision으로 재마감한다.

재마감은 해당 날짜 Silver와 이후 영향받는 베이스라인을 새 버전으로 만든다. 과거 위험 이벤트에 기록한 판단 기준은 덮어쓰지 않는다.

## 8. 베이스라인 계산과 적용

### 8.1 초기 계산 규칙 제안

초기 구현은 새로운 루틴 시간대를 자동 발견하는 알고리즘까지 포함하지 않는다. **설정된 가전별 마감시각까지 평소 얼마나 자주 사용하는지**를 최근 관측 이력으로 계산한다. `08:10`은 현재 예시 설정이지 모든 가구에서 학습된 시각이 아니다.

| 항목 | 초기 제안 |
|---|---|
| 조회 범위 | 마감된 D를 포함한 최근 14개 달력일 |
| 유효 표본 | 원천·추론·해당 시간대 품질이 유효한 날 |
| `sample_days` | 유효 표본 일수 |
| `active_days` | 해당 날 00:00부터 `expected_until`까지 확정 사용이 있는 유효일 수 |
| `daily_use_probability` | `active_days / sample_days` |
| 최소 표본 | 7일 미만이면 신규 활성화하지 않음 |
| `reliability_weight` | 초기 제안 `sample_days / 14`, 정책 버전으로 보관 |

이 수치들은 운영 검증 전 제안이며 코드의 기존 고정값과 다르다. 자료가 없는 날을 미사용일로 세지 않는다. 하루 전체 사용 빈도와 마감시각 이전 사용 빈도도 섞지 않는다. 향후 최초 사용시각 분위수 등으로 마감시각을 자동 산정하려면 별도 알고리즘·평가가 필요하다.

현재 `RoutineBaseline` 입력과의 명시적 변환은 `normal_days=active_days`, `window_days=sample_days`로 한다. 후자의 이름은 최근 14개 달력일이라는 뜻과 다르므로 변환 계층에서 문서화한다. 표본 0일에는 변환하지 않는다. 현재 탐지 점수는 `(normal_days/window_days) × 100 × reliability_weight`이므로 신규 가중치가 실제 알림 빈도에 주는 영향도 시험한다.

현재 tracker의 하루 사용 여부는 지연 입력·재시작 복원·자정 이월·마감시각 경계에 대한 검증이 필요하다. 배치와 실시간 서비스가 동일한 “마감시각까지 사용” 의미를 적용하도록 사용시각 기반 조회 계약을 맞춘다.

### 8.2 저장과 갱신

- `routine_baseline`: 현재 기준으로 upsert. 기존 unique key `(household_id, appliance_type, baseline_type)`를 유지한다.
- `baseline_data`: `expected_until`, `history_start/end`, `algorithm_version`, `dataset_revision`, `baseline_version`, `source_run_id`, 품질 설정을 보존한다. 이는 새 JSON 계약이다.
- HDFS Gold: 버전별 기준 전체와 계산 근거를 변경 불가능한 산출물로 보존한다. DB 현재 행만으로 과거 기준을 복구하려 하지 않는다.
- 현재 JSON 파일 로딩만으로는 DB 갱신이 실시간 서비스에 반영되지 않는다. 1차 구현에 **DB-backed BaselineRepository와 주기적 버전 reload**를 포함한다.
- 새 버전 전체를 검증한 후 메모리 기준 목록을 원자적으로 교체한다. 실패하면 기존 버전을 유지하고 운영 오류를 기록한다. 새 버전 적용 시각은 아침 판정 이전으로 고정하고, 늦어진 계산은 다음 적용 시점으로 넘긴다.
- 분석 이벤트 계약에도 `baseline_version`, `algorithm_version` 또는 이에 대응하는 식별자를 추가해 당시 기준을 보존한다.
- 초기 고정 JSON은 seed/fallback으로만 취급한다. 실제 계산 결과와 출처를 구별한다. 유효 표본 부족 시 기존 기준을 무기한 유지하지 않도록 유효기간·비활성화 정책을 별도 설정한다.

## 9. 배치 실행 기록과 복구

신규 `batch_run` 테이블을 제안한다. 원천 행을 SQL에 다시 저장하지 않고 실행·재시도·적용 상태만 기록한다.

| 컬럼 | 이유 |
|---|---|
| `run_id`, `job_name`, `target_date`, `revision` | 실행 및 마감 버전 식별 |
| `status` | `RUNNING/SUCCEEDED/FAILED`, 재시도 판단 |
| `input_manifest_uri`, `output_manifest_uri` | 재현 가능한 입출력 범위 |
| `algorithm_version` | 계산 규칙 추적 |
| `started_at`, `finished_at`, `error_code` | 처리 시간과 실패 원인 |
| `input_count`, `valid_count`, `duplicate_count`, `quarantine_count` | 건수 대사 |
| `applied_at` | 산출 성공과 서비스 적용을 구별 |

예약 작업은 A의 systemd timer 또는 cron 하나로 시작한다. `(job_name, target_date)` 중복 실행을 DB advisory lock 등으로 막고, 결과는 revision을 올려 발행한다. 파일 완료 후 DB 트랜잭션 반영 순서로 재실행을 설계한다. HDFS와 PostgreSQL 사이의 분산 트랜잭션을 가정하지 않는다.

Kafka 보관 기간은 최대 HDFS 장애 시간과 따라잡기 시간보다 길어야 한다. 구체적인 보관 시간은 B 디스크와 실측 유입량을 확인한 뒤 정한다. NameNode 메타데이터뿐 아니라 복원 절차·edits·checkpoint 일관성을 포함한 백업 계획이 필요하다. DataNode 복제는 삭제·논리 오류 백업을 대체하지 않는다.

## 10. 보관 정책

- 초기 Bronze 보관 목표는 14일이며, late data 재마감 유예를 추가해야 하므로 실제 유지 일수와 용량은 함께 재산정한다.
- 14일 베이스라인은 작은 일일 요약을 읽는다. 이미 품질 검증된 일일 요약은 원천보다 길게 보관할 수 있다.
- 원천 삭제 후에는 모델·전처리 규칙을 바꿔 과거를 완전히 재추론할 수 없다. 재현이 필요한 기간은 선택적으로 S3 등에 보존한다.
- 삭제 작업은 성공한 마감 manifest, 실행 중 배치 참조 여부, 유예 기간을 모두 확인한다. 용량이 부족하다는 이유만으로 미처리 원천을 먼저 삭제하지 않는다.
- HDFS에는 S3 lifecycle 설정이 그대로 적용되지 않는다. 삭제·병합·Trash 만료까지 배치로 관리하고, Trash가 차지하는 공간도 포함한다.

## 11. 구현 순서와 완료 기준

1. **저장 기반:** A/B 사설망 연결·영속 볼륨·복제 2 검증. `hdfs fsck`와 DataNode 장애 시험으로 실제 복제 확인.
2. **원천 보관 개선:** 토픽 통일, 전체 payload·offset·오류 격리, manifest 후 commit, 중복·rebalance·재시작 시험.
3. **일일 마감:** KST 경계, 대상 목록, 중복·역순·미수신·지연 데이터, 파일 병합, 반복 실행 정합성 시험.
4. **가전 이력 정산:** 원천은 정상이나 추론 실패한 날, 자정 이월과 미종료 세션을 시험. 누락을 미사용으로 세지 않음.
5. **베이스라인:** 통제된 14일 fixture로 sample/active 일수 검증, 최소 표본·버전·reload·이전 기준 복원 시험.
6. **용량 실측:** 10가구 24시간 후 100가구 확장 여부 판단. 1,000가구 1시간 부하 테스트는 별도 수행.
7. **분산 비교:** 동일 입력·출력 규칙의 단일 배치와 Spark 분산 배치를 비교. HDFS 복제 수와 Spark 워커 수 효과를 분리해 보고.

성공 기준은 원천/격리/중복 건수 대사가 일치하고, 동일 입력 재실행 결과가 같으며, 잘못된 날이 기준 계산에서 제외되고, 새 기준이 서비스에 반영되는 것까지다. 저장 용량으로 처리 TPS를 추정하거나 생성한 데이터 건수만으로 분산 성능을 주장하지 않는다.

## 12. 코드 및 문서 근거

- [CLI 시뮬레이터](../../../../infrastructure/mqtt/simulator/simulator.py)
- [웹 시뮬레이터 실행 관리](../../../../infrastructure/mqtt/simulator/server/manager.py)
- [MQTT payload 생성](../../../../infrastructure/mqtt/simulator/engine/publisher.py)
- [현재 HDFS Compose](../../../../infrastructure/hdfs/docker-compose.yml)
- [현재 원본 적재기](../../../../collection/bronze-loader/loader.py)
- [B 서비스 Compose](../../../../infrastructure/ec2-b/compose.yaml)
- [분석 DB ORM](../../../../ai/realtime-analysis-service/src/realtime_analysis/models.py)
- [관측 및 활동 저장](../../../../ai/realtime-analysis-service/src/realtime_analysis/activity_repository.py)
- [현재 JSON 기준 저장소](../../../../ai/realtime-analysis-service/src/realtime_analysis/baseline.py)
- [현재 기준 스키마](../../../../ai/realtime-analysis-service/src/realtime_analysis/schemas.py)
- [현재 이상 판정](../../../../ai/realtime-analysis-service/src/realtime_analysis/anomaly_detector.py)
- [기존 최종 아키텍처 4안](../0903/NILM_최종_아키텍처_4안.md)
- [기존 WBS](../0905/NILM_3안_백엔드_WBS_1주.md)

이 문서의 코드 현황은 2026-09-14 로컬 작업 트리 확인 기준이다. 서버 배포 버전이 동일한지는 별도 확인해야 한다.

