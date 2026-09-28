# Kafka 다중 Consumer 리밸런싱 트러블슈팅

> 작성일: 2026-09-20  
> 작성자: 최보경  
> 대상 서비스: `ai/realtime-analysis-service`  
> 환경: 로컬 Docker Compose, Kafka 3.9.0, `confluent-kafka==2.6.0`

## 1. 작업 목적

`realtime-analysis-service`를 1개에서 2개, 4개로 증설하면서 다음 동작을 검증한다.

- `cooperative-sticky` 파티션 할당
- revoke된 파티션에 속한 가구 상태만 초기화
- 유지된 파티션의 가구 상태 보존
- 이동한 가구만 299개 샘플을 다시 수집
- 리밸런싱 중 Consumer 프로세스 유지
- Prometheus에서 Consumer별 파티션, 처리량, lag, warm-up 관측

최종적으로 기대하는 1개에서 2개 증설 흐름은 다음과 같다.

```text
기존 Consumer: 24개 파티션
  -> 이동할 12개 파티션만 revoke
  -> 해당 파티션의 가구 상태만 초기화
  -> 나머지 12개 파티션 상태 유지

신규 Consumer:
  -> 이동한 12개 파티션 assign
  -> 해당 파티션의 가구만 299개 샘플 warm-up
```

---

## 2. 첫 번째 장애: 리밸런싱 중 Consumer 반복 종료

### 2.1 증상

Consumer를 증설하자 다음 예외가 발생하면서 프로세스가 종료됐다.

```text
cimpl.KafkaException: KafkaError{
  code=ILLEGAL_GENERATION,
  val=22,
  str="Commit failed: Broker: Specified group generation id is not valid"
}
```

Docker의 재시작 정책 때문에 종료된 Consumer가 다시 실행되고, 재가입으로 리밸런싱이 다시
발생하는 현상이 반복됐다.

### 2.2 원인

메시지 처리 후 다음과 같이 offset을 동기 커밋하고 있었다.

```python
consumer.commit(message=message, asynchronous=False)
```

새 Consumer가 그룹에 가입하면 Consumer Group generation이 변경된다. 기존 generation으로
처리 중이던 메시지가 이 시점에 동기 커밋되면 `ILLEGAL_GENERATION`이 발생할 수 있다. 예외를
처리하지 않았기 때문에 Consumer 실행 루프까지 전파되어 프로세스가 종료됐다.

### 2.3 1차 수정

다음 오류만 리밸런싱 경합으로 분류해 경고를 기록하고 프로세스를 유지하도록 수정했다.

- `ILLEGAL_GENERATION`
- `REBALANCE_IN_PROGRESS`
- `UNKNOWN_MEMBER_ID`

`on_lost` 콜백도 추가하여 정상 revoke가 완료되기 전에 파티션 소유권을 잃는 경우를 별도로
관측하도록 했다.

수정 후에는 다음 경고가 발생해도 프로세스가 종료되지 않았다.

```text
Offset commit skipped during rebalance:
topic=power.raw.v1 partition=12 offset=3620 error=ILLEGAL_GENERATION
```

이 단계에서 해결된 것은 **프로세스 종료와 재시작 반복**이었다. 이후 테스트에서
파티션 선택 초기화는 아직 해결되지 않았다는 사실을 확인했다.

---

## 3. Consumer lag가 계속 증가한 현상

### 3.1 관측 결과

초기 부하 테스트에서 시뮬레이터는 약 `900 msg/s`를 발행했지만 Consumer 1개의 실제 처리량은
약 `70 msg/s`였다. Consumer를 2개로 늘려도 입력량보다 전체 처리량이 작아 lag가 계속
증가했다.

```promql
sum(nilm_analysis_consumer_lag_messages{job="ai-analysis"})
```

이는 Consumer 장애가 아니라 다음 관계에 따른 정상적인 backlog 증가였다.

```text
Kafka 입력 속도 > 전체 Consumer 처리 속도
```

### 3.2 시뮬레이터를 중단한 뒤에도 잠시 증가한 이유

데이터 경로는 다음과 같다.

```text
Simulator -> MQTT -> mqtt-kafka-bridge -> Kafka -> Analysis Consumer
```

시뮬레이터를 종료해도 MQTT QoS 1과 bridge 내부에 남아 있던 데이터가 잠시 Kafka로 전달될 수
있다. 또한 당시 lag 지표는 `get_watermark_offsets(..., cached=True)`를 사용하므로 리밸런싱 직후
Consumer의 high watermark 캐시가 실제 Kafka 끝 지점을 따라가는 동안 값이 증가하는 것처럼
보일 수 있다.

실제 Kafka 상태는 다음 명령으로 `CURRENT-OFFSET`, `LOG-END-OFFSET`, `LAG`를 비교한다.

```cmd
docker exec nilm-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --group realtime-analysis-service-v1 --describe
```

판정 기준은 다음과 같다.

| 상태 | 의미 |
| --- | --- |
| `LOG-END-OFFSET` 증가 | MQTT 또는 bridge에서 Kafka로 계속 유입 중 |
| `LOG-END-OFFSET` 고정, `CURRENT-OFFSET` 증가 | Consumer가 backlog 처리 중 |
| 두 값 모두 고정 | Consumer 처리 상태 추가 확인 필요 |

시뮬레이터 중단 후 lag는 약 8만 건에서 점차 감소해 최종적으로 `0`이 됐다. 따라서 당시
Consumer는 멈춘 것이 아니라 backlog를 정상적으로 처리하고 있었다.

### 3.3 기능 검증 부하 조정

`100가구 x 10Hz = 약 1,000 msg/s`는 현재 처리 한도보다 높아 리밸런싱 기능 검증보다
backlog 부하 시험이 됐다. 선택적 상태 초기화 검증은 다음과 같이 낮은 부하로 분리했다.

```cmd
python simulator.py --scenario random --houses 20 --hz 1 --count 1200 --quiet
```

이 조건은 약 `20 msg/s`이며 lag를 거의 `0`으로 유지하면서 1Hz 기준 약 5분 후 299개
warm-up을 완료할 수 있다.

고부하 성능 비교는 기능 검증 후 별도 시나리오에서 입력량을 단계적으로 높여 수행한다.

---

## 4. 두 번째 장애: 전체 파티션 lost 및 전체 가구 초기화

### 4.1 증상

1개 Consumer가 20가구의 warm-up을 완료한 뒤 2개로 증설했을 때 최종 파티션 분배는
12/12였지만 기존 Consumer가 24개 파티션 전체를 `lost` 처리했다.

```text
Kafka partitions lost:
partitions=[power.raw.v1[0] ... power.raw.v1[23]]
reset_households=20
```

Prometheus에서도 다음 결과가 확인됐다.

```text
assign = 1
revoke = 0
lost = 1
```

이는 최종 파티션 분배만 보면 정상처럼 보이지만, 유지될 파티션의 가구 상태까지 모두
초기화하므로 요구사항을 충족하지 못한다.

### 4.2 제외한 원인

다음 항목을 확인했다.

- `MAXPOLL` 및 `max.poll.interval.ms` 초과 로그 없음
- `Shutdown requested`, `Kafka consumer stopped`, `Traceback` 없음
- 컨테이너 재생성 또는 종료 흔적 없음
- 런타임 설정은 `partition.assignment.strategy=cooperative-sticky`
- 로컬 static membership은 비활성화되어 `group.instance.id` 없음

런타임 설정 확인 명령:

```cmd
docker compose exec -T realtime-analysis-service python -c "from realtime_analysis.config import get_settings; print(get_settings().consumer_config())"
```

확인 결과:

```python
{
    'bootstrap.servers': 'kafka:19092',
    'group.id': 'realtime-analysis-service-v1',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    'partition.assignment.strategy': 'cooperative-sticky'
}
```

전체 파티션을 잃기 직전에 다음 로그가 있었다.

```text
Offset commit skipped during rebalance:
topic=power.raw.v1 partition=19 offset=12976 error=ILLEGAL_GENERATION

Offset commit skipped during rebalance:
topic=power.raw.v1 partition=19 offset=12977 error=ILLEGAL_GENERATION
```

### 4.3 최종 원인

프로젝트는 `confluent-kafka==2.6.0`을 사용한다. 이 버전에 포함된 librdkafka에는
cooperative incremental rebalance 중 수동 offset commit에서 `ILLEGAL_GENERATION`이 발생하면
추가 JoinGroup 요청으로 assignment 전체를 lost 처리할 수 있는 알려진 문제가 있다.

- 공식 이슈: <https://github.com/confluentinc/librdkafka/issues/4059>
- 수정 PR: <https://github.com/confluentinc/librdkafka/pull/4908>

이 문제는 librdkafka `v1.6.0`부터 존재한다고 기록돼 있다. 1차 수정처럼 예외를 잡아
프로세스 종료를 막아도, 라이브러리 내부 Consumer Group 상태가 이미 assignment lost로
전환되는 문제는 막을 수 없다.

---

## 5. 최종 수정: 처리 완료 offset 저장과 안정 상태 자동 커밋 분리

### 5.1 설정 변경

실시간 전력 Consumer와 외출 이벤트 Consumer 모두 다음 설정을 사용한다.

```python
"enable.auto.commit": True,
"enable.auto.offset.store": False,
```

두 옵션의 역할은 다르다.

- `enable.auto.offset.store=False`: 메시지를 poll한 즉시 offset을 처리 완료로 간주하지 않는다.
- `store_offsets(message=...)`: DB 저장, 이벤트 발행 또는 DLQ 발행이 성공한 메시지만 처리 완료로 표시한다.
- `enable.auto.commit=True`: librdkafka가 안정 상태에서 저장된 offset을 주기적으로 Kafka에 커밋한다.

처리 흐름은 다음과 같다.

```text
메시지 poll
  -> 역직렬화 및 검증
  -> 비즈니스 처리 또는 DLQ 발행
  -> 성공한 경우 store_offsets(message)
  -> librdkafka가 안정 상태에서 자동 commit
```

처리 또는 DB 저장이 실패하면 `store_offsets()`를 호출하지 않으므로 해당 메시지는 재처리된다.
프로세스가 offset 저장 후 실제 자동 커밋 전에 종료되면 일부 메시지가 다시 전달될 수 있으므로
at-least-once 전달 특성은 유지된다.

### 5.2 파티션 상실 경합 처리

메시지 처리가 끝난 시점에 이미 파티션을 잃어 `store_offsets()`가 `_STATE`를 반환하면
프로세스를 종료하지 않고 다음 경고와 오류 메트릭을 남긴다.

```text
Offset store skipped after partition loss
```

이 메시지는 커밋되지 않으므로 새 소유 Consumer에서 다시 처리할 수 있다.

### 5.3 관측 단계 이름 변경

메시지 처리 단계 이름을 실제 동작에 맞게 변경했다.

```text
offset_commit -> offset_store
```

이 값은 Kafka broker commit 왕복 시간이 아니라 처리 완료 offset을 로컬 Consumer 상태에
저장하는 시간이다.

### 5.4 테스트

추가 또는 수정한 검증 항목은 다음과 같다.

- 정상 메시지 처리 후 `store_offsets()` 호출
- 핸들러 실패 시 offset 미저장
- DLQ 발행 성공 후 offset 저장
- 수동 `consumer.commit()` 미호출
- 파티션 상실 후 `_STATE` 발생 시 프로세스 유지
- 외출 이벤트 Consumer에도 동일 계약 적용
- `on_lost` 발생 시 lost 파티션 가구만 안전하게 초기화

전체 테스트 결과:

```text
122 passed, 8 skipped
```

---

## 6. 최종 로컬 검증 결과

### 6.1 실행 순서

Consumer 1개를 새 이미지로 재생성했다.

```cmd
docker compose up -d --build --force-recreate --scale realtime-analysis-service=1 realtime-analysis-service
```

20가구, 1Hz 시뮬레이터를 실행하고 warm-up 완료 후 기존 Consumer를 재생성하지 않은 채
2개로 증설했다.

```cmd
docker compose up -d --scale realtime-analysis-service=2 --no-recreate realtime-analysis-service
```

### 6.2 성공 로그

```text
realtime-analysis-service-1
Kafka partitions revoked:
partitions=[power.raw.v1[0] ... power.raw.v1[11]]
reset_households=10

realtime-analysis-service-2
Kafka partitions assigned:
partitions=[power.raw.v1[0] ... power.raw.v1[11]]
```

결과는 다음과 같다.

- 기존 Consumer는 파티션 `0~11`만 revoke
- 신규 Consumer는 파티션 `0~11`을 할당받음
- 기존 Consumer의 파티션 `12~23` 상태는 보존
- 이동한 파티션의 가구 10개만 초기화
- `lost` 없음
- `ILLEGAL_GENERATION` 없음
- `Offset store skipped` 없음
- 최종 파티션 분배 12/12

로그의 `Kafka partitions assigned: partitions=[]`는 오류가 아니다. cooperative 방식의 콜백에는
전체 할당이 아니라 해당 단계에서 새로 추가되는 파티션만 전달된다. 기존 파티션을 유지하면서
새로 추가되는 파티션이 없으면 빈 목록이 전달될 수 있다.

### 6.3 Consumer 2개에서 4개로 증설

2개 Consumer의 lag가 `0`인 상태에서 다음 명령으로 4개까지 증설했다.

```cmd
docker compose up -d --scale realtime-analysis-service=4 --no-recreate realtime-analysis-service
```

실제 이동 결과는 다음과 같다.

```text
service-2 revoke: partition 0~5
reset_households=5

service-1 revoke: partition 12~17
reset_households=3

service-3 assign: partition 1, 3, 5, 13, 15, 17
service-4 assign: partition 0, 2, 4, 12, 14, 16
```

기존 Consumer 2개는 각각 이동할 파티션 6개만 반납했고, 신규 Consumer 2개는 각각 6개를
할당받았다. 이동한 가구는 총 `5 + 3 = 8`개였으며 나머지 12가구의 상태는 보존됐다.

- 최종 Consumer 수: 4
- 최종 파티션 분배: 6/6/6/6
- 파티션 합계: 24
- 이동한 가구 초기화: 8
- `lost`: 없음
- `ILLEGAL_GENERATION`: 없음
- `Offset store skipped`: 없음
- 최종 lag: 0 확인

### 6.4 Consumer 4개에서 2개로 축소

기존 Consumer를 재생성하지 않고 다음 명령으로 2개까지 축소했다.

```cmd
docker compose up -d --scale realtime-analysis-service=2 --no-recreate realtime-analysis-service
```

종료된 `service-3`, `service-4`의 파티션은 다음과 같이 기존 Consumer로 돌아갔다.

```text
service-2 additional assign: partition 1, 3, 5, 13, 15, 17
service-1 additional assign: partition 0, 2, 4, 12, 14, 16
```

각 Consumer가 기존 6개에 추가 6개를 받아 12/12 상태로 복구됐다. 이 과정에서도
`lost`, `ILLEGAL_GENERATION`, `Offset store skipped`은 발생하지 않았다.

### 6.5 Consumer 2개 중 1개 종료

2개 상태에서 다음 방식으로 `service-1`을 통제 종료했다.

```cmd
docker stop <service-1-container-id>
```

실제 로그는 다음과 같다.

```text
service-1 revoke:
partition 0, 2, 4, 12, 14, 16, 18, 19, 20, 21, 22, 23
reset_households=12

service-1 Kafka consumer stopped
service-1 Outing event consumer stopped

service-2 additional assign:
partition 0, 2, 4, 12, 14, 16, 18, 19, 20, 21, 22, 23
```

남은 `service-2`가 종료된 Consumer의 파티션 12개를 모두 인수해 최종적으로 전체 24개를
보유했다. `docker stop`은 SIGTERM을 전달하므로 이 결과는 프로세스 강제 장애가 아니라
**통제된 Consumer 종료와 파티션 인수** 검증이다.

### 6.6 Consumer 1개에서 2개로 복구

중단한 Consumer를 포함해 다시 2개로 복구했다.

```cmd
docker compose up -d --scale realtime-analysis-service=2 --no-recreate realtime-analysis-service
```

실제 리밸런싱 결과는 다음과 같다.

```text
service-2 revoke: partition 0~11
reset_households=10

service-1 assign: partition 0~11
```

기존 `service-2`는 이동할 파티션 12개만 반납하고, 그 파티션에 속한 10가구만 초기화했다.
복구된 `service-1`은 동일한 파티션을 할당받았다. 다음 오류는 발생하지 않았다.

- `lost`
- `ILLEGAL_GENERATION`
- `Offset store skipped`

로그 기준으로 `1 -> 2 -> 4 -> 2 -> 1 -> 2`의 모든 파티션 재분배가 정상적으로 완료됐다.
복구 후 최종 Prometheus 상태는 Consumer 2개, 12/12, 합계 24, warm-up 0, lag 0을 별도로
확인해 기록한다.

---

## 7. 재검증 명령과 판정 기준

### 7.1 Consumer 수

```promql
count(up{job="ai-analysis"} == 1)
```

2개 증설 후 기대값은 `2`다.

### 7.2 파티션 분배

```promql
nilm_analysis_consumer_assigned_partitions{job="ai-analysis"}
```

```promql
sum(nilm_analysis_consumer_assigned_partitions{job="ai-analysis"})
```

24개 파티션 기준 일반적으로 12/12이며 합계는 반드시 `24`여야 한다.

### 7.3 리밸런싱 이벤트

```promql
sum by(instance, event) (
  increase(nilm_analysis_consumer_rebalances_total{job="ai-analysis"}[5m])
)
```

정상 증설에서는 `revoke`와 `assign`이 증가하고 `lost`는 증가하지 않아야 한다.
`increase()` 결과가 `1.003`처럼 소수로 보일 수 있는데, 이는 Prometheus가 scrape 간격을
기준으로 값을 보정한 결과이며 실질적으로 1회다.

### 7.4 초기화된 가구와 warm-up

```promql
sum by(instance) (
  increase(nilm_analysis_household_state_resets_total{job="ai-analysis"}[5m])
)
```

```promql
sum(nilm_analysis_warmup_households{job="ai-analysis"})
```

20가구 테스트에서 증설 직후 이동한 가구 10개만 warm-up에 진입했다. 1Hz 기준 약 299초 후
warm-up 합계가 다시 `0`이 되어야 한다.

### 7.5 처리량과 lag

```promql
sum(
  rate(nilm_analysis_messages_total{
    job="ai-analysis",
    status="processed"
  }[1m])
)
```

```promql
sum(nilm_analysis_consumer_lag_messages{job="ai-analysis"})
```

20가구 1Hz 입력에서는 전체 처리량이 약 `20 msg/s`로 안정되고 lag는 거의 `0`을 유지해야
한다. 실행 직후 `[1m]` 구간이 채워지는 동안 rate가 0에서 20으로 점차 증가하는 것은 정상이다.

### 7.6 로그 확인

```cmd
docker compose logs --since=5m realtime-analysis-service | findstr /I /C:"partitions assigned" /C:"partitions revoked" /C:"partitions lost" /C:"ILLEGAL_GENERATION" /C:"Offset store skipped"
```

정상 기준:

| 항목 | 기대 결과 |
| --- | --- |
| `partitions revoked` | 이동한 파티션만 출력 |
| `partitions assigned` | 신규 Consumer가 받은 파티션 출력 |
| `partitions lost` | 없음 |
| `ILLEGAL_GENERATION` | 없음 |
| `Offset store skipped` | 정상 증설에서는 없음 |

---

## 8. 남은 테스트

다음 항목은 이 문서 작성 시점에 남아 있다.

1. 최종 2개 복구 상태에서 Consumer 수 2, 파티션 12/12, 합계 24를 Prometheus로 캡처
2. 최종 복구 후 이동 가구 warm-up과 lag가 다시 `0`이 되는지 확인
3. 필요하면 `docker kill` 또는 프로세스 비정상 종료를 이용한 실제 장애 감지 테스트를 별도로 수행
4. 실제 장애 상황의 `lost`와 통제 종료의 `revoke`를 구분해 기록
5. 동일한 유한 부하로 1개·2개·4개 처리량과 lag를 비교
6. Grafana에서 Consumer별 처리량, lag, 파티션, warm-up, reset 지표를 최종 캡처

고부하 시험은 기능 검증과 분리한다. 현재 확인된 단일 Consumer 처리량보다 큰 입력을 무제한으로
발행하면 lag가 계속 누적되므로 `--count`를 지정한 유한 부하를 사용하고, 각 시험 전 lag를
`0`으로 맞춘다.

---

## 9. 결론

초기 문제는 단순히 `ILLEGAL_GENERATION` 예외를 잡는 것으로는 충분하지 않았다. 프로세스는
유지됐지만 librdkafka 내부에서 assignment 전체가 lost로 바뀌어 cooperative-sticky의 핵심인
상태 보존이 깨졌다.

메시지 처리와 broker offset commit을 분리하여 성공한 메시지만 `store_offsets()`로 표시하고,
librdkafka의 안정 상태 자동 커밋을 사용하도록 변경한 뒤 다음 결과를 확인했다.

```text
Consumer 1 -> 2 증설: 24 -> 12 / 12, 이동 가구 10개 초기화
Consumer 2 -> 4 증설: 12 / 12 -> 6 / 6 / 6 / 6, 이동 가구 8개 초기화
Consumer 4 -> 2 축소: 6 / 6 / 6 / 6 -> 12 / 12
Consumer 2 -> 1 종료: 남은 Consumer가 전체 24개 인수
Consumer 1 -> 2 복구: 24 -> 12 / 12, 이동 가구 10개 초기화

전체 파티션 합계: 모든 단계에서 24
전체 가구 일괄 초기화: 없음
lost: 0
ILLEGAL_GENERATION: 0
Offset store skipped: 0
```

따라서 계획된 증설, 축소, 통제 종료 및 복구 과정에서 cooperative-sticky의 선택적 파티션
이동과 가구 상태 초기화가 의도대로 동작했다. 실제 프로세스 강제 종료와 스케일별 성능 비교는
별도 테스트로 남긴다.

현재 변경사항은 로컬 작업 트리에 있으며 아직 커밋하지 않았다.

추천 커밋 메시지:

```text
fix(ai): defer Kafka offset commits during cooperative rebalancing
```
