### 간편 실행 (local)

1. 기존 서비스 실행하여 nilm-net 네트워크 활성화
2. observability 폴더 내의 .env.local.example 파일을 복사하여 .env로 생성 (기존 .env는 유지)
3. 아래 명령어를 observability 폴더에서 실행

```
docker compose --env-file .env up -d prometheus blackbox-exporter grafana
```

4. 링크를 통해 웹 대시보드 접속

- Prometheus: http://localhost:19090
- Targets: http://localhost:19090/classic/targets
- Grafana: http://localhost:13001

---

# Observability

EC2-A에서 Prometheus·Grafana·Blackbox Exporter를 실행하는 관측 설정 모음.
EC2-A 서비스는 Docker 네트워크, EC2-B의 AI/Kafka/PostgreSQL은 사설 IP를 통해 수집.
운영에서는 `infrastructure/ec2-a/compose.yaml`에 통합되어 Jenkins가 기본 서비스와
함께 배포한다. 이 디렉터리의 Compose는 로컬 개발과 기존 독립 실행 호환용이다.

## 수집 범위

| 구성               | 수집 내용                                                                         |
| ------------------ | --------------------------------------------------------------------------------- |
| Prometheus         | AI는 1초, 나머지는 15초 주기 수집. 15일 또는 5GB 중 먼저 도달하는 보존 한도        |
| AI `/metrics`      | 단계별 지연, 처리 상태, Consumer별 Lag·처리량, 리밸런싱, 할당 파티션, 상태 초기화, warm-up |
| 집계 `/metrics`    | 일일 활동 지수·패턴 변화·baseline 갱신 작업의 실행 횟수와 지연                   |
| Blackbox Exporter  | 기존 HTTP health/readiness의 성공 여부·시간·상태 코드, TCP 연결 여부·시간         |
| Grafana            | 데이터소스와 대시보드 4개 자동 등록, 화면은 1초마다 갱신                           |
| cAdvisor 선택 구성 | 같은 Linux Docker 호스트의 컨테이너 CPU·메모리·네트워크                           |

기본 ec2-a 대상: EC2-B의 AI health/ready 및 metrics, Kafka 9092/PostgreSQL 5432 TCP. EC2-A의 Spring 3개 서비스 readiness, Keycloak readiness, frontend healthz, Redis 6379/Mosquitto 8883 TCP.
Mosquitto probe는 TCP 연결까지만 확인하며 TLS 인증서나 MQTT 인증은 검증하지 않음.
선택 cAdvisor는 EC2-A 컨테이너만 관측. EC2-B의 mqtt-kafka-bridge 등 컨테이너 자원은 별도 원격 Exporter 구성이 필요하며 이번 구성에 포함하지 않음.
DB 쿼리 성능, Kafka 전체 Consumer Group의 committed lag, JVM 업무 지표, MQTT 메시지 통계는 이번 범위에 포함하지 않음.

## EC2-A에서 실행

운영에서는 EC2-A 런타임 환경 파일에 `GRAFANA_ADMIN_PASSWORD`,
`EC2_B_PRIVATE_IP`, `PROMETHEUS_PORT`, `GRAFANA_PORT`를 설정한다. Jenkins의
`Prepare A`가 이 디렉터리를 `/opt/nilm/observability`로 복사하고 `Deploy A`가
EC2-A 기본 Compose의 세 관측 서비스를 함께 기동한다.

수동 확인 시 EC2-A에서 다음을 사용한다.

```sh
cd /opt/nilm
docker compose ps prometheus grafana blackbox-exporter
docker compose logs --tail=100 prometheus
```

- Grafana: http://localhost:13001 (`admin` / 지정 비밀번호)
- Prometheus: http://localhost:19090
- 수집 상태: http://localhost:19090/classic/targets
- Grafana의 `NILM Observability` 폴더에서 대시보드 확인

원격 접근: `ssh -L 13001:127.0.0.1:13001 -L 19090:127.0.0.1:19090 user@ec2-a` 후 로컬 브라우저에서 위 주소 사용.

포트는 루프백에만 바인딩. Prometheus 19090과 Grafana 13001은 프로젝트의 기존 포트와 충돌을 피하기 위한 호스트 포트.
Exporter는 호스트 포트를 노출하지 않음. Prometheus/Grafana 데이터는 named volume에 보존.
Grafana 초기 관리자 비밀번호 환경변수는 기존 DB에 생성된 계정 비밀번호를 자동 변경하지 않음.

## Linux 컨테이너 자원 수집

Linux의 일반적인 rootful Docker와 `/var/lib/docker` 경로 기준.
cAdvisor는 호스트 파일시스템 및 Docker 메타데이터 읽기와 privileged 권한이 필요하므로 별도 파일로 분리.
기본 스택에는 이러한 호스트 마운트가 없음.

```sh
docker compose -f compose.yaml -f compose.resources.yaml up -d
```

Prometheus의 resources 디렉터리 마운트가 활성 대상 파일로 교체되어 cAdvisor 수집 시작.
기본 실행에서는 빈 대상 목록을 사용하므로 cAdvisor가 불필요하게 DOWN으로 표시되지 않음.
Docker Desktop/WSL은 Linux VM 내부만 관측하며 호스트 경로·장치가 다를 수 있음. 기본 스택부터 실행하고 Linux 서버에서 자원 수집 구성 사용 권장.
rootless Docker나 다른 data-root에서는 이 폴더의 `compose.resources.yaml` 마운트를 실제 경로에 맞춰 조정.
자원 대시보드에는 Compose project/service 라벨이 있는 컨테이너만 표시.

선택 구성을 사용했다면 종료할 때도 같은 파일 목록 사용.

```sh
docker compose -f compose.yaml -f compose.resources.yaml down
```

## EC2-B 수집 포트 연결

EC2-B 기본 Compose가 AI 8000과 node-exporter 9100을 EC2-B 사설 IP에 바인딩한다.
별도 `compose.ec2-b.override.yaml`은 더 이상 운영 배포에 필요하지 않으며 기존 수동
실행과의 호환을 위해서만 남긴다.
EC2-B 보안 그룹에서 EC2-A 보안 그룹 또는 사설 IP에 대해서만 TCP 8000 허용 필요.
node-exporter 수집을 위해 9100도 같은 범위로 허용한다. TCP probe를 위해
5432/9092도 EC2-A에서 접근 가능해야 한다.
8000에서는 metrics뿐 아니라 health/ready도 제공하므로 인터넷에 공개하지 않음.

## 로컬 개발용 대상

로컬에서는 `.env.local.example`, EC2-A에서는 `.env.ec2-a.example`을 `.env`로 복사.
로컬 예시는 `APPLICATION_NETWORK=nilm-net`, `TARGET_ENV=local`, `EC2_B_PRIVATE_IP=127.0.0.1` 사용.
로컬 대상 JSON은 EC2-B 별칭을 사용하지 않으며 `TARGET_ENV`로 대상 묶음 선택.
로컬 AI replica는 `realtime-analysis-service-local` DNS 별칭의 모든 A 레코드를
5초마다 발견해 각각 수집한다. `prometheus/targets/local/ai.json`은 단일
`aggregation-service` 대상만 별도로 등록한다.

## AI 대시보드 해석

- 지연은 초 단위 Histogram의 bucket/sum/count 사용. p50/p95/p99 및 평균은 관측된 샘플 기준.
- 단계 지연은 단계 호출당 관측. 메시지 하나에서 이벤트를 여러 번 발행하면 여러 관측 발생.
- `processed`는 추론 없이 버퍼 적재 후 커밋한 입력도 포함.
- E2E는 `snapshot.published_at - snapshot.observed_at`. published_at은 Snapshot 생성 시각이므로 Kafka ACK 시간은 제외. ACK는 `snapshot_publish_ack` 단계로 확인.
- Consumer Lag은 캐시 high watermark와 현재 position의 차이. committed offset 기준 Lag과 다름. 서로 다른 consumer group을 추가할 경우 그룹별 대상을 분리해야 합계가 혼합되지 않음.
- 모델 정보는 Manifest 이름과 버전이며 현재 FakePredictor를 실제 모델로 바꾸거나 모델 준비 상태를 검증하지 않음.
- 최초 DLQ/오류 발생 전, 버퍼 준비 전, 관측 없는 단계는 `No data`가 정상일 수 있음. 0으로 강제 보정해 수집 장애를 숨기지 않음.
- rate는 최소 두 수집 샘플 필요. AI는 1초 수집 및 쿼리 Min step 1초 사용. 시작 직후 여러 샘플이 쌓인 뒤 확인. Histogram 관측이 없으면 분위수는 비어 있을 수 있음.
- AI instance 선택기로 여러 인스턴스 중 조회 대상 선택.
- 할당 파티션과 warm-up 가구는 현재값, 리밸런싱과 상태 초기화는 Counter 증가량으로 표시.

## 패턴 감지·일일 작업 및 1초 갱신

`NILM · AI 패턴 감지 및 일일 작업` 대시보드에서 패턴별 결과/지연, Kafka 최종 발행, 일일 작업 실행 결과/지연을 확인합니다. 일일 작업은 Prometheus `job`과 충돌한 애플리케이션 라벨 `exported_job`으로 묶습니다. 기본 조회 기간은 48시간이며 최초 실행을 놓치지 않도록 프로세스 누적값도 함께 제공합니다.

Grafana 최소 갱신 제한과 모든 대시보드 기본 갱신을 1초로 변경했습니다. AI scrape도 1초이며 쿼리별 Min step 1초를 적용했습니다. HTTP/TCP 점검과 cAdvisor는 기존 15초 수집을 유지하므로 화면 갱신과 원본 데이터 갱신 간격이 다릅니다. AI Consumer Lag의 내부 계산 주기(기본 5초)는 AI 코드·설정 변경이 필요하여 변경하지 않았습니다.

상세 라벨 정의, PromQL, 첫 관측/재시작 해석 및 설정 적용 명령은 [PATTERN_METRICS_HANDOFF.md](PATTERN_METRICS_HANDOFF.md)에 정리했습니다. 기존 AI 이미지에 새 메트릭이 없으면 최신 이미지 배포가 별도로 필요합니다.

## 대상 추가 및 설정 반영

`prometheus/targets/<환경>/ai.json`, `http.json`, `tcp.json`에 target과 service/environment 라벨 정의.
같은 job 안에서 대상이 중복되지 않게 유지. 대상 JSON 변경은 최대 30초 후 자동 반영.
Prometheus 설정 변경은 `docker compose restart prometheus`, Blackbox 설정 변경은 `docker compose restart blackbox-exporter`.
Grafana dashboard JSON은 약 30초 후 자동 반영, provisioning 설정 변경은 `docker compose restart grafana`.
대시보드는 파일에서 관리하며 UI 저장은 비활성화.

## 확인 방법 및 제한

1. Prometheus Targets에서 모든 `ai-analysis` replica와 `aggregation-service`가 UP인지 확인.
2. `nilm_analysis_model_info`와 `nilm_analysis_messages_total` 조회.
3. 기존 입력 메시지 처리 후 AI 지연 및 처리량 패널 확인.
4. `probe_success`로 각 HTTP/TCP 대상의 상태 확인. `up`은 Exporter 호출 성공이지 대상 서비스 정상 여부가 아님.
5. 401/403이면 인증 설정, 503이면 readiness 의존성, DNS 오류면 네트워크/대상 주소 확인.

현재 구현에는 외부 폴더 수정이 필요하지 않음. 후속 JVM/업무 지표 수집에는 서비스 의존성·endpoint 노출 변경이 필요할 수 있으며 이번 작업에서는 적용하지 않음.
AI E2E를 Kafka ACK 완료까지로 변경하거나 Consumer Lag을 committed 기준으로 바꾸려면 AI 코드 변경이 필요하며 현재 의미 그대로 표시.
EC2-B 디스크·inode 알림 규칙은 구성되어 있다. Alertmanager 등 외부 알림 전송은 미구성이다.
빌드 테스트는 수행하지 않음. 설정 검증은 다음 명령으로 실행 가능. `--promtool`은 공식 Prometheus v3.5.0 이미지로 설정, 44개 대시보드 쿼리 문법, 후보/발행 건수 구분, exported_job 집계, 첫 관측과 미실행 작업 처리를 검증하며 실행 중 서비스나 DB를 변경하지 않음.

```powershell
node verify-metrics.mjs
node verify-metrics.mjs --promtool
```

Docker 이미지가 없으면 다운로드가 필요함. 검증용 임시 파일은 이 폴더 안에 만들고 종료 시 정리함. 환경별 실제 수집 성공과 Grafana 표시 결과는 스택에 설정 반영 후 위 절차로 확인 필요.

## 참고

- [Prometheus 다중 대상 Exporter 구성](https://prometheus.io/docs/guides/multi-target-exporter/)
- [Grafana provisioning](https://grafana.com/docs/grafana/latest/administration/provisioning/)
- [cAdvisor 실행 구성](https://github.com/google/cadvisor/blob/master/README.md)
