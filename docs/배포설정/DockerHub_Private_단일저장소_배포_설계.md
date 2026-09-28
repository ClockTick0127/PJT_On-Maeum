# Docker Hub Private 저장소 하나를 사용하는 배포 설계

작성일: 2026-09-09

상태: 핵심 파이프라인 구현 완료, 실제 Docker Hub/Jenkins/EC2 검증 전. 사용자가 생성한 저장소는 `docker.io/leejeongmin24/on-maum`이며 Private 여부는 별도 확인이 필요하다. 현재 절차는 [실행 가이드](DockerHub_단일_Private_실행_가이드.md)를 따른다. 아래 계획 중 전역 배포 잠금, 공식 이미지 digest 고정, 자동 복구는 구현 범위에 포함되지 않았다. EC2 직접 파일 전송 설계를 대체한다.

## 1. 결정과 전제

GitLab master의 코드를 Jenkins CI Agent가 테스트·빌드하고, 완성된 이미지 5개를 Docker Hub에 push한다. EC2-A/B Agent는 필요한 이미지를 pull한 후 각 서버의 Compose로 실행한다. EC2 간 이미지 파일 전송과 자체 Registry 서버는 사용하지 않는다.

사용자가 새로 생성한 `leejeongmin24/on-maum`을 배포 대상으로 선택했다. 해당 저장소를 Private으로 설정하고 서비스별 태그로 5개 이미지를 저장한다. 이전 화면의 저장소와 다른 계정·이름이므로 새 저장소의 공개 범위 및 토큰 접근 권한은 별도 확인한다. 기존 Public 저장소는 배포 대상으로 사용하지 않으며 자동 삭제·공개 범위 변경도 하지 않는다.

이번 변경에서 이미지 이름 생성 규칙과 Compose를 단일 저장소 방식으로 수정했다. 운영에는 수정된 커밋이 반영되어야 하며, 이전 Jenkins REGISTRY 칸에 저장소 전체 경로만 넣어서는 전환되지 않는다.

## 2. 전체 흐름

```text
GitLab master에 변경 반영
  → Jenkins Controller가 CI Agent에 작업 지시
  → 코드·Compose checkout, 테스트, 이미지 5개 순차 빌드
  → Docker Hub의 Private 저장소 하나에 이미지 5개 push
  → A/B 배포 설정 및 이미지 준비
  → B: PostgreSQL/Kafka 준비와 토픽 초기화
  → A: 기반 서비스 + Backend 3개 + Frontend 실행, HTTP 검증
  → B: Bridge 실행, MQTT→Kafka 검증
```

Controller는 실행 순서를 관리한다. CI Agent가 실제 빌드하고 A/B Agent가 각 호스트 Docker에서 배포한다. ci Agent가 A에 있어도 되고 별도 서버에 있어도 된다. 현재 실제 위치는 미확인이다. Controller executor 0, 초기 Agent executor 1을 유지하고 빌드는 순차 수행한다. 컨테이너 배포 Agent는 호스트 Docker 및 동일 경로의 `/opt/nilm`에 접근해야 한다.

A/B의 기존 Compose 프로젝트 이름, 서버 역할, 서비스 이름은 유지한다. 이미지 참조를 바꾸는 것이며 `nilm-a` 같은 Compose 이름을 Docker Hub 저장소 이름으로 바꾸는 작업이 아니다.

## 3. 저장소와 이미지 이름

예시 저장소 경로는 `docker.io/<계정>/<기존-private-저장소>`이다. 아래의 `nilm`은 설명용 저장소 이름이며 기존 저장소 이름을 그대로 사용할 수 있다.

| 서비스 | 예시 이미지 참조 | 배포 위치 |
|---|---|---|
| api-gateway | docker.io/계정/nilm:api-gateway-릴리스ID | A |
| iot-device-service | docker.io/계정/nilm:iot-device-service-릴리스ID | A |
| monitoring-service | docker.io/계정/nilm:monitoring-service-릴리스ID | A |
| frontend | docker.io/계정/nilm:frontend-릴리스ID | A |
| mqtt-kafka-bridge | docker.io/계정/nilm:mqtt-kafka-bridge-릴리스ID | B |

저장소는 하나지만 이미지와 실행 서비스는 5개다. PostgreSQL/Kafka/Redis/Keycloak/Mosquitto는 기존 공식 이미지를 사용한다.

릴리스 ID는 `<전체40자리GitSHA>-b<Jenkins빌드번호>`로 한다. SHA만 사용하면 같은 커밋의 재빌드가 이전 이미지를 덮어쓸 수 있으므로 빌드 번호를 추가한다. 배포 Job은 하나로 제한하고 번호 재사용을 피한다. 서비스별 push digest를 작은 릴리스 manifest로 보관하고, 운영 Compose에는 `docker.io/<계정>/<저장소>@sha256:<서비스별digest>`를 주입해 실제 실행 내용을 고정한다. 태그는 사람이 버전을 찾는 용도, digest는 배포·복구 대상을 확정하는 용도다. `latest`로 운영 배포하지 않는다.

같은 저장소의 태그에는 저장소 단위 공개 범위가 적용되므로 5개 모두 Private이다. 계정/요금제의 현재 저장·전송 제한과 접근 정책은 실제 계정에서 확인한다. 이 설계는 특정 무료 한도를 보장하지 않는다.

## 4. 인증과 파라미터

최초에는 기존 Credential ID `registry-login`을 유지한다. Username with password 유형에서 username은 Docker Hub 사용자명, password는 해당 Private 저장소에 push/pull할 수 있는 Read & Write PAT이다. Delete 권한은 필요하지 않다. 이 단순 구성은 CI와 배포 노드가 같은 토큰을 사용하므로, 운영 안정화 후 push용과 read-only pull용 Credential 분리를 권장한다.

기존 `with-registry.sh`의 임시 DOCKER_CONFIG, password-stdin, 종료 시 인증 파일 정리 방식을 유지한다. 로그인 대상은 `docker.io`로 명시하고 저장소 경로로 로그인하지 않는다. Docker Hub pull이 발생하는 모든 배포 명령은 인증 컨텍스트 안에서 수행한다. Bridge 단계도 누락 이미지 재다운로드 가능성을 고려해 인증으로 감싸거나 사전 검증 후 pull 금지 실행을 명시해야 한다.

| 목표 파라미터 | 값/의미 |
|---|---|
| ENABLE_CD | 기본 true. master 자동 빌드는 항상 배포, 수동 빌드에서만 해제 가능 |
| IMAGE_REPOSITORY | docker.io/<계정>/<Private 저장소>, 스킴·끝 슬래시·태그 없음 |
| FRONTEND_ENV_CREDENTIAL | 기존 선택적 공개 Vite 빌드 설정 |

REGISTRY라는 namespace 파라미터는 IMAGE_REPOSITORY로 대체해 의미를 구분한다. 서버 Secret file의 과거 REGISTRY/IMAGE_TAG 키가 남아 있어도 선택된 릴리스 참조를 덮어쓰지 않게 배포 관리 키를 정리한다.

기존 `ec2-a-runtime-env`, `ec2-b-runtime-env`, TLS 인증서, MQTT 계정, DB 연결 설정은 계속 필요하다. Private 저장소 이름과 사용자명은 확인받되 토큰은 Jenkins에 직접 입력한다.

## 5. CI/CD 단계와 실패 처리

1. CI는 모든 서비스 테스트와 빌드를 먼저 끝낸다. CD=false는 로그인·push·배포 없이 종료한다. 이때 로컬 임시 이미지 이름을 사용해도 된다.
2. master이고 CD=true일 때만 선택한 Private 저장소의 경로를 검증하고 5개 이미지를 서비스별 릴리스 태그로 push한다.
3. 5개 push가 모두 끝나고 digest manifest가 완성되기 전에는 운영 설정이나 컨테이너를 변경하지 않는다. 일부 push 실패는 배포 전 실패이며, 이미 올라간 일부 태그는 미완료 릴리스로 기록한다.
4. A/B에서 필요한 이미지의 digest pull을 모두 완료하고 존재·OS·아키텍처를 검증한다. 이 단계는 활성 env/Compose 변경과 분리한다. A/B의 CPU 아키텍처가 다르면 대상별 빌드 또는 multi-platform 이미지가 필요하다.
5. 이전 성공 설정을 백업한 후 Prepare A/B가 새 Compose와 비밀 env, manifest의 서비스별 digest 참조를 배치한다. 설정 파일은 Controller의 stash/unstash를 쓸 수 있지만 이미지는 Hub를 통해 받는다.
6. 기존 B 기반→A 및 HTTP→Bridge 및 메시지 검증 순서를 유지한다. 기존 300/360/180초 readiness 제한은 초기값이며 실제 기동 시간에 따라 조정한다.
7. 성공 시 SHA, 빌드 번호, 서비스별 digest, 배포 시각, 검증 결과를 기록한다. 기존 `disableConcurrentBuilds`에 더해 다른 Job/수동 운영 배포와 충돌하지 않도록 운영 배포 잠금을 구성한다.

공식 이미지는 태그가 가변적일 수 있다. 복구 manifest에는 실제 사용한 공식 이미지 digest도 기록하고 복구용 설정에 반영한다. 첫 구현에서 팀 이미지 digest만 고정한다면 공식 이미지까지 완전히 동일하게 복구된다고 주장하지 않는다.

배포 시작 후에는 일부 서비스만 새 버전일 수 있다. 자동 원자적 롤백이나 무중단 배포는 현재 구현에 없다. 복구 시 직전 성공본의 이미지 digest와 설정을 같이 적용하고 B 기반→A→Bridge 순서로 검증한다. DB 변경은 이미지 롤백으로 되돌아가지 않으며 별도 호환성 확인과 백업이 필요하다.

## 6. 코드 변경 계획

| 파일 | 변경 |
|---|---|
| Jenkinsfile | IMAGE_REPOSITORY 파라미터·검증, 릴리스 ID 생성, 서비스 태그 push, manifest와 이미지 사전 준비 단계 추가 |
| infrastructure/scripts/build.sh | 현재 순차 빌드 유지, CI 임시 태그와 CD 대상 태그 규칙 정리 |
| infrastructure/scripts/push.sh | 서비스별 저장소 대신 단일 저장소의 서비스별 태그 push, digest 수집·완전성 검증 |
| infrastructure/scripts/with-registry.sh | docker.io 로그인, 기존 임시 인증 방식 유지, 전체 pull 단계 적용 |
| 신규 릴리스 manifest/이미지 준비 스크립트 | 서비스 5개·SHA·릴리스 ID·digest 검증, A/B에 필요한 이미지 사전 pull |
| infrastructure/scripts/prepare.sh, release-env.py | REGISTRY 필수 조건과 SHA 단일 태그 주입을 서비스별 digest 참조 주입으로 변경 |
| infrastructure/ec2-a/compose.yaml | 4개 팀 서비스의 image를 API_GATEWAY_IMAGE, IOT_DEVICE_IMAGE, MONITORING_IMAGE, FRONTEND_IMAGE로 변경 |
| infrastructure/ec2-b/compose.yaml | Bridge image를 MQTT_KAFKA_BRIDGE_IMAGE로 변경 |
| infrastructure/scripts/deploy.sh | 사전 확보한 팀 digest 이미지 실행, 인증 범위와 공식 이미지 pull 정리, 기존 health/smoke 유지 |
| A/B .env.example 및 문서 | 서비스별 image 변수, 단일 저장소 입력값, Credentials 절차 설명 |

목표 Compose 예시:

```yaml
services:
  api-gateway:
    image: ${API_GATEWAY_IMAGE:?API_GATEWAY_IMAGE required}
```

Prepare가 기록하는 값의 형식:

```text
API_GATEWAY_IMAGE=docker.io/<계정>/<저장소>@sha256:<api-gateway digest>
```

실제 구현에서는 `.env.example`만으로 수행하는 현재 CI Compose config 검사가 계속 통과하도록 형식상 유효한 예시 이미지 값을 제공한다. local Compose/개발 스크립트도 참조를 전체 검색해 호환성을 확인한다. 실제 SHA/digest/사용자명은 예시 값으로 배포하지 않는다.

## 7. 자원과 운영

빌드 CPU·메모리는 CI Agent의 Docker 실행 환경이 사용한다. Docker Hub는 이 설계에서 빌드를 대신하지 않는다. A/B의 실행 컨테이너는 지속적으로 메모리를 사용하고, pull은 일시적인 네트워크·디스크·CPU/RAM을 사용한다.

Hub에 이미지를 보관해도 A/B가 실행할 이미지를 로컬 디스크에 받아야 하므로 EC2 이미지 저장 공간은 필요하다. 직접 전송용 tar 파일과 별도 SSH 전송 계정은 필요하지 않다. Docker Hub 인증·이미지 다운로드에 필요한 HTTPS 아웃바운드를 CI/A/B에서 허용해야 한다. DB/MQTT 등 기존 A/B 통신은 유지한다.

현재 성공 릴리스와 직전 성공 릴리스의 모든 서비스 digest·태그·설정·manifest를 보존한다. 다섯 태그를 한 릴리스 단위로 관리해 일부 서비스만 삭제하지 않는다. EC2에서는 실행 중 이미지와 복구용 이미지를 제외한 오래된 이미지·캐시만 정리한다. DB/Kafka 볼륨 삭제는 이미지 정리 절차에 포함하지 않는다.

## 8. 적용 순서와 검증

1. Docker Hub 사용자명과 기존 Private 저장소 이름·공개 범위를 확인한다.
2. 단일 저장소 태그 규칙과 manifest를 코드에 구현한다. 현재 코드는 아직 서비스별 저장소 방식임을 유의한다.
3. CI 모드로 기존 테스트·이미지 5개 빌드와 Compose 검사를 검증한다.
4. registry-login, A/B Secret file·Agent·인증서·네트워크를 준비한다.
5. 코드가 적용된 master에서 IMAGE_REPOSITORY를 지정하고 ENABLE_CD=true로 최초 수동 배포한다.
6. Private 저장소에 5개 태그가 있고 비인증 접근이 차단되는지, A/B의 실제 digest가 manifest와 일치하는지 확인한다.
7. HTTP 인증 검증과 MQTT→Bridge→Kafka 검증이 모두 통과해야 배포 성공으로 기록한다.
8. 인증 실패·5개 중 일부 push 실패·이미지 누락 시 배포 전 중단, 이전 성공본 복구도 검증한다.
9. 최초 수동 성공 후 자동 CD 기본값을 변경하고 다음 master 이벤트로 다시 검증한다. 한 번 수동 입력한 true가 다음 웹훅에 유지된다고 가정하지 않는다.

## 9. 참고

- Docker Hub repositories: https://docs.docker.com/docker-hub/repos/
- Docker PAT: https://docs.docker.com/security/access-tokens/personal-access-tokens/
- 로컬 검토: Jenkinsfile, build.sh, push.sh, with-registry.sh, release-env.py, A/B Compose.
