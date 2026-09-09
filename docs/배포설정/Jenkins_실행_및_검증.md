# Jenkins 설정 및 검증

## 구현된 구성

- 독립된 local/ec2-a/ec2-b Compose.
- Backend 3개, Frontend/Nginx, Python Bridge Dockerfile.
- Backend의 테스트와 bootJar, Frontend lint/build, Bridge 전달·TLS 단위 테스트.
- Keycloak nilm realm/client 최초 import, Kafka 토픽 초기화.
- Bridge TLS, 초기 연결 재시도, SIGTERM 처리, Kafka ACK 이후 MQTT ACK.
- 서버 배치 스크립트 및 HTTP 인증·MQTT→Kafka smoke 검증.

실제 운영 Jenkins job이나 EC2 배포는 실행하지 않았다. 아래 인프라·Credentials를 준비해야 한다.

## Jenkins 노드와 도구

Multibranch Pipeline으로 이 저장소의 `Jenkinsfile`을 사용한다.

| 라벨 | 필요한 도구 | 역할 |
| --- | --- | --- |
| ci | Linux, Bash, Docker/BuildKit, Docker Compose | 코드 테스트와 이미지 빌드, Registry 업로드 |
| ec2-a | Linux, Bash, Python 3, Docker Compose, Jenkins Agent Java | A 서버 파일 배치와 실행 |
| ec2-b | Linux, Bash, Python 3, Docker Compose, Jenkins Agent Java | B 서버 파일 배치와 실행 |

Pipeline, Credentials Binding, Git/SCM 연동을 준비한다. 이미지 빌드에 Java/Node/Python 도구는 Dockerfile 내부에서 설치된다. Agent Java와 Backend 빌드용 Java는 역할이 다르다.

Controller의 자체 executor는 0, 배포 Agent는 각 1개로 설정한다. feature/PR 코드는 운영 Agent와 운영 Credentials에 접근할 수 없도록 Jenkins 권한·노드/폴더 설정으로 제한한다. Jenkinsfile의 branch 조건만 보안 경계로 사용하지 않는다.

## Credentials

| ID | 종류 | 내용 |
| --- | --- | --- |
| registry-login | Username with password | 이미지 Registry 로그인 계정과 토큰 |
| ec2-a-runtime-env | Secret file | infrastructure/ec2-a/.env.example 기반 실제 값 |
| ec2-b-runtime-env | Secret file | infrastructure/ec2-b/.env.example 기반 실제 값 |
| frontend-build-env | Secret file, 선택 | 필요한 공개 Vite 빌드 변수 |

공통 DB 사용자/비밀번호는 A와 B에서 일치해야 한다. MQTT_USER/MQTT_PASS는 A의 운영 passwd 파일에 있는 계정과 일치해야 한다. 인증서의 SAN에는 B Bridge가 사용하는 MQTT_HOST가 포함되어야 한다.

Jenkins 파라미터:

- `REGISTRY`: 예를 들어 `registry.example.com/team/nilm`. 스킴 없이 Registry와 namespace를 지정한다.
- `FRONTEND_ENV_CREDENTIAL`: 필요하면 `frontend-build-env`, 현재 프론트에는 비워도 된다.
- `ENABLE_CD`: 기본 false. 최초 EC2·인증서·Credentials 준비 후 master에서 활성화한다.

모든 브랜치는 CI를 수행한다. master이면서 ENABLE_CD=true인 경우에만 Registry 업로드와 운영 배포를 수행한다. IMAGE_TAG는 전체 커밋 SHA이며 Jenkins가 env 파일에 기록한다.

## EC2 최초 준비

배포 Agent 사용자가 `/opt/nilm`에 쓸 수 있어야 한다. Docker 접근 권한 및 Registry 네트워크 연결도 준비한다.

운영 실제 비밀 파일은 서버에 별도 배치한다. Jenkins는 기존 파일 존재를 검사하며 임의의 계정이나 자체 서명 인증서를 운영에 생성하지 않는다.

EC2-A:

```text
/opt/nilm/
├─ mqtt/passwd
├─ mqtt/certs/server.crt
├─ mqtt/certs/server.key
├─ certs/fullchain.pem
└─ certs/privkey.pem
```

Nginx 인증서는 APP_DOMAIN과 AUTH_DOMAIN 모두를 포함해야 한다. Mosquitto 개인키와 passwd는 컨테이너의 mosquitto 사용자가 읽을 수 있도록 권한을 설정한다. 배포 사용자가 파일 존재를 확인할 수 있어야 한다. 개인키를 모든 사용자에게 공개하는 방식으로 권한 문제를 해결하지 않는다.

EC2-B:

```text
/opt/nilm/mqtt/certs/ca.crt
```

공인 CA를 쓰는 경우에도 현재 배포 스크립트는 CA 파일을 요구하므로 해당 신뢰 체인을 배치한다. 서버 인증서 검증이나 호스트명 검증을 비활성화하지 않는다.

## 파이프라인 단계

1. CI: Compose config 검사, 이미지 빌드 및 테스트.
2. master CD: 이미지 push.
3. A/B 설정 준비: Compose·env·SQL·realm·프록시 설정 복사, 인증서 파일 확인.
4. B 기반 서비스: PostgreSQL/Kafka readiness 확인, 토픽 생성.
5. A: Redis/Mosquitto/Keycloak, 이후 Backend/Frontend 실행.
6. HTTP 검증: 프론트 응답, 미인증 요청 401, client_credentials 토큰 발급, Gateway→두 Backend 요청.
7. B Bridge: health 확인, 고유 테스트 레코드의 MQTT→Kafka 전달 확인.

별도 서버의 의존성은 Jenkins 단계로 처리한다. 운영 A Compose에 B의 postgres를 depends_on으로 넣지 않는다.

## 검증 결과 (2026-09-06)

이 변경에서 실제로 수행한 검증:

- local/ec2-a/ec2-b Compose 문법·변수 연결 검사.
- Backend 3개 Docker 이미지 빌드 및 Gradle 테스트 통과.
- Frontend Docker 빌드, ESLint 및 TypeScript/Vite build 통과.
- Bridge Docker 빌드 및 단위 테스트 5개 통과.
- Bash 배포 스크립트 문법과 PowerShell 스크립트 구문 검사.
- 기존 환경과 분리된 임시 네트워크/볼륨에서 전체 서비스 기동과 health 통과.
- Keycloak 최초 realm import와 토큰 발급, 미인증 401, Gateway→Device/Monitoring 요청 통과.
- MQTT→Bridge→Kafka 고유 메시지 전달 통과.
- 임시 인증서로 운영 Nginx 설정 검사 통과.
- 운영 Mosquitto TLS 설정과 임시 인증서로 Bridge TLS 연결·MQTT→Kafka 전달 통과.

운영 Jenkins 컨트롤러에서 Declarative Pipeline 실행, 실제 EC2 사설망·DNS·인증서 검증은 아직 수행하지 않았다. 실제 사용자의 Kafka 데이터 이전도 수행하지 않았다.

## 배포 실패 및 롤백

- 실패한 health 또는 smoke 단계는 Jenkins 빌드를 실패시킨다.
- 자동 DB 초기화, 토픽 삭제, 데이터 볼륨 삭제는 수행하지 않는다.
- 이미지 롤백은 각 서버 .env의 IMAGE_TAG를 이전 성공 커밋 SHA로 바꾼 뒤 pull/up한다.
- DB 마이그레이션과 이전 이미지의 호환성은 별도 확인한다.
- 전체 A/B 원자적 롤백은 아직 구현하지 않았다. A는 성공하고 B가 실패할 수 있으므로 각 서버와 Jenkins 단계 상태를 확인한다.
- Kafka/S3 분석 파이프라인을 추가할 때 smoke_test 레코드를 처리 대상에서 제외할지 결정한다.
