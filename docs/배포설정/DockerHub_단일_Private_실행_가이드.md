# Docker Hub 단일 Private 저장소 실행 가이드

현재 저장소: `docker.io/leejeongmin24/on-maum`.

사용자가 생성한 `leejeongmin24/on-maum` 저장소에 Backend 3개, Frontend, 실시간 분석 서비스, Bridge를 서비스별 태그로 모두 저장한다. 새 저장소의 공개 범위는 아직 확인하지 않았으므로 배포 전에 Private인지 확인한다. EC2-A/B Compose 프로젝트 이름 `nilm-a`, `nilm-b`는 그대로다. 기존 Public 저장소는 이 파이프라인에서 push하지 않으며 이미 공개된 이미지의 상태는 이 변경으로 바뀌지 않는다.

## 1. 적용 전 준비

- `ci`, `ec2-a`, `ec2-b` Agent에 Python 3.9 이상 필요: `python3 --version`.
- 각 Agent에 기존 Git/Bash/Docker/Compose 등 실행 도구가 있어야 한다. Python은 새 릴리스 검증에 사용하며 CI에서도 필요하다.
- CI와 A/B의 Docker OS/CPU 아키텍처가 호환되어야 한다. 현재 코드는 단일 플랫폼 빌드이며 아키텍처 불일치 시 배포를 중단한다.
- Docker Hub에서 `leejeongmin24/on-maum`의 Visibility가 Private인지 확인한다. 코드는 저장소 공개 범위를 변경하거나 검증하지 않는다.
- Docker PAT는 Read & Write 권한으로 만들고 Jenkins `registry-login`에 Username with password로 등록한다. Username은 `leejeongmin24`, Password는 PAT다.
- 기존 A/B Agent, `/opt/nilm` mount·권한, DB/MQTT/TLS/DNS 설정 및 `ec2-a-runtime-env`, `ec2-b-runtime-env`를 준비한다.
- 운영을 변경하는 Job은 하나만 사용한다. 현재 disableConcurrentBuilds는 해당 Job만 직렬화하며 다른 Job/수동 배포를 잠그지 않는다.

기존 Secret file에 REGISTRY/IMAGE_TAG 키가 있어도 Prepare가 제거·갱신한다. 서비스별 이미지 참조는 Jenkins가 생성하므로 사람이 임의 digest를 입력할 필요는 없다.

## 2. 코드 반영과 첫 CI

변경 파일을 검토·커밋하고 팀의 master 반영 절차를 따른다. Jenkins가 읽는 브랜치에 코드가 반영돼야 한다.

```text
ENABLE_CD = false   (CI만 확인할 때: 수동 Build with Parameters에서 해제)
IMAGE_REPOSITORY = docker.io/leejeongmin24/on-maum
FRONTEND_ENV_CREDENTIAL = 비움 또는 기존 Credential ID
```

REGISTRY 입력란은 IMAGE_REPOSITORY로 바뀐다. 최초 전환 시 이전 파라미터 화면이 남을 수 있으므로 Scan/CI 실행 후 새 화면을 확인한다. IMAGE_REPOSITORY 파라미터가 아직 없으면 코드의 기본 Private 경로를 사용한다. ENABLE_CD 기본값은 true이며, master 푸시로 시작된 자동 빌드는 이 값과 무관하게 항상 배포한다. 운영 준비가 끝나기 전에는 master에 푸시하지 않고, CI만 확인하려면 수동 Build with Parameters에서 ENABLE_CD를 해제해 실행한다.

이전 계정 경로가 IMAGE_REPOSITORY에 남아 있으면 명시적으로 `docker.io/leejeongmin24/on-maum`을 입력한다. 코드 기본값 변경만으로 이미 전달된 파라미터가 덮어써지지는 않는다. `registry-login`이 이전 계정 토큰이라면 새 저장소 접근 권한이 있는 `leejeongmin24` 계정과 PAT로 갱신한다.

이 모드에서는 릴리스 로직 테스트, Compose config, 이미지 5개 테스트·빌드만 수행한다. Docker Hub push, A/B pull, Prepare/배포는 수행하지 않는다. 단, 빌드에 필요한 공식 기반 이미지 다운로드는 발생할 수 있다.

태그 형식:

```text
docker.io/leejeongmin24/on-maum:api-gateway-<40자리SHA>-b<빌드번호>
docker.io/leejeongmin24/on-maum:iot-device-service-<40자리SHA>-b<빌드번호>
docker.io/leejeongmin24/on-maum:monitoring-service-<40자리SHA>-b<빌드번호>
docker.io/leejeongmin24/on-maum:frontend-<40자리SHA>-b<빌드번호>
docker.io/leejeongmin24/on-maum:mqtt-kafka-bridge-<40자리SHA>-b<빌드번호>
```

빌드 번호는 같은 master Job 내에서 유일해야 한다. Job을 재생성해 번호를 초기화할 때 기존 태그와 충돌하지 않게 한다. 배포는 태그 대신 push에서 반환된 digest를 사용하므로 실행 내용을 고정한다.

## 3. 최초 CD

CI 성공과 운영 준비를 확인한 뒤 master에 푸시하거나 master에서 ENABLE_CD=true로 수동 실행한다. 이후에는 master 푸시마다 자동으로 배포된다.

1. CI 테스트·빌드 → 6개 태그 push → `release.json` 생성·Jenkins artifact 보관.
2. Pull A images: A용 4개를 digest로 pull하고 플랫폼/digest 검사.
3. Pull B images: 실시간 분석 서비스·Bridge를 digest로 pull하고 플랫폼/digest 검사.
4. Prepare A/B: manifest·이미지·필수 파일 확인, 임시 env로 Compose 검사, 기존 설정 백업, 새 설정 배치.
5. Deploy B base: 공식 PostgreSQL/Kafka 준비, 토픽 초기화.
6. Deploy A and verify HTTP: 기반 서비스·Backend·Frontend 시작, HTTP 검증.
7. Deploy realtime analysis: 이미 받은 분석 서비스를 B에서 실행.
8. Deploy Bridge and verify pipeline: 이미 받은 Bridge 실행, 메시지 검증.
9. Record success A/B: 두 서비스 검증 통과 후 성공 manifest 기록.

팀 이미지 하나라도 push/pull에 실패하면 Prepare 이전에 중단한다. 성공한 일부 push는 Hub에 남을 수 있지만 완전한 release.json이 없으면 배포하지 않는다. 서비스 실행 시 팀 이미지는 `--pull never`로 실행해 사전 검증한 로컬 digest를 사용한다.

Docker Hub가 서비스 코드를 빌드하거나 실행하는 것은 아니다. CI가 빌드 자원을, A/B가 실행 자원을 사용하며 Hub는 이미지를 보관한다.

## 4. 확인 및 복구

- Jenkins build artifact `release.json`: 커밋, 빌드 릴리스 ID, 5개 서비스 tag/digest/reference.
- A/B `/opt/nilm/release.json`: 이번에 준비한 릴리스. 실패한 배포의 값일 수도 있다.
- A/B `/opt/nilm/last-success.json`: 검증 완료 후 기록한 마지막 성공본. 성공 기록 단계 자체가 실패하면 실제 서비스 상태와 대조한다.
- `/opt/nilm/releases/<새 릴리스ID>/previous/`: 변경 직전 env/Compose/manifest/관리 설정. `.env` 백업에는 비밀값이 있으므로 접근 권한을 유지한다.

HTTP `PASS: frontend, authentication, Gateway -> device/monitoring`와 Bridge `PASS: MQTT -> Bridge -> Kafka (unique sample)`를 확인한다. Compose ps와 실행 image digest도 manifest와 대조한다.

일반적인 재실행은 새 빌드 번호로 수행한다. Jenkins의 중간 Stage 재시작은 stash 보존을 별도로 구성하지 않았으므로 지원한다고 가정하지 않는다.

롤백은 직전 성공 manifest와 일치하는 백업 설정을 먼저 확인한 뒤 해당 digest 이미지를 인증된 상태에서 pull하고, 당시 설정과 함께 B 기반→A→B 분석→Bridge 순서로 적용·검증한다. 수동으로 스크립트를 호출할 경우 IMAGE_REPOSITORY/IMAGE_TAG/RELEASE_ID도 선택한 manifest와 맞춰야 한다. 단순히 IMAGE_TAG만 바꿔도 이미지가 바뀌는 구조가 아니다.

Prepare 이후 실패하면 설정은 바뀌었지만 컨테이너는 이전 버전일 수 있다. 자동 전체 롤백/무중단 배포는 구현하지 않았다. DB 변경은 별도 호환성 확인이 필요하며 `down -v`를 복구 방법으로 사용하지 않는다. 공식 이미지의 digest 고정과 전역 배포 잠금은 후속 작업이다.

현재/직전 성공 릴리스의 5개 태그·digest 및 설정을 보관하고, 사용 중인 이미지와 DB/Kafka 볼륨을 지우지 않는다.

## 5. 로컬 검증 범위

릴리스 검증 단위 테스트와 Docker Compose 설정 통합 테스트, 셸 문법 검사를 수행한다. 실제 Docker Hub 인증/push/pull과 운영 Jenkins Declarative 실행·EC2 배포는 해당 환경에서 별도 검증해야 한다. 이 가이드 작성 작업에서 토큰 등록·업로드·배포를 실행하지 않았다.
