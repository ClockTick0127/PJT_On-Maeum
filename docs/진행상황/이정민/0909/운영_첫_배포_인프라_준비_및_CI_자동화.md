# 운영 첫 배포 — 인프라 준비 및 CI 자동화

> 대상 저장소: `C:\dev\특화 프로젝트 - 분산`
> 작성일: 2026-09-09
> 관련 문서: [DockerHub 단일 Private 실행 가이드](../../../배포설정/DockerHub_단일_Private_실행_가이드.md), [MQTT 인증서 및 계정 준비](../../../배포설정/MQTT_인증서_및_계정_준비.md), [Keycloak 운영환경 설정](../../../배포설정/Keycloak_운영환경_설정.md)

---

## 0. 오늘의 목표

Docker Hub 단일 Private 저장소 다이제스트 배포 파이프라인(`b0c2d20`)을 실제 EC2-A/B에 처음 올리기 위해, 코드 외부에서 사람이 준비해야 하는 인프라(도메인·인증서·MQTT 계정·네트워크·Credentials)를 갖추고 CI 자동 트리거와 빌드 알림을 붙였다.

## 1. 완료한 것

### 1-1. 도메인 · HTTPS

| 항목 | 결정 | 비고 |
| --- | --- | --- |
| `APP_DOMAIN` | 기관 제공 호스트 | 팀 EC2-A 퍼블릭 IP를 가리킴 |
| `AUTH_DOMAIN` | DuckDNS 서브도메인 | 기관 호스트에 서브도메인을 추가할 수 없어 무료 DDNS로 확보 |
| 인증서 | Let's Encrypt, 두 도메인 한 장 | `certbot certonly --standalone -d <APP> -d <AUTH>` |
| 배치 | `/opt/nilm/certs/fullchain.pem`(644), `privkey.pem`(600, root) | nginx 컨테이너가 root로 읽음. `prepare.sh`는 존재만 검사 |

nginx가 호스트명으로 프론트/Keycloak을 분기하므로 두 도메인은 서로 달라야 하고, 하나의 인증서 파일이 둘을 모두 포함해야 한다. 현재 코드 구조상 HTTPS는 필수(PKCE, `KC_HOSTNAME`, `prepare.sh` 검사).

### 1-2. MQTT 자체 CA 인증서 · 계정

- EC2-A `~/mqtt-ca/`에 CA 생성 → SAN에 A 사설 IP를 넣은 서버 인증서 발급(10년)
- A: `/opt/nilm/mqtt/certs/server.crt`(644), `server.key`(1883:1883, 640), `/opt/nilm/mqtt/passwd`(`mosquitto_passwd`를 `eclipse-mosquitto:2` 이미지로 1회 실행)
- B: `/opt/nilm/mqtt/certs/ca.crt`, SHA-256 Fingerprint로 원본과 대조
- B `.env`의 `MQTT_HOST`는 SAN과 동일한 A 사설 IP, `MQTT_USER`/`MQTT_PASS`는 passwd와 동일
- 절차 전체를 [MQTT 인증서 및 계정 준비](../../../배포설정/MQTT_인증서_및_계정_준비.md)로 문서화. 실제 IP는 `<A_PRIVATE_IP>` 자리표시자로만 표기

### 1-3. 네트워크

A·B가 같은 VPC임을 확인하고 사설 IP로 통신하도록 구성.

| 서버 | ufw 추가 | 보안 그룹 |
| --- | --- | --- |
| EC2-A | 443, 8883(B 사설 IP만) | 80·443 전체, 8883 B SG |
| EC2-B | 5432·9092(A 사설 IP만) | 5432·9092 A SG |

SSAFY 기본 ufw(22·80·8080·9090 전체 개방)에 위 규칙을 더했다. ufw와 보안 그룹은 둘 다 통과해야 한다.

### 1-4. Jenkins Credentials

| ID | Kind | 내용 |
| --- | --- | --- |
| `ec2-a-runtime-env` | Secret file | A `.env` (도메인 6개 변수 확정) |
| `ec2-b-runtime-env` | Secret file | B `.env` |
| `registry-login` | Username/password | Docker Hub PAT |

Scope는 **Global** (System은 Job에서 보이지 않음). 파일 교체는 **Update → Replace 체크** 후 업로드.

### 1-5. 첫 배포 시도와 수정

- Build → Push → Pull A/B → Prepare A/B 통과
- `Deploy B base`에서 `failed to bind host port 10.0.2.20:5432: cannot assign requested address`
- 원인: B `.env`의 `EC2_B_PRIVATE_IP`가 `.env.example` 자리표시자 그대로. A `.env`의 `POSTGRES_HOST`도 동일 값이어야 함
- 실제 사설 IP로 수정 후 Credential 재업로드

`EC2_B_PRIVATE_IP`는 B의 PostgreSQL/Kafka 포트를 사설 IP에만 바인딩하고, Kafka `ADVERTISED_LISTENERS`로 광고하는 자기 주소로 쓰인다.

### 1-6. CI 자동 트리거

- GitLab Webhook: `<Jenkins>/gitlab-webhook/post`, Push events → HTTP 200
- 처음 `/job/nilm/`으로 등록해 CSRF crumb 403 → 플러그인(GitLab Branch Source) 고정 엔드포인트로 수정
- 모든 브랜치 푸시 시 CI 자동 실행. 배포는 `ENABLE_CD` 기본값 false라 여전히 master에서 **Build with Parameters** 수동

### 1-7. 빌드 알림

Jenkinsfile `post { success / failure }`에 Discord Webhook 알림 추가. 항목은 작업자·커밋·소요 시간·안내문, 제목은 `배포 성공/실패`(master + ENABLE_CD) 또는 `빌드 성공/실패`. Jenkins URL은 포함하지 않는다. Credential `discord-webhook`(Secret text) 필요.

### 1-8. 문서

- [Keycloak 운영환경 설정](../../../배포설정/Keycloak_운영환경_설정.md) — realm import가 자동 처리하는 범위, 콘솔에서 반드시 해야 하는 사용자 생성, 운영 권장 설정
- [MQTT 인증서 및 계정 준비](../../../배포설정/MQTT_인증서_및_계정_준비.md) — 1-2 절차

## 2. 확인한 사실 · 결정 사항

- **Keycloak**: realm/role/client는 import되지만 사용자는 0명이고 셀프 가입도 꺼져 있어 배포 후 콘솔에서 계정을 만들어야 로그인 가능. import는 `nilm` realm이 없을 때만 실행되므로 `FRONTEND_ORIGIN`·smoke secret 변경은 콘솔에서 수동 반영.
- **nginx**: 별도 컨테이너가 아니라 `frontend` 이미지 자체가 nginx. 운영에서는 정적 파일 + `/api/` 프록시 + Keycloak 프록시 + TLS 종단을 한 컨테이너가 담당.
- **로컬 compose**: 오늘 변경은 운영 경로만 건드려 반영 사항 없음. CI가 매 빌드마다 로컬 compose `config`를 검사.
- **DB 스키마**: `ddl-auto=none` + Flyway. 엔티티 변경 시 `V{n}__*.sql`을 함께 커밋하면 배포 시 자동 적용. 적용된 파일 수정 금지, 하위 호환 마이그레이션, 새 DB는 운영에서 수동 `CREATE DATABASE`.
- **멀티 브랜치 배포**: 서버 1세트에 develop/release/master를 모두 배포하면 마지막 배포가 덮어쓰고, 동시 배포 시 `disableConcurrentBuilds`가 브랜치 간에는 적용되지 않아 충돌하며, Flyway 마이그레이션이 브랜치 간 어긋나면 기동 실패. 당분간 master만 배포, 확대 시 `lock` 필수.

## 3. 남은 작업

| 우선 | 항목 |
| --- | --- |
| 필수 | Keycloak `nilm` realm 사용자 생성 및 `USER`/`ADMIN` role 부여 |
| 필수 | 수정된 Credential로 `ENABLE_CD=true` 재배포 및 `verify-http.py` 통과 확인 |
| 권장 | Jenkins 8080 보안 그룹을 팀 IP로 제한 + Multibranch 주기 스캔(1분), 또는 HTTPS 도메인 부여 |
| 권장 | SSH 22 팀 IP 제한, 미사용 8080/9090 ufw 규칙 정리 |
| 권장 | Keycloak 임시 관리자 교체, Brute force 방어, 토큰 수명 |
| 나중 | Let's Encrypt 90일 갱신 자동화(`certbot renew` + `frontend` 재시작 훅) |
| 결정 | develop 브랜치 배포 허용 여부 |
| 선택 | `spring.jpa.hibernate.ddl-auto=validate`로 엔티티-스키마 불일치 조기 검출 |
