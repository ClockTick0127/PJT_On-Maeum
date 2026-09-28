# Keycloak 운영환경 설정

EC2-A에 Keycloak이 배포된 뒤 사람이 직접 해야 하는 설정을 정리한다. realm import가 자동으로 처리하는 범위와, 콘솔에서 반드시 해야 하는 것, 운영이면 권장하는 것을 구분한다.

기준 코드: `infrastructure/ec2-a/compose.yaml`의 `keycloak` 서비스(Keycloak 26.4, `start --import-realm`), `infrastructure/keycloak/nilm-realm.json`.

## 1. 자동으로 처리되는 것

최초 기동 시 `nilm-realm.json`이 import된다. 아래는 콘솔에서 다시 만들 필요가 없다.

| 항목 | 값 | 비고 |
| --- | --- | --- |
| realm | `nilm` | `registrationAllowed: false` (셀프 회원가입 차단) |
| realm role | `USER`, `ADMIN` | 백엔드가 `realm_access.roles`를 `ROLE_*`로 변환해 검사 |
| client `nilm-dashboard` | public, Authorization Code + PKCE(S256) | redirect `${FRONTEND_ORIGIN}/*`, web origin `${FRONTEND_ORIGIN}` |
| client `nilm-smoke` | confidential, service account | secret은 `NILM_SMOKE_CLIENT_SECRET`, CI 검증(`verify-http.py`) 전용 |
| master 관리자 | `KEYCLOAK_ADMIN` / `KEYCLOAK_ADMIN_PASSWORD` | `keycloak_db`가 비어 있을 때 한 번만 생성 |

`${FRONTEND_ORIGIN}`, `${NILM_SMOKE_CLIENT_SECRET}`은 import 시점의 컨테이너 환경변수로 치환된다.

## 2. 반드시 해야 하는 것: 로그인 사용자 생성

realm 파일에 사용자가 없고 셀프 회원가입도 막혀 있으므로, **배포 직후에는 로그인 가능한 계정이 0개**다. 대시보드에 접속할 사람마다 계정을 만들고 role을 부여해야 한다.

### 2-1. Admin Console 접속

```text
https://<AUTH_DOMAIN>/admin/
```

nginx가 `AUTH_DOMAIN`의 모든 경로를 Keycloak으로 넘기므로 별도 포트 개방은 없다. `master` realm에 `KEYCLOAK_ADMIN` 계정으로 로그인한 뒤, 좌상단 realm 선택에서 `nilm`으로 전환한다.

### 2-2. 사용자 추가 (콘솔)

1. **Users → Add user**
   - Username 입력
   - Email verified: **On** (SMTP를 붙이지 않았으므로 Off면 인증 메일을 보낼 수 없다)
   - Create
2. **Credentials → Set password**
   - Temporary: **Off** (On이면 첫 로그인에 비밀번호 변경 화면이 뜬다)
3. **Role mapping → Assign role → Filter by realm roles**
   - 일반 사용자: `USER`
   - `/api/devices/admin/**`, `/api/monitoring/admin/**` 접근 필요 시: `ADMIN`

### 2-3. 사용자 추가 (kcadm CLI)

EC2-A에서 실행한다. 비밀번호는 명령줄에 쓰지 말고 프롬프트에 입력한다.

```bash
docker compose --project-directory /opt/nilm exec -it keycloak /opt/keycloak/bin/kcadm.sh config credentials --server http://localhost:8080 --realm master --user "$KEYCLOAK_ADMIN"
```

```bash
docker compose --project-directory /opt/nilm exec keycloak /opt/keycloak/bin/kcadm.sh create users -r nilm -s username=<사용자명> -s enabled=true -s emailVerified=true
```

```bash
docker compose --project-directory /opt/nilm exec -it keycloak /opt/keycloak/bin/kcadm.sh set-password -r nilm --username <사용자명>
```

```bash
docker compose --project-directory /opt/nilm exec keycloak /opt/keycloak/bin/kcadm.sh add-roles -r nilm --uusername <사용자명> --rolename USER
```

`kcadm.sh config credentials`가 저장하는 토큰은 컨테이너 안 `~/.keycloak/kcadm.config`에 남는다. 작업이 끝나면 컨테이너 재생성으로 사라지지만, 즉시 지우려면 `rm /opt/keycloak/.keycloak/kcadm.config`를 실행한다.

## 3. 운영이면 권장하는 것

### 3-1. 임시 관리자 교체

`KC_BOOTSTRAP_ADMIN_*`로 생성된 계정은 Keycloak 26에서 **임시 관리자**로 표시되며, 콘솔 상단에 경고가 뜬다.

1. `master` realm → **Users → Add user**로 영구 관리자 계정 생성, Credentials에 비밀번호 설정(Temporary Off)
2. **Role mapping → Assign role**에서 `admin` realm role 부여
3. 새 계정으로 재로그인 후 임시 관리자 삭제 또는 비활성화

이후 `.env`의 `KEYCLOAK_ADMIN`, `KEYCLOAK_ADMIN_PASSWORD`는 재기동 시 무시된다(DB가 비어 있지 않으므로). 값은 남겨두되, 실제 관리자 자격과 다르다는 점을 팀에 공유한다.

### 3-2. 무차별 대입 방어

`nilm` realm → **Realm settings → Security defenses → Brute force detection**

- Brute Force Mode: `Lockout temporarily` 이상
- Max login failures, Wait increment 등을 팀 정책에 맞춰 설정

### 3-3. 토큰·세션 수명

`nilm` realm → **Realm settings → Sessions / Tokens**

| 항목 | 기본값 | 권장 검토 |
| --- | --- | --- |
| Access Token Lifespan | 5분 | 백엔드는 JWKS로만 검증하므로 짧게 유지 |
| SSO Session Idle | 30분 | 대시보드 사용 패턴에 맞춰 조정 |
| SSO Session Max | 10시간 | 하루 근무 시간 기준 검토 |

### 3-4. 비밀번호 정책

`nilm` realm → **Authentication → Policies → Password policy**에서 길이, 문자 조합, 재사용 금지 등을 추가한다. 현재 realm 파일에는 정책이 없다.

### 3-5. 이메일 (선택)

비밀번호 재설정, 이메일 인증을 쓸 경우에만 **Realm settings → Email**에 SMTP를 등록하고, **Realm settings → Login**에서 Forgot password를 켠다. 등록 전에는 사용자 Email verified를 수동으로 On 처리해야 한다.

## 4. 배포 후 검증

EC2-A에서 실행한다. 프론트 응답, 미인증 401, `nilm-smoke` 토큰 발급, Gateway → device/monitoring 경로를 확인한다.

```bash
python3 infrastructure/scripts/verify-http.py
```

사람 계정 로그인 확인은 브라우저에서 `https://<AUTH_DOMAIN>/realms/nilm/account/`에 접속해 2장에서 만든 계정으로 로그인해 본다. 프론트엔드 로그인 연동은 아직 코드에 없으므로(`nilm-dashboard`를 참조하는 프론트 코드 없음) 이 시점에는 Account Console로만 확인한다.

## 5. 주의사항

- **realm import는 `nilm` realm이 없을 때만 실행된다.** 이미 realm이 있으면 `nilm-realm.json`을 고쳐 재기동해도 반영되지 않는다. 사용자·realm을 자동으로 삭제하거나 덮어쓰지 않는다.
- `FRONTEND_ORIGIN`을 바꾼 경우: 콘솔에서 **Clients → nilm-dashboard → Settings**의 Valid redirect URIs, Web origins를 직접 수정한다.
- `NILM_SMOKE_CLIENT_SECRET`을 교체한 경우: **Clients → nilm-smoke → Credentials**에서 Client secret을 `.env`와 같은 값으로 갱신한다. env만 바꾸면 `verify-http.py`가 401로 실패한다.
- `KEYCLOAK_ADMIN_PASSWORD`를 바꾼 경우: 콘솔(`master` → Users → admin → Credentials) 또는 아래 명령으로 바꾼다.

```bash
docker compose --project-directory /opt/nilm exec -it keycloak /opt/keycloak/bin/kcadm.sh set-password -r master --username admin
```

- Keycloak 데이터는 EC2-B의 PostgreSQL `keycloak_db`에 있다. Keycloak 컨테이너를 지워도 사용자·설정은 유지되며, 초기화하려면 `keycloak_db`를 비워야 한다. 그 경우 임시 관리자와 realm import가 다시 실행된다.
- Admin Console은 `AUTH_DOMAIN`으로 외부에 노출된다. 관리자 계정 비밀번호 강도와 3-2 무차별 대입 방어 설정을 우선 적용한다.
