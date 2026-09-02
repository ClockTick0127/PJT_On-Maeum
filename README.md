# Git Convention

## Git Flow

Git Flow 전략을 기반으로 브랜치를 관리

| Branch      | 역할                       |
| ----------- | -------------------------- |
| `master`    | 배포 가능한 운영 버전      |
| `develop`   | 다음 배포를 위한 개발 통합 |
| `feature/*` | 기능 개발                  |
| `release/*` | 배포 준비 및 최종 수정     |
| `hotfix/*`  | 운영 환경 긴급 수정        |

```text
master
│
├── hotfix/*
│
└── develop
    ├── feature/*
    └── release/*
```

<br />
<br />

## Branch Flow

#### Feature

- develop 에서만 분기
- 하나의 기능이 완성될경우 develop으로 병합

#### Release

- develop에 어느정도 기능이 쌓였을 경우 release로 병합
- release에서 발생한 버그는 release 브랜치 내에서 수정
- release 내에서 버그를 수정한 경우 develop에도 병합
- 버그가 수정되었고 서비스 가능하다면 master와 develop 양쪽으로 PR

#### Hotfix

- master 에서만 분기
- 빠른 버그수정이 필요할때 사용하며 버그 수정이 완료되면 master로 PR

#### main, develop

- Push하지 않고 Pull Request를 통해 반영

<br />
<br />

## Branch Naming

```text
{type}/{scope}/{description}
```

#### Type

```text
feature
release
hotfix
```

#### Scope

| Scope     | 대상                       |
| --------- | -------------------------- |
| `front`   | Frontend                   |
| `back`    | Backend                    |
| `ai`      | AI / ML                    |
| `data`    | 데이터 수집 / 전처리 / ETL |
| `bigdata` | 대용량 데이터 처리         |
| `spark`   | Spark / 분산 처리          |
| `kafka`   | Kafka / 분산 처리          |
| `infra`   | Docker / 서버 / 배포 환경  |
| `common`  | 공통 작업                  |

예시:

```text
feature/front/login
feature/back/auth-api
feature/ai/recommendation

release/1.0.0
hotfix/back/auth-error
```

<br />
<br />

## Commit Convention

```text
type(scope): message
```

| Type       | 설명                              |
| ---------- | --------------------------------- |
| `feat`     | 기능 추가                         |
| `fix`      | 버그 수정                         |
| `refactor` | 코드 구조 개선                    |
| `perf`     | 성능 개선                         |
| `docs`     | 문서 수정                         |
| `test`     | 테스트 추가 / 수정                |
| `style`    | 동작에 영향을 주지 않는 코드 수정 |
| `chore`    | 설정 / 패키지 / 빌드 / 기타 작업  |
| `data`     | 데이터셋 / 전처리 변경            |

| Scope     | 대상                       |
| --------- | -------------------------- |
| `front`   | Frontend                   |
| `back`    | Backend                    |
| `ai`      | AI / ML                    |
| `data`    | 데이터 수집 / 전처리 / ETL |
| `bigdata` | 대용량 데이터 처리         |
| `spark`   | Spark / 분산 처리          |
| `kafka`   | Kafka / 분산 처리          |
| `infra`   | Docker / 서버 / 배포 환경  |
| `common`  | 공통 작업                  |

<br />
<br />

## Pull Request

PR은 하나의 기능 또는 작업 단위로 생성한다.

```text
feature/front/login → develop
feature/recommendation → develop

release/1.0.0 → main
release/1.0.0 → develop

hotfix/back/auth-error → main
hotfix/back/auth-error → develop
```

PR 제목은 Commit Convention과 동일한 형식을 사용한다.

```text
feat(front): 로그인 페이지 구현
feat(ai): 추천 모델 구현
perf(bigdata): 데이터 처리 성능 개선
```
