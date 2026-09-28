# SonarQube 로컬 검증 및 SSAFY 공용 서버 연동

> 진행일: 2026-09-15  
> 작성자: 최보경  
> 작업 브랜치: `feature/infra/sonarqube`  
> 대상 프로젝트: `S15P21D201`

---

## 1. 오늘 작업 요약

NILM Platform의 프론트엔드, Python 실시간 분석 서비스, Java 백엔드 3개 서비스를
SonarQube로 통합 분석할 수 있도록 설정했다.

처음에는 로컬 Docker 환경에서 SonarQube와 전용 PostgreSQL을 실행해 전체 분석 흐름을
검증했다. 이후 SSAFY가 제공하는 공용 SonarQube 서버를 확인해 프로젝트와 토큰을 새로
생성하고, 동일한 분석 결과를 공용 서버로 전송하는 데 성공했다.

최종 구성은 다음과 같다.

```text
로컬 또는 Jenkins 작업 공간
  ├─ 소스 코드
  ├─ Python pytest-cov 결과
  ├─ Java JaCoCo 결과·컴파일 산출물·라이브러리
  └─ Docker SonarScanner
              │
              ▼
    https://sonarqube.ssafy.com
              │
              ▼
       팀 공용 대시보드
```

SonarQube 분석에는 Jenkins가 필수는 아니다. 오늘은 Docker Scanner를 수동으로 실행했고,
추후 같은 명령을 Jenkins Pipeline에 추가하면 자동화할 수 있다.

## 2. 로컬 SonarQube 검증

### 2.1 구성

`infrastructure/local/compose.sonar.yaml`에 다음 컨테이너를 분리해 구성했다.

| 서비스 | 이미지 | 역할 |
| --- | --- | --- |
| `nilm-sonarqube` | `sonarqube:community` | 분석 결과 처리 및 대시보드 제공 |
| `nilm-sonar-db` | `postgres:16-alpine` | 분석 결과, Issue, 설정 및 이력 저장 |

SonarQube 데이터, Extension, 로그 및 PostgreSQL 데이터는 각각 Named Volume에 저장한다.
웹 포트는 로컬에서만 접근하도록 `127.0.0.1:9000`에 바인딩했다.

```cmd
cd infrastructure\local
docker compose -f compose.sonar.yaml up -d
docker compose -f compose.sonar.yaml ps
```

확인 결과 PostgreSQL은 `healthy`, SonarQube는 `Up` 상태였고
`http://localhost:9000` 로그인과 프로젝트 생성에 성공했다.

### 2.2 PostgreSQL이 필요한 이유

SonarQube 서버는 Scanner가 만든 분석 보고서를 처리한 뒤 다음 정보를 DB에 저장한다.

- 프로젝트 및 사용자 설정
- 발견된 Security, Reliability, Maintainability Issue
- 코드 품질 지표와 Quality Gate 결과
- Coverage 및 Duplication 지표
- 분석 이력과 대시보드 표시용 데이터

따라서 각 팀원이 로컬 SonarQube를 별도로 실행하면 설정 파일은 같더라도 DB가 다르므로
대시보드도 서로 분리된다. 하나의 대시보드를 공유하려면 하나의 공용 SonarQube 서버를
사용해야 한다.

## 3. 분석 대상 설정

저장소 루트에 `sonar-project.properties`를 추가했다.

현재 SSAFY 공용 서버 프로젝트 정보는 다음과 같다.

```properties
sonar.projectKey=S15P21D201
sonar.projectName=S15P21D201
sonar.sourceEncoding=UTF-8
```

분석 대상은 다음과 같다.

| 구분 | 경로 |
| --- | --- |
| Frontend | `frontend/src` |
| Python | `ai/realtime-analysis-service/src` |
| API Gateway | `backend/api-gateway/src/main` |
| IoT Device | `backend/iot-device-service/src/main` |
| Monitoring | `backend/monitoring-service/src/main` |

테스트 코드는 Python과 Java 3개 서비스의 테스트 디렉터리를 별도로 지정했다.
`node_modules`, `dist`, `.venv`, `__pycache__`는 분석에서 제외했다. 현재 `sonar.sources`가
각 서비스의 `src`로 한정되어 있어 저장소의 `.env` 파일은 분석 범위에 포함되지 않는다.

## 4. Python Coverage 연동

`ai/realtime-analysis-service/pyproject.toml`에 `pytest-cov`를 개발 의존성으로 추가하고,
컨테이너 경로가 아닌 저장소 기준 상대 경로가 XML에 기록되도록 설정했다.

```toml
[project.optional-dependencies]
dev = [
    "pytest>=8,<9",
    "pytest-cov>=6,<7",
]

[tool.coverage.run]
relative_files = true
```

Coverage 보고서는 다음 경로로 생성한다.

```text
ai/realtime-analysis-service/coverage.xml
```

저장소 루트에서 실행한 명령은 다음과 같다.

```cmd
docker run --rm ^
  -v "%cd%:/workspace" ^
  -w /workspace ^
  python:3.11-slim ^
  sh -c "pip install -e './ai/realtime-analysis-service[dev]' && pytest ai/realtime-analysis-service/tests --cov=ai/realtime-analysis-service/src/realtime_analysis --cov-config=ai/realtime-analysis-service/pyproject.toml --cov-report=xml:ai/realtime-analysis-service/coverage.xml"
```

결과는 다음과 같다.

```text
46 passed in 8.36s
Coverage XML written to file ai/realtime-analysis-service/coverage.xml
```

SonarQube에는 Python 버전과 Coverage 경로를 명시했다.

```properties
sonar.python.version=3.11
sonar.python.coverage.reportPaths=ai/realtime-analysis-service/coverage.xml
```

이 설정으로 모든 Python 3 버전을 대상으로 분석한다는 경고를 제거하고 Python Coverage를
대시보드에 반영했다.

## 5. Java JaCoCo 및 정밀 분석 설정

다음 세 서비스에 동일한 JaCoCo 설정을 적용했다.

- `backend/api-gateway`
- `backend/iot-device-service`
- `backend/monitoring-service`

각 `build.gradle`에 `jacoco` 플러그인을 추가하고, 테스트 종료 후 XML과 HTML 보고서를
생성하도록 구성했다.

```groovy
plugins {
    id 'java'
    id 'jacoco'
}

tasks.named('test') {
    useJUnitPlatform()
    finalizedBy tasks.named('jacocoTestReport')
}

tasks.named('jacocoTestReport') {
    dependsOn tasks.named('test')

    reports {
        xml.required = true
        html.required = true
    }
}
```

Java Analyzer가 타입과 의존 관계를 정확하게 확인할 수 있도록 Main/Test Runtime
Classpath를 복사하는 Task도 추가했다.

```groovy
tasks.register('copySonarLibraries', Sync) {
    from configurations.runtimeClasspath
    into layout.buildDirectory.dir('sonar-libraries/main')
}

tasks.register('copySonarTestLibraries', Sync) {
    from configurations.testRuntimeClasspath
    into layout.buildDirectory.dir('sonar-libraries/test')
}
```

서비스별 실행 예시는 다음과 같다.

```cmd
cd backend\api-gateway
gradlew.bat clean test copySonarLibraries copySonarTestLibraries
cd ..\..

cd backend\iot-device-service
gradlew.bat clean test copySonarLibraries copySonarTestLibraries
cd ..\..

cd backend\monitoring-service
gradlew.bat clean test copySonarLibraries copySonarTestLibraries
cd ..\..
```

세 서비스 모두 테스트, JaCoCo XML 생성, Main/Test 라이브러리 복사에 성공했다.

`sonar-project.properties`에는 다음 산출물을 연결했다.

- Main Java class: `build/classes/java/main`
- Test Java class: `build/classes/java/test`
- Main dependency: `build/sonar-libraries/main/*.jar`
- Test dependency: `build/sonar-libraries/test/*.jar`
- JaCoCo XML: `build/reports/jacoco/test/jacocoTestReport.xml`

이 과정을 통해 다음 경고를 제거했다.

```text
Missing 'sonar.java.libraries' property
Missing 'sonar.java.test.binaries' and 'sonar.java.test.libraries' properties
```

라이브러리를 제공한 뒤 기존보다 더 많은 Security 및 Reliability Issue가 발견됐다.
이는 코드 품질이 갑자기 낮아진 것이 아니라 Java Analyzer가 Spring Security 등의 실제 타입과
호출 관계를 더 정확하게 분석하게 된 결과다.

## 6. 로컬 전체 분석 결과

Python과 Java 전체 Coverage 및 Java 라이브러리를 반영한 최종 로컬 결과는 다음과 같다.

| 지표 | 결과 |
| --- | ---: |
| 분석 코드 | 약 9k Lines of Code |
| Security | 4 Issues / D |
| Reliability | 7 Issues / C |
| Maintainability | 78 Issues / A |
| Coverage | 42.6% / 약 3.8k Lines to cover |
| Duplications | 1.0% / 약 11k Lines |
| Security Hotspots | 0 |

Python과 일부 코드만 포함했을 때보다 전체 Java 소스를 포함한 뒤 Coverage가 낮아진 것은
테스트 대상 코드의 분모가 증가했기 때문이다. Scanner 오류나 Coverage XML 누락으로 인한
감소는 아니다.

Security Issue 4건은 Spring Security 설정에서 CSRF 보호를 비활성화한 부분이다. 현재처럼
Cookie/Session이 아닌 Authorization Header의 Bearer JWT를 사용하는 API라면 비활성화를
정당화할 수 있지만, 향후 Cookie 기반 인증을 사용한다면 다시 검토해야 한다. 또한
`app.security.enabled`의 기본값이 비활성화되는 Fail-open 설정인지 별도 점검이 필요하다.

## 7. Scanner 실행 방식

SonarScanner는 Windows에 직접 설치하지 않고 Docker 이미지를 사용했다.

```text
Windows 직접 설치: 사용하지 않음
Docker 이미지: sonarsource/sonar-scanner-cli
```

`docker run --rm` 실행 시 임시 컨테이너가 생성되고 분석 종료 후 컨테이너는 삭제된다.
Scanner 이미지는 로컬 Docker에 남아 다음 실행에서 재사용된다.

Scanner는 실행되는 PC 또는 Jenkins Workspace에서 소스 파일과 빌드 보고서를 읽고,
분석 보고서를 SonarQube 서버로 전송한다. SonarQube가 테스트를 직접 실행하는 것은 아니므로
Scanner 실행 전에 pytest-cov와 JaCoCo 보고서를 먼저 생성해야 한다.

## 8. SSAFY 공용 SonarQube 연동

SSAFY 공용 서버에 프로젝트를 생성하고 프로젝트 전용 분석 토큰을 발급했다.

| 항목 | 값 |
| --- | --- |
| Server URL | `https://sonarqube.ssafy.com` |
| Project Key | `S15P21D201` |
| Project Name | `S15P21D201` |
| Token | 저장소에 기록하지 않고 실행 환경에서만 설정 |

토큰 값은 명령행이나 설정 파일에 직접 기록하지 않고 CMD 환경변수로 전달한다.

```cmd
set "SONAR_TOKEN=발급받은_토큰"
```

토큰 유효성은 실제 값을 출력하지 않고 다음과 같이 확인했다.

```cmd
curl.exe -s -u "%SONAR_TOKEN%:" https://sonarqube.ssafy.com/api/authentication/validate
```

정상 응답은 다음과 같다.

```json
{"valid":true}
```

공용 서버 분석은 저장소 루트에서 다음과 같이 실행한다.

```cmd
docker run --rm ^
  -e SONAR_HOST_URL=https://sonarqube.ssafy.com ^
  -e SONAR_TOKEN ^
  -v "%cd%:/usr/src" ^
  -w /usr/src ^
  sonarsource/sonar-scanner-cli
```

최종 실행 결과는 다음과 같다.

```text
Analysis total time: 6:01.005 s
SonarScanner Engine completed successfully
EXECUTION SUCCESS
Total time: 6:19.194s
```

이제 팀원은 SSAFY 공용 프로젝트에 대한 Browse 권한이 있으면 동일한 분석 결과와 대시보드를
볼 수 있다. 별도 EC2에 SonarQube와 PostgreSQL을 배포할 필요는 없다.

## 9. 보안 주의사항

- SonarQube 토큰과 DB 비밀번호는 Git에 커밋하지 않는다.
- 토큰을 채팅, 문서, 스크린샷 또는 Jenkinsfile에 직접 노출하지 않는다.
- 토큰이 노출되면 즉시 폐기하고 새 토큰을 발급한다.
- Jenkins 연동 시 토큰은 Jenkins Credentials에 Secret Text로 등록한다.
- 팀원이 직접 분석할 때는 각자 토큰을 발급하거나 권한 정책에 맞는 전용 CI 토큰을 사용한다.
- `coverage.xml`, Java `build` 디렉터리와 복사된 라이브러리는 생성 산출물이므로 Git에 커밋하지 않는다.

## 10. 현재 상태와 다음 작업

### 완료

- [x] 로컬 SonarQube 및 전용 PostgreSQL Docker 구성
- [x] 로컬 로그인 및 프로젝트 생성
- [x] Docker SonarScanner 실행 검증
- [x] Frontend, Python, Java 3개 서비스 분석 범위 설정
- [x] Python 3.11 및 pytest-cov Coverage 연동
- [x] Java 3개 서비스 JaCoCo XML 연동
- [x] Java Main/Test Binary 및 Library 경로 연동
- [x] 전체 로컬 분석 및 대시보드 확인
- [x] SSAFY 공용 SonarQube 프로젝트와 토큰 생성
- [x] SSAFY 공용 서버 토큰 인증 확인
- [x] SSAFY 공용 서버 최초 분석 업로드 성공

### 후속 작업

- [ ] SSAFY 대시보드에서 최종 지표와 Background Task 정상 처리 확인
- [ ] 팀원 계정의 프로젝트 조회 권한 확인
- [ ] 발견된 Security 4건과 Reliability 7건의 실제 위험도 검토
- [ ] 필요한 Issue 수정 또는 근거를 남기고 상태 처리
- [ ] Frontend 테스트 및 Coverage 도구 도입 검토
- [ ] Jenkins Credentials에 SonarQube 토큰 등록
- [ ] pytest, Gradle test, Coverage 생성 및 Scanner 실행을 Jenkins Pipeline으로 자동화
- [ ] Quality Gate 실패 시 배포를 중단할지 팀 정책 확정
- [ ] Scanner와 SonarQube Docker 이미지 버전 고정 검토

## 11. 결론

오늘 작업으로 다중 언어 Monorepo의 소스 분석과 Python/Java Coverage 수집을 로컬에서 먼저
검증했고, 같은 설정으로 SSAFY 공용 SonarQube에 분석 보고서를 업로드하는 데 성공했다.

현재는 수동 실행 방식이지만 품질 분석에 필요한 프로젝트 설정과 테스트 산출물 생성 흐름은
준비됐다. 다음 단계는 공용 대시보드의 Issue를 팀 기준에 맞게 검토하고, 검증된 명령을
Jenkins Pipeline에 옮겨 `develop` 또는 배포 대상 브랜치 분석을 자동화하는 것이다.

