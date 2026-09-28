# On:마음 프론트엔드

React, TypeScript, Vite 기반의 돌봄 대시보드입니다. REST 요청은 axios, 서버 상태는 TanStack Query, 화면 스타일은 Tailwind CSS로 관리합니다.

## 실행

```powershell
cd frontend
npm ci
npm run dev
```

개발 서버는 `/api` 요청을 `http://localhost:8080`으로 전달합니다.

Web Push를 사용하려면 백엔드의 `WEB_PUSH_VAPID_PUBLIC_KEY`와 같은 공개키를 프론트엔드 빌드 환경에 설정합니다.

```text
VITE_WEB_PUSH_VAPID_PUBLIC_KEY=공개키
```

## src 구조

```text
src/
├─ api/          # axios 인스턴스, 토큰 갱신, 도메인별 HTTP 함수, SSE
├─ app/          # React Router 설정과 인증 라우트
├─ assets/       # 이미지 등 정적 리소스
├─ components/   # 공통 및 페이지 전용 컴포넌트
├─ hooks/        # TanStack Query 쿼리·뮤테이션과 세션 훅
├─ pages/        # 페이지 상태와 컴포넌트 조합
├─ providers/    # QueryClient 등 전역 Provider
├─ styles/       # Tailwind 진입점과 전역·접근성 스타일
├─ types/        # 도메인 타입
├─ utils/        # 날짜·전화번호·표시 형식 유틸리티
├─ App.tsx       # RouterProvider
└─ main.tsx      # StrictMode, QueryProvider, 서비스 워커 등록
```

Redux 저장소를 사용하지 않으므로 `PersistGate`는 두지 않았습니다. 토큰은 기존 동작과 동일하게 탭 단위 `sessionStorage`에 저장합니다.

## 주요 경로

- `/`: 서비스 선택
- `/staff`: 담당자 대시보드
- `/staff?view=subjects`: 대상자 검색·필터·등록
- `/staff?view=alerts`: 최근 알림
- `/staff/subjects/{subjectId}`: 대상자 상세, 전력 사용량, 이상 징후
- `/user`: 대상자 홈, 외출 설정, 담당자 연락
- `/user?notificationId={id}`: 알림 응답
- `/user/orange-preview`: 비교 화면

## API 구조

- `api/client.ts`: 토큰 없는 `publicApi`, 토큰을 포함하는 `authApi`, 401 토큰 갱신
- `api/auth.ts`: 로그인
- `api/monitoring.ts`: 모니터링 REST 요청
- `api/stream.ts`: axios fetch adapter 기반 인증 SSE
- `hooks/useMonitoring.ts`: 쿼리 키, 주기 조회, 뮤테이션, 캐시 무효화, SSE 캐시 반영

페이지와 컴포넌트는 URL이나 axios 설정을 직접 만들지 않고 도메인 함수와 커스텀 훅을 사용합니다.

## 테스트와 커버리지

테스트는 서비스 코드와 분리된 `tests`에서 관리합니다.

```powershell
npm run test
npm run test:watch
npm run test:coverage
```

`test:coverage` 실행 후 사람이 확인할 수 있는 터미널 요약과 `coverage/lcov.info`가 생성됩니다. 현재 커버리지 대상은 API·타입 변환·유틸리티처럼 회귀 위험이 크고 UI와 분리 가능한 코드입니다. 화면 테스트를 추가할 때는 `vite.config.ts`의 `coverage.include`에 `src/pages` 또는 `src/components`를 추가하면 됩니다.

frontend 디렉터리에서 SonarScanner를 실행하면 `sonar-project.properties`가 `coverage/lcov.info`를 읽습니다. 순서는 다음과 같습니다.

```powershell
npm run test:coverage
sonar-scanner
```

## 정적 확인

```powershell
npm run lint
```

프로젝트 지침에 따라 빌드 명령은 검증 과정에서 실행하지 않습니다.
