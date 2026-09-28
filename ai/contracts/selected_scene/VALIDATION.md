# 1번 브랜치 검증 기록

- 브랜치: `feature/ai/inference-contract`
- 분기 기준: 로컬 `develop`의 `45c3282`
- 작성일: 2026-09-21
- 변경 범위: `ai/contracts/selected_scene/`만
- 실행 환경: Python 3.12, 기존 분석 서비스 가상환경의 Pydantic 2

## 수행 결과

`python -m unittest discover -s ai/contracts/selected_scene/tests -v`: **14개 통과**.
이 중 예제 검사는 정상 8개·오류 11개, 총 19개 사례를 모두 검증한다.

검사 범위: 준비 상태·미분석 UNKNOWN, 확률·boolean 타입, timezone, runtime/profile,
전환 일관성, 세션의 알려진/알 수 없는 시작, EOF와 물리적 종료 구분, 위험 이벤트 경과 시간,
기존 AnalysisEvent 외곽 호환, 구조적 schema 출력, JSONL 실패 행·exit code 및 빈 입력 거부.

이전 브랜치에서 실제 broker를 통해 저장했던 로컬 응답을 `validate('snapshot', ..., broker=True)`로
재검사했다. 신규 모델 추론이나 원격/배포 시험을 수행한 결과가 아니다.

| 장면 | 실제 응답 수 | 계약 검사 |
|---|---:|---|
| kettle | 592 | 통과 |
| induction | 585 | 통과 |
| iron | 591 | 통과 |
| microwave | 825 | 통과 |
| hair_dryer | 761 | 통과 |
| vacuum_cleaner | 613 | 통과 |
| 합계 | 3,967 | 통과 |

원 기록은 로컬 `ai/evidence/broker-six-scenes/*.jsonl`이며 이 브랜치에 새로 포함하지 않는다.
다른 환경에서는 README의 CLI에 해당 환경의 실제 응답 JSONL을 전달해 같은 검사를 할 수 있다.
정상 snapshot 예제 2개는 이 브랜치에 포함되어 별도 원 기록 없이도 검증기를 시험할 수 있다.

## 미수행·후속 의존성

- 기반 모델 브랜치의 develop 병합: 미수행. 기존 3개 커밋은 원 브랜치에 보존.
- 신규 session/risk 계약: 합성 예제만 검증. 실제 생성·발행·DB 멱등 처리는 후속 4번 기능.
- 계약 모델의 기존 런타임 삽입: 미수행. 후속 2번 기능.
- 다른 파트 수정, 새 broker/EC2 배포, 화면 변경, 알림 생성/발송: 미수행.
- 프로파일 해시·연속 입력·stateful 규칙: 후속 런타임/통합 시험에서 검사.

결론: **AI 측 계약 초안과 실행 가능한 검증 도구 완료**. 상대 서비스의 계약 수락이나
최종 서비스 연동 완료를 의미하지 않는다.
