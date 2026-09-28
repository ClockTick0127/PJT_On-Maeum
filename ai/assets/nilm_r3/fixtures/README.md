# 선택한 실측 장면과 검산 전용 자료

각 candidate 폴더의 panel.csv는 원4특징·source_index·valid/context와 실제254초 prefix를 포함한다. metadata와 source_*는 원본 R2 기록이다. 원서버 경로를 사용자PC 경로로 덮어쓰지 않는다.

**evaluation_only/**에는 연구 정답·기존 점수·상태·요약이 있다. 새 inference와 backend decision은 이 디렉터리를 읽지 않는다. 재추론 후 별도 검산 코드에서만 읽는다. controls/에는 같은 profile을 대조할 주변·OFF·다른날 입력이 있다. 일반화 성능과 구분한다.

각 scene은 별도 가구/날짜/threshold를 선택한 F2다. 이 fixture 여섯 개를 합친 것이 한 가구의 여섯 가전 동시 사용은 아니다. source label UNKNOWN과 모델 UNKNOWN을 혼동하지 않는다.
