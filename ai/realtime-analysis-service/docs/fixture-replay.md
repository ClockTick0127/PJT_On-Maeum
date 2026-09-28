# TLS fixture 발행기

`python -m realtime_analysis.fixture_publisher`는 잠긴 panel만 읽어 MQTT로 발행한다.
프로파일 SHA와 전체 행을 연결 전에 검증한다. 기본 TLS는 시스템 CA 또는 --ca-file을
사용하며 hostname/certificate 검증을 끌 수 없다. 평문은 로컬 시험에 한해
`--allow-plaintext`를 명시한다. 자격증명은 MQTT_USER/MQTT_PASS 환경변수로 전달한다.

```sh
python -m realtime_analysis.fixture_publisher \
  --asset-root /assets --appliance kettle --household TEST_HOUSE \
  --run-id r3-kettle-001 --start-time 2026-09-21T12:00:00+09:00 \
  --mqtt-host BROKER_HOST --mqtt-port 8883 --ca-file /certs/ca.crt \
  --output /evidence/publish-001
```

기본 topic은 v1/power/demo/{household_id}/main이다. 기존 시뮬레이터는 prepare/replay_mqtt를
호출할 수 있다. 전용 bridge에서 power.scene.v2로 전달한다. MODEL_HOUSEHOLD_ID,
ANALYSIS_RUN_ID, MODEL_APPLIANCE는 분석 worker와 일치해야 한다.

기본 wall interval=1초다. --interval 0.01은 전달 시험을 가속하지만 measured_at은 항상
1초 간격이다. 재시도는 전체 panel을 같은 ID로 재발행한다. MQTT ACK만으로 중간부터
시작하지 않으며 백엔드의 durable 멱등성을 이용한다. --output은 매번 새 디렉터리다.
manifest.json에는 panel SHA/run/start/expected rows가, progress.json에는 발행 ACK 진행률이
기록된다. published는 모델·서비스 처리 완료가 아니다. 별도 E2E readback으로 확인해야 한다.
TLS handshake/인증 실패는 failed로 기록되고 성공으로 대체하지 않는다. 비밀정보를 출력하지 않는다.
