-- 가구 기기 접속 상태 — device-service가 MQTT LWT로 감지해 보내 준다.
--
-- 왜 필요한가: 안부 확인에서 "데이터가 안 온다"는 것 자체가 위험 신호인데,
-- 그 원인이 사람이 활동을 안 한 것인지 기기가 꺼진 것인지 구분하지 못하면
-- Wi-Fi가 끊길 때마다 보호자에게 위험 알림이 가는 오경보 시스템이 된다.
--
-- device_db와는 FK로 묶지 않는다(서비스 간 FK 금지). house_id 값만 공유한다.
-- 한 가구에 기기가 여러 대일 수 있으므로 기기 단위로 저장하고,
-- 판정은 "그 시각에 살아 있던 기기가 하나라도 있었나"로 한다.

CREATE TABLE household_device_connectivity (
    device_id          BIGINT PRIMARY KEY,
    household_id       VARCHAR(255) NOT NULL,
    connection_status  VARCHAR(10)  NOT NULL,
    occurred_at        TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_hdc_status CHECK (connection_status IN ('UNKNOWN', 'ONLINE', 'OFFLINE'))
);

CREATE INDEX idx_hdc_household ON household_device_connectivity (household_id);
