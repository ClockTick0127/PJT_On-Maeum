-- 기기 접속 상태 — MQTT LWT(Last Will and Testament)로 채운다.
--
-- 안부 확인 시스템에서 데이터가 끊긴 것 자체가 신호인데, 지금은 그 원인이
-- 정전인지 네트워크 장애인지 기기 고장인지 구분하지 못한다. 구분하지 못하면
-- Wi-Fi가 끊길 때마다 보호자에게 위험 알림이 가는 오경보 시스템이 된다.
--
-- LWT는 기기가 접속할 때 "내가 끊기면 이 메시지를 대신 보내 달라"고 브로커에 맡기는
-- 기능이다. 기기가 스스로 알릴 수 없는 상황(정전, 크래시)에서도 브로커가 알려 준다.
--
-- status(등록/활성/정지/폐기)는 운영자가 정하는 생명주기이고,
-- connection_status는 기기가 실제로 붙어 있는지다. 둘은 독립이다.
-- ACTIVE인데 OFFLINE = 정상 등록된 기기가 지금 끊겨 있음, 확인이 필요한 상태.
--
-- 주의: 테스트는 H2(PostgreSQL 호환 모드)로 돌아간다.
-- ALTER TABLE 한 문장에 컬럼 여러 개를 넣거나 COMMENT ON COLUMN을 쓰면 H2가 거부한다.

ALTER TABLE devices ADD COLUMN connection_status VARCHAR(10) DEFAULT 'UNKNOWN' NOT NULL;
ALTER TABLE devices ADD COLUMN last_seen_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE devices ADD COLUMN connection_changed_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE devices ADD CONSTRAINT chk_device_connection
    CHECK (connection_status IN ('UNKNOWN', 'ONLINE', 'OFFLINE'));

-- 상태 토픽 발행 권한을 기존 기기에도 부여한다.
-- 새 기기는 등록 시 DeviceService가 함께 만든다.
INSERT INTO device_acl (device_id, topic_pattern, permission)
SELECT d.device_id, 'v1/device/' || d.device_id || '/status', 'PUBLISH'
FROM devices d
WHERE NOT EXISTS (
    SELECT 1 FROM device_acl a
    WHERE a.device_id = d.device_id
      AND a.topic_pattern = 'v1/device/' || d.device_id || '/status'
);
