-- MQTT 계정을 Mosquitto passwd 파일로 동기화하기 위한 브로커 호환 해시.
-- 평문은 저장하지 않으므로, 발급 시점에 BCrypt(검증용)와 함께 이 해시를 만들어 둔다.
-- 형식: $7$<iterations>$<salt b64>$<hash b64>  (PBKDF2-HMAC-SHA512)
ALTER TABLE device_credentials ADD COLUMN mosquitto_hash VARCHAR(300);
