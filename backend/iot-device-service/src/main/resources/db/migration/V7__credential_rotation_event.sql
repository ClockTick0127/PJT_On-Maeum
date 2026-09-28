-- 계정 로테이션도 설치 이력으로 남기기 위해 event_type 허용 목록 확장
ALTER TABLE install_history DROP CONSTRAINT chk_history_event;
ALTER TABLE install_history ADD CONSTRAINT chk_history_event CHECK (event_type IN
    ('INSTALLED', 'RELOCATED', 'STATUS_CHANGED', 'FIRMWARE_UPDATED', 'RETIRED', 'CREDENTIAL_ROTATED'));
