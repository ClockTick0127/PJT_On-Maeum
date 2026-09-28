-- 이상 징후 기록 화면은 알림마다 담당자가 어디까지 처리했는지 함께 보여준다.
-- 기존 행은 아직 확인하지 않은 것으로 간주한다.
ALTER TABLE notifications
    ADD COLUMN manager_response_status VARCHAR(20) NOT NULL DEFAULT 'UNCONFIRMED';

ALTER TABLE notifications
    ADD COLUMN manager_status_updated_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE notifications
    ADD CONSTRAINT chk_notifications_manager_response_status
        CHECK (manager_response_status IN (
            'UNCONFIRMED', 'ACKNOWLEDGED', 'RESOLVED'
        ));

-- 이벤트 단위로 알림을 찾아오는 조회(이상 징후 목록)를 위한 인덱스는
-- V6의 idx_notifications_event_id를 그대로 사용한다.
