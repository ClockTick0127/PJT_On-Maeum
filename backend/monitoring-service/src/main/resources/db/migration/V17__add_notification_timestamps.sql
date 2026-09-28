-- 알림 행의 생성·갱신 시각.
-- "알림이 언제 만들어졌고 언제 마지막으로 상태가 바뀌었는지"를 행 자체가 답하게 한다.
ALTER TABLE notifications
    ADD COLUMN created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE notifications
    ADD COLUMN updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP;

-- 기존 행의 실제 생성 시각은 알 수 없다. 응답·담당자 처리 시각이 있으면
-- 그중 이른 값을 생성 시각의 상한 근사치로, 늦은 값을 마지막 갱신 시각으로 쓴다.
UPDATE notifications
SET created_at = LEAST(
        COALESCE(responded_at, CURRENT_TIMESTAMP),
        COALESCE(manager_status_updated_at, CURRENT_TIMESTAMP)
    );

UPDATE notifications
SET updated_at = GREATEST(
        COALESCE(responded_at, created_at),
        COALESCE(manager_status_updated_at, created_at)
    );
