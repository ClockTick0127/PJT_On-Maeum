-- analysis.snapshot.v1은 전환이 아니라 매 시점의 ON/OFF 상태를 통째로 보낸다.
-- 직전 상태를 남겨두어야 ON→OFF 전환을 가려내 마지막 활동으로 기록할 수 있다.
CREATE TABLE appliance_states (
    household_id VARCHAR(255) NOT NULL,
    appliance_type VARCHAR(50) NOT NULL,
    is_on BOOLEAN NOT NULL,
    -- 이 상태로 바뀐 스냅샷의 observed_at. 늦게 도착한 과거 스냅샷을 걸러내는 데에도 쓴다.
    changed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (household_id, appliance_type)
);
