-- 구간에서 실제로 적산에 쓰인 시간(초).
-- 이 값이 없으면 "그 시간에 전기를 안 썼다"와 "그 시간을 보지 못했다"를 구분할 수 없다.
-- 앞의 것은 평온한 하루지만 뒤의 것은 관측이 끊겼다는 뜻이라, 독거 대상자 화면에서
-- 둘을 같은 빈칸으로 그리면 안 된다.
ALTER TABLE hourly_power_usage
    ADD COLUMN observed_seconds INTEGER NOT NULL DEFAULT 0;

ALTER TABLE hourly_power_usage
    ADD CONSTRAINT chk_hourly_power_usage_observed_seconds
        CHECK (observed_seconds >= 0);
