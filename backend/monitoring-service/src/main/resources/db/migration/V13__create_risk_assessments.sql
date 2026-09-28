-- 모니터링이 스스로 위험을 평가하기 위해 필요한 현재 상태 입력과 평가 이력.
-- 설계 문서 11장(평가 자격·점수식·등급 정책)과 12장(평가 이력 필드)을 따른다.

-- 가구별 마지막 관측 시각. 평가에서 "현재 값이 얼마나 신선한가"를 판정하는 근거다.
-- 스냅샷이 끊기면 무활동·활동량 지표를 계산하지 않고 제외한다.
CREATE TABLE household_observations (
    household_id VARCHAR(255) PRIMARY KEY,
    -- 분석 서비스가 측정한 시각. 되돌아가지 않는다.
    last_observed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    -- 분석 서비스가 발행한 시각. 관측과 발행 사이 지연을 따로 보기 위해 남긴다.
    last_published_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL
);

-- 가구·가전·영업일(KST)별 당일 사용 사실.
-- 루틴 미사용(M) 지표가 "오늘 이미 썼는가"와 "오늘 몇 번 켰는가"를 여기서 읽는다.
CREATE TABLE household_daily_appliance_usage (
    household_id VARCHAR(255) NOT NULL,
    appliance_type VARCHAR(50) NOT NULL,
    -- KST 기준 날짜. 프로필의 일별 집계와 같은 영업일 경계를 쓴다.
    usage_date DATE NOT NULL,
    -- 그날 처음 켜진 시각. 한 번만 채우고 이후 스냅샷으로 덮어쓰지 않는다.
    first_on_at TIMESTAMP WITH TIME ZONE NOT NULL,
    last_on_at TIMESTAMP WITH TIME ZONE NOT NULL,
    -- OFF→ON 전환 횟수. 활동 감소(A) 지표의 현재 값이다.
    start_count INTEGER NOT NULL,

    CONSTRAINT pk_household_daily_appliance_usage
        PRIMARY KEY (household_id, appliance_type, usage_date)
);

CREATE INDEX idx_household_daily_appliance_usage_day
    ON household_daily_appliance_usage(household_id, usage_date);

-- 모니터링 자체 평가 1회의 기록. 설계 12장의 평가 이력 필드를 담는다.
-- 지표별 관측값·비교 통계값은 indicators에 JSON 문자열로 넣는다.
-- 테스트 DB가 H2라 jsonb를 쓸 수 없어 TEXT로 둔다.
CREATE TABLE risk_assessments (
    id UUID PRIMARY KEY,
    subject_id BIGINT NOT NULL,
    household_id VARCHAR(255) NOT NULL,
    assessed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    -- 평가가 근거로 삼은 현재 입력의 기준 시각(마지막 관측). 없으면 NULL.
    data_as_of TIMESTAMP WITH TIME ZONE,
    -- 무엇이 이 평가를 불렀는가(ACTIVITY/AWAY_MODE/PROFILE_UPDATED/ASSESSMENT).
    trigger VARCHAR(30) NOT NULL,
    -- 평가 불가를 0점·정상으로 적지 않는다. 계산하지 못했으면 NULL이다.
    risk_score INTEGER,
    risk_level VARCHAR(20),
    assessment_status VARCHAR(30) NOT NULL,
    confidence DOUBLE PRECISION NOT NULL,
    indicators TEXT,
    profile_version VARCHAR(100),
    policy_version VARCHAR(50) NOT NULL,
    score_version VARCHAR(50) NOT NULL,
    -- 이 평가로 유효 등급이 실제로 움직였는지.
    level_changed BOOLEAN NOT NULL DEFAULT FALSE,
    -- 이 평가가 만든 알림. 만들지 않았으면 NULL.
    notification_id BIGINT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE INDEX idx_risk_assessments_subject
    ON risk_assessments(subject_id, assessed_at DESC);

-- 자체 평가 결과를 담는 슬롯. 기존 current_risk_level/current_risk_score는
-- 이제 "유효 등급" = max(자체 평가 등급, 이벤트 등급)이라는 뜻으로 바뀐다.
ALTER TABLE subjects ADD COLUMN assessed_risk_level VARCHAR(20);
ALTER TABLE subjects ADD COLUMN assessed_risk_score INTEGER;
ALTER TABLE subjects ADD COLUMN assessment_status VARCHAR(30);
ALTER TABLE subjects ADD COLUMN assessment_confidence DOUBLE PRECISION;
ALTER TABLE subjects ADD COLUMN assessed_at TIMESTAMP WITH TIME ZONE;
-- 마지막으로 VALID였던 평가 시각. 평가 불가가 이어져도 이 값은 지워지지 않는다.
ALTER TABLE subjects ADD COLUMN last_valid_assessed_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE subjects ADD COLUMN risk_level_since TIMESTAMP WITH TIME ZONE;

-- 등급 전이 히스테리시스의 후보. 같은 후보가 유지시간을 채워야 실제로 움직인다.
ALTER TABLE subjects ADD COLUMN pending_risk_level VARCHAR(20);
ALTER TABLE subjects ADD COLUMN pending_since TIMESTAMP WITH TIME ZONE;

-- 분석 서비스 이벤트가 세운 등급 슬롯. 자체 평가와 섞지 않고 따로 보관한다.
ALTER TABLE subjects ADD COLUMN event_risk_level VARCHAR(20);
ALTER TABLE subjects ADD COLUMN event_risk_score INTEGER;
ALTER TABLE subjects ADD COLUMN event_risk_event_id UUID;
-- reason의 appliance_type. 이 가전의 ON→OFF 전환으로 슬롯을 해제한다.
ALTER TABLE subjects ADD COLUMN event_risk_appliance VARCHAR(50);
ALTER TABLE subjects ADD COLUMN event_risk_set_at TIMESTAMP WITH TIME ZONE;

-- 같은 등급이 이어질 때 알림을 다시 보낼지 판정하는 기준.
ALTER TABLE subjects ADD COLUMN last_alert_at TIMESTAMP WITH TIME ZONE;

-- 자체 평가가 만든 알림은 분석 이벤트에 걸리지 않는다.
-- 대상자를 알림 행에서 바로 읽을 수 있도록 subject_id를 둔다.
ALTER TABLE notifications ADD COLUMN subject_id BIGINT;
ALTER TABLE notifications ADD COLUMN assessment_id UUID;

-- 기존 행은 연결된 분석 이벤트에서 대상자를 채운다.
-- 이벤트가 없는 일일 요약 알림은 대상자를 특정할 수 없어 NULL로 남는다.
UPDATE notifications n
SET subject_id = (
    SELECT e.subject_id FROM analysis_events e WHERE e.id = n.event_id
)
WHERE n.event_id IS NOT NULL;

CREATE INDEX idx_notifications_subject
    ON notifications(subject_id, id DESC);
