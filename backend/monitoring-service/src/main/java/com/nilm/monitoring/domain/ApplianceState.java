package com.nilm.monitoring.domain;

import jakarta.persistence.*;
import lombok.Getter;

import java.time.OffsetDateTime;

/**
 * 가구별·가전별 최근 ON/OFF 상태.
 * 스냅샷에는 전환이 아니라 현재 상태만 담겨 오므로, 직전 상태를 여기에 남겨
 * ON→OFF 전환을 가려낸다.
 */
@Entity
@Getter
@Table(name = "appliance_states")
@IdClass(ApplianceStateId.class)
public class ApplianceState {

    @Id
    @Column(name = "household_id", nullable = false)
    private String householdId;

    @Id
    @Column(name = "appliance_type", nullable = false, length = 50)
    private String applianceType;

    @Column(name = "is_on", nullable = false)
    private boolean on;

    @Column(name = "changed_at", nullable = false)
    private OffsetDateTime changedAt;

    protected ApplianceState() {
    }

    public ApplianceState(
            String householdId,
            String applianceType,
            boolean on,
            OffsetDateTime changedAt
    ) {
        this.householdId = householdId;
        this.applianceType = applianceType;
        this.on = on;
        this.changedAt = changedAt;
    }

    /** 스냅샷 한 건이 만든 상태 전환의 방향. */
    public enum Transition {

        /** 직전과 같은 상태라 아무 일도 없었다. */
        NONE,

        /** OFF에서 ON으로. 당일 사용 사실을 남긴다. */
        TURNED_ON,

        /** ON에서 OFF로. 마지막 활동 시각이 된다. */
        TURNED_OFF
    }

    /**
     * 상태가 실제로 바뀐 경우에만 갱신한다.
     *
     * <p>ON 전환도 알려준다. 당일 첫 사용 시각과 사용 횟수는 루틴 미사용·활동 감소
     * 지표의 현재 값이라, OFF 전환만 보던 때와 달리 양쪽이 모두 필요하다.
     *
     * @return 이번 스냅샷이 만든 전환 방향
     */
    public Transition changeTo(boolean nextOn, OffsetDateTime observedAt) {
        if (this.on == nextOn) {
            return Transition.NONE;
        }
        Transition transition = nextOn ? Transition.TURNED_ON : Transition.TURNED_OFF;
        this.on = nextOn;
        this.changedAt = observedAt;
        return transition;
    }
}
