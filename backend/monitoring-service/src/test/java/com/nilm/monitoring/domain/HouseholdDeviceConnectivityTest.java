package com.nilm.monitoring.domain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 기기 접속 상태 반영 규칙.
 * 틀리면 살아 있는 기기를 죽은 것으로 판정해 진짜 위험을 놓친다.
 */
class HouseholdDeviceConnectivityTest {

    private static final OffsetDateTime T1 =
            OffsetDateTime.of(2026, 9, 22, 10, 0, 0, 0, ZoneOffset.UTC);
    private static final OffsetDateTime T2 = T1.plusMinutes(10);

    private HouseholdDeviceConnectivity at(OffsetDateTime t,
                                           HouseholdDeviceConnectivity.Status s) {
        return new HouseholdDeviceConnectivity(1L, "H001", s, t);
    }

    @Test
    @DisplayName("더 최근 사건은 반영한다")
    void appliesNewer() {
        var c = at(T1, HouseholdDeviceConnectivity.Status.ONLINE);

        assertTrue(c.applyIfNewer(HouseholdDeviceConnectivity.Status.OFFLINE, T2));

        assertEquals(HouseholdDeviceConnectivity.Status.OFFLINE, c.getConnectionStatus());
        assertEquals(T2, c.getOccurredAt());
    }

    @Test
    @DisplayName("늦게 도착한 옛 사건은 최신 상태를 덮지 않는다 — 살아 있는 기기를 죽은 것으로 만들면 안 된다")
    void ignoresStaleEvent() {
        var c = at(T2, HouseholdDeviceConnectivity.Status.ONLINE);

        assertFalse(c.applyIfNewer(HouseholdDeviceConnectivity.Status.OFFLINE, T1));

        assertEquals(HouseholdDeviceConnectivity.Status.ONLINE, c.getConnectionStatus(),
                "Kafka 재전송으로 옛 OFFLINE이 늦게 와도 현재 ONLINE을 유지해야 한다");
        assertEquals(T2, c.getOccurredAt());
    }

    @Test
    @DisplayName("같은 시각의 사건은 반영한다 — 경계에서 상태가 멈추면 안 된다")
    void appliesSameInstant() {
        var c = at(T1, HouseholdDeviceConnectivity.Status.ONLINE);

        assertTrue(c.applyIfNewer(HouseholdDeviceConnectivity.Status.OFFLINE, T1));

        assertEquals(HouseholdDeviceConnectivity.Status.OFFLINE, c.getConnectionStatus());
    }
}
