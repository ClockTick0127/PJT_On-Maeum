package com.nilm.device.domain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 기기 접속 상태(LWT) 규칙.
 * 틀리면 "언제부터 끊겼는지"를 잃거나 오프라인을 놓쳐 오경보로 이어진다.
 */
class DeviceConnectionTest {

    private static final OffsetDateTime T1 =
            OffsetDateTime.of(2026, 9, 22, 10, 0, 0, 0, ZoneOffset.UTC);
    private static final OffsetDateTime T2 = T1.plusMinutes(5);
    private static final OffsetDateTime T3 = T1.plusMinutes(10);

    private Device device() {
        return new Device("H001", DeviceType.CLAMP, "분전반", "1.0.0");
    }

    @Test
    @DisplayName("등록 직후에는 한 번도 붙은 적 없음(UNKNOWN)이다")
    void startsUnknown() {
        Device d = device();

        assertEquals(Device.ConnectionStatus.UNKNOWN, d.getConnectionStatus());
        assertNull(d.getLastSeenAt());
    }

    @Test
    @DisplayName("온라인 통지는 마지막 확인 시각을 갱신한다")
    void onlineUpdatesLastSeen() {
        Device d = device();

        assertTrue(d.updateConnection(Device.ConnectionStatus.ONLINE, T1));

        assertEquals(Device.ConnectionStatus.ONLINE, d.getConnectionStatus());
        assertEquals(T1, d.getLastSeenAt());
        assertEquals(T1, d.getConnectionChangedAt());
    }

    @Test
    @DisplayName("같은 상태가 반복 통지되면 변경 시각을 밀지 않는다 — 언제부터 끊겼는지를 잃으면 안 된다")
    void repeatedStatusKeepsChangedAt() {
        Device d = device();
        d.updateConnection(Device.ConnectionStatus.ONLINE, T1);
        d.updateConnection(Device.ConnectionStatus.OFFLINE, T2);

        boolean changed = d.updateConnection(Device.ConnectionStatus.OFFLINE, T3);

        assertFalse(changed, "상태가 그대로면 변경으로 보지 않는다");
        assertEquals(T2, d.getConnectionChangedAt(), "끊긴 시각은 T2로 유지되어야 한다");
    }

    @Test
    @DisplayName("오프라인이 되어도 마지막 확인 시각은 지우지 않는다")
    void offlineKeepsLastSeen() {
        Device d = device();
        d.updateConnection(Device.ConnectionStatus.ONLINE, T1);

        d.updateConnection(Device.ConnectionStatus.OFFLINE, T2);

        assertEquals(T1, d.getLastSeenAt(), "마지막으로 살아 있던 시각은 남아야 한다");
        assertEquals(Device.ConnectionStatus.OFFLINE, d.getConnectionStatus());
    }

    @Test
    @DisplayName("접속 상태는 생명주기(status)와 독립이다 — ACTIVE인데 OFFLINE일 수 있다")
    void connectionIsIndependentOfLifecycle() {
        Device d = device();
        d.transitionTo(DeviceStatus.ACTIVE);

        d.updateConnection(Device.ConnectionStatus.OFFLINE, T1);

        assertEquals(DeviceStatus.ACTIVE, d.getStatus());
        assertEquals(Device.ConnectionStatus.OFFLINE, d.getConnectionStatus());
    }

    @Test
    @DisplayName("상태나 시각이 비면 거부한다")
    void rejectsNulls() {
        Device d = device();

        assertThrows(IllegalArgumentException.class,
                () -> d.updateConnection(null, T1));
        assertThrows(IllegalArgumentException.class,
                () -> d.updateConnection(Device.ConnectionStatus.ONLINE, null));
    }
}
