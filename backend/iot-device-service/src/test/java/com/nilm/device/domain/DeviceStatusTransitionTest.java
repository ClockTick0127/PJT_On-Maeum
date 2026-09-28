package com.nilm.device.domain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.nilm.device.common.InvalidStateTransitionException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DeviceStatusTransitionTest {

    private Device newDevice() {
        return new Device("H001", DeviceType.CLAMP, "분전반", "1.0.0");
    }

    @Test
    @DisplayName("정상 생명주기: REGISTERED → ACTIVE → SUSPENDED → ACTIVE → RETIRED")
    void allowsNormalLifecycle() {
        Device device = newDevice();
        assertEquals(DeviceStatus.REGISTERED, device.transitionTo(DeviceStatus.ACTIVE));
        assertEquals(DeviceStatus.ACTIVE, device.transitionTo(DeviceStatus.SUSPENDED));
        assertEquals(DeviceStatus.SUSPENDED, device.transitionTo(DeviceStatus.ACTIVE));
        assertEquals(DeviceStatus.ACTIVE, device.transitionTo(DeviceStatus.RETIRED));
        assertEquals(DeviceStatus.RETIRED, device.getStatus());
    }

    @Test
    @DisplayName("REGISTERED에서 SUSPENDED로 바로 갈 수 없다")
    void rejectsRegisteredToSuspended() {
        assertThrows(InvalidStateTransitionException.class,
                () -> newDevice().transitionTo(DeviceStatus.SUSPENDED));
    }

    @Test
    @DisplayName("RETIRED는 종착 상태 — 어떤 전이도 불가")
    void retiredIsTerminal() {
        Device device = newDevice();
        device.transitionTo(DeviceStatus.ACTIVE);
        device.transitionTo(DeviceStatus.RETIRED);
        assertThrows(InvalidStateTransitionException.class,
                () -> device.transitionTo(DeviceStatus.ACTIVE));
        assertThrows(InvalidStateTransitionException.class,
                () -> device.transitionTo(DeviceStatus.REGISTERED));
    }

    @Test
    @DisplayName("같은 상태로의 전이도 거부된다")
    void rejectsSelfTransition() {
        assertThrows(InvalidStateTransitionException.class,
                () -> newDevice().transitionTo(DeviceStatus.REGISTERED));
    }
}
