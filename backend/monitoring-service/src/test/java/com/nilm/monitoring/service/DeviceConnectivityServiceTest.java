package com.nilm.monitoring.service;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.nilm.monitoring.domain.HouseholdDeviceConnectivity;
import com.nilm.monitoring.repository.HouseholdDeviceConnectivityRepository;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * 오경보 방지 판정 — "그 시각에 기기가 끊겨 있었나".
 *
 * <p>여기가 틀리면 두 방향으로 다 위험하다. 너무 많이 억제하면 진짜 위험을 놓치고,
 * 억제하지 못하면 Wi-Fi가 끊길 때마다 보호자에게 알림이 간다.
 */
@ExtendWith(MockitoExtension.class)
class DeviceConnectivityServiceTest {

    private static final String HOUSE = "H001";
    private static final OffsetDateTime EVENT_AT =
            OffsetDateTime.of(2026, 9, 22, 12, 0, 0, 0, ZoneOffset.UTC);

    @Mock
    private HouseholdDeviceConnectivityRepository repository;

    @InjectMocks
    private DeviceConnectivityService service;

    private HouseholdDeviceConnectivity device(long id, HouseholdDeviceConnectivity.Status s,
                                               OffsetDateTime at) {
        return new HouseholdDeviceConnectivity(id, HOUSE, s, at);
    }

    @Test
    @DisplayName("기록이 없으면 끊긴 것으로 보지 않는다 — 모른다고 위험 알림을 막으면 안 된다")
    void noRecordMeansNotDisconnected() {
        when(repository.findByHouseholdId(anyString())).thenReturn(List.of());

        assertFalse(service.wasDisconnectedAt(HOUSE, EVENT_AT));
    }

    @Test
    @DisplayName("사건 전부터 끊겨 있었으면 억제 대상이다")
    void offlineBeforeEvent() {
        when(repository.findByHouseholdId(anyString())).thenReturn(List.of(
                device(1L, HouseholdDeviceConnectivity.Status.OFFLINE, EVENT_AT.minusMinutes(30))));

        assertTrue(service.wasDisconnectedAt(HOUSE, EVENT_AT));
    }

    @Test
    @DisplayName("한 대라도 살아 있었으면 억제하지 않는다 — 기기 하나 꺼졌다고 진짜 위험을 버리면 안 된다")
    void anyOnlineMeansConnected() {
        when(repository.findByHouseholdId(anyString())).thenReturn(List.of(
                device(1L, HouseholdDeviceConnectivity.Status.OFFLINE, EVENT_AT.minusMinutes(30)),
                device(2L, HouseholdDeviceConnectivity.Status.ONLINE, EVENT_AT.minusMinutes(5))));

        assertFalse(service.wasDisconnectedAt(HOUSE, EVENT_AT));
    }

    @Test
    @DisplayName("사건보다 나중에 일어난 상태 변화는 그 시점의 근거가 아니다")
    void ignoresStateAfterEvent() {
        // 사건 10분 뒤에 끊겼다 — 사건 당시에는 알 수 없던 사실이다
        when(repository.findByHouseholdId(anyString())).thenReturn(List.of(
                device(1L, HouseholdDeviceConnectivity.Status.OFFLINE, EVENT_AT.plusMinutes(10))));

        assertFalse(service.wasDisconnectedAt(HOUSE, EVENT_AT),
                "사건 이후의 상태로 과거를 판단하면 안 된다");
    }

    @Test
    @DisplayName("UNKNOWN만 있으면 억제한다 — 한 번도 붙은 적 없는 기기는 데이터를 보낼 수 없다")
    void unknownCountsAsDisconnected() {
        when(repository.findByHouseholdId(anyString())).thenReturn(List.of(
                device(1L, HouseholdDeviceConnectivity.Status.UNKNOWN, EVENT_AT.minusHours(1))));

        assertTrue(service.wasDisconnectedAt(HOUSE, EVENT_AT));
    }
}
