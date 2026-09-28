package com.nilm.device.api.dto;

import com.nilm.device.domain.Device;
import com.nilm.device.domain.DeviceStatus;
import com.nilm.device.domain.DeviceType;
import com.nilm.device.domain.InstallHistory;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.time.OffsetDateTime;
import java.util.List;

public final class DeviceDtos {

    private DeviceDtos() {
    }

    public record RegisterRequest(
            @NotBlank @Pattern(regexp = "^H\\d{3}$", message = "house_id는 H001 형식이어야 합니다")
            String houseId,
            @NotNull
            DeviceType deviceType,
            @Size(max = 50)
            String location,
            @Size(max = 20)
            String firmwareVer
    ) {
    }

    public record Response(
            Long deviceId,
            String houseId,
            DeviceType deviceType,
            String location,
            String firmwareVer,
            DeviceStatus status,
            /** MQTT 접속 상태 — status(생명주기)와 독립이다 */
            Device.ConnectionStatus connectionStatus,
            OffsetDateTime lastSeenAt,
            OffsetDateTime registeredAt
    ) {
        public static Response from(Device d) {
            return new Response(d.getDeviceId(), d.getHouseId(), d.getDeviceType(),
                    d.getLocation(), d.getFirmwareVer(), d.getStatus(),
                    d.getConnectionStatus(), d.getLastSeenAt(), d.getRegisteredAt());
        }
    }

    /** 등록 응답 — mqttPassword는 이 응답에서 1회만 노출되고 서버에는 해시만 남는다. */
    public record RegisterResponse(
            Response device,
            String mqttUsername,
            String mqttPassword,
            List<String> aclTopics
    ) {
    }

    /** 로테이션 응답 — 새 mqttPassword는 이 응답에서 1회만 노출된다. */
    public record CredentialRotateResponse(
            Long deviceId,
            String mqttUsername,
            String mqttPassword
    ) {
    }

    public record StatusChangeRequest(
            @NotNull
            DeviceStatus status,
            @Size(max = 200)
            String reason
            // changedBy는 받지 않는다 — 클라이언트가 보내면 남을 사칭할 수 있다.
            // 서버가 인증된 호출자로 채운다.
    ) {
    }

    public record HistoryResponse(
            Long historyId,
            InstallHistory.EventType eventType,
            String fromValue,
            String toValue,
            String reason,
            String changedBy,
            OffsetDateTime changedAt
    ) {
        public static HistoryResponse from(InstallHistory h) {
            return new HistoryResponse(h.getHistoryId(), h.getEventType(), h.getFromValue(),
                    h.getToValue(), h.getReason(), h.getChangedBy(), h.getChangedAt());
        }
    }
}
