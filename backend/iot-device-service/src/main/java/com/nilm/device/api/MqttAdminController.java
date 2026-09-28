package com.nilm.device.api;

import com.nilm.device.service.MqttPasswdSyncService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.io.IOException;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 관리자 전용 — /api/devices/admin/** 경로는 보안 활성화 시 ADMIN 롤 필요 (SecurityConfig).
 */
@Tag(name = "MQTT Admin", description = "Mosquitto passwd 파일 동기화")
@RestController
@RequestMapping("/api/devices/admin/mqtt")
public class MqttAdminController {

    private final MqttPasswdSyncService syncService;

    public MqttAdminController(MqttPasswdSyncService syncService) {
        this.syncService = syncService;
    }

    @Operation(summary = "passwd 파일 동기화",
            description = "DB의 유효 계정(dev_*)을 passwd 파일에 반영. 다른 계정 라인은 보존. "
                    + "app.mqtt.reload-enabled=true면 브로커 SIGHUP까지 수행.")
    @PostMapping("/sync")
    public MqttPasswdSyncService.SyncResult sync() throws IOException {
        return syncService.sync();
    }
}
