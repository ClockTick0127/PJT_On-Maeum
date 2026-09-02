package com.nilm.device.api;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 실행 환경 확인용 최소 API.
 * - GET  /api/devices/ping : 서비스 동작 확인
 * - POST /api/devices/echo : @Valid 검증과 공통 오류 응답 확인
 */
@RestController
@RequestMapping("/api/devices")
public class PingController {

    @GetMapping("/ping")
    public Map<String, String> ping() {
        return Map.of("service", "iot-device-service", "status", "ok");
    }

    @GetMapping("/admin/ping")
    public Map<String, String> adminPing() {
        return Map.of("service", "iot-device-service", "admin", "ok");
    }

    @PostMapping("/echo")
    public Map<String, String> echo(@Valid @RequestBody EchoRequest request) {
        return Map.of("echo", request.message());
    }

    public record EchoRequest(
            @NotBlank(message = "message는 비어 있을 수 없습니다.") String message
    ) {
    }
}
