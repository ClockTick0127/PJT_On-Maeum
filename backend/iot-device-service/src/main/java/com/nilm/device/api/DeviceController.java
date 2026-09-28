package com.nilm.device.api;

import com.nilm.device.api.dto.DeviceDtos;
import com.nilm.device.security.CurrentUser;
import com.nilm.device.service.DeviceService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@Tag(name = "Device", description = "기기 등록·조회·생명주기 관리")
@RestController
@RequestMapping("/api/devices")
public class DeviceController {

    private final DeviceService deviceService;
    private final CurrentUser currentUser;

    public DeviceController(DeviceService deviceService, CurrentUser currentUser) {
        this.deviceService = deviceService;
        this.currentUser = currentUser;
    }

    @Operation(summary = "기기 등록",
            description = "기기 저장 + MQTT 계정 발급 + ACL 생성. mqttPassword는 이 응답에서 1회만 노출된다.")
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public DeviceDtos.RegisterResponse register(@Valid @RequestBody DeviceDtos.RegisterRequest request) {
        return deviceService.register(request, currentUser.id().toString());
    }

    @Operation(summary = "가구별 기기 목록 조회")
    @GetMapping
    public List<DeviceDtos.Response> listByHouse(@RequestParam String houseId) {
        return deviceService.listByHouse(houseId);
    }

    @Operation(summary = "기기 단건 조회")
    @GetMapping("/{deviceId}")
    public DeviceDtos.Response get(@PathVariable Long deviceId) {
        return deviceService.get(deviceId);
    }

    @Operation(summary = "기기 상태 전이",
            description = "허용 전이: REGISTERED→ACTIVE, ACTIVE↔SUSPENDED, (any)→RETIRED. 위반 시 400.")
    @PatchMapping("/{deviceId}/status")
    public DeviceDtos.Response changeStatus(@PathVariable Long deviceId,
                                            @Valid @RequestBody DeviceDtos.StatusChangeRequest request) {
        return deviceService.changeStatus(deviceId, request, currentUser.id().toString());
    }

    @Operation(summary = "MQTT 계정 비밀번호 로테이션",
            description = "username 유지, 비밀번호만 재발급. 새 비밀번호는 이 응답에서 1회만 노출. "
                    + "브로커 반영은 /api/devices/admin/mqtt/sync 호출 필요. RETIRED 기기는 400.")
    @PostMapping("/{deviceId}/credentials/rotate")
    public DeviceDtos.CredentialRotateResponse rotateCredential(@PathVariable Long deviceId) {
        return deviceService.rotateCredential(deviceId, currentUser.id().toString());
    }

    @Operation(summary = "기기 이력 조회")
    @GetMapping("/{deviceId}/history")
    public List<DeviceDtos.HistoryResponse> history(@PathVariable Long deviceId) {
        return deviceService.history(deviceId);
    }
}
