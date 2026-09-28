package com.nilm.device.service;

import com.nilm.device.api.dto.DeviceDtos;
import com.nilm.device.common.NotFoundException;
import com.nilm.device.domain.Device;
import com.nilm.device.domain.DeviceAcl;
import com.nilm.device.domain.DeviceCredential;
import com.nilm.device.domain.DeviceStatus;
import com.nilm.device.domain.InstallHistory;
import com.nilm.device.repository.DeviceAclRepository;
import com.nilm.device.repository.DeviceCredentialRepository;
import com.nilm.device.repository.DeviceRepository;
import com.nilm.device.repository.HouseholdRepository;
import com.nilm.device.repository.InstallHistoryRepository;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional(readOnly = true)
public class DeviceService {

    /** 기기 발행 토픽 규칙 — MQTT 시뮬레이터·브릿지와 동일한 체계 */
    private static final String TOPIC_PATTERN = "v1/power/sim/%s/#";
    /** 접속 상태(LWT) 토픽 — 기기 번호로 좁혀 남의 기기를 위장하지 못하게 한다 */
    private static final String STATUS_TOPIC_PATTERN = "v1/device/%d/status";

    private final DeviceRepository deviceRepository;
    private final HouseholdRepository householdRepository;
    private final DeviceCredentialRepository credentialRepository;
    private final DeviceAclRepository aclRepository;
    private final InstallHistoryRepository historyRepository;
    private final PasswordEncoder passwordEncoder = new BCryptPasswordEncoder();
    private final SecureRandom random = new SecureRandom();
    private final MosquittoPasswordHasher mosquittoHasher;

    public DeviceService(DeviceRepository deviceRepository,
                         HouseholdRepository householdRepository,
                         DeviceCredentialRepository credentialRepository,
                         DeviceAclRepository aclRepository,
                         InstallHistoryRepository historyRepository,
                         MosquittoPasswordHasher mosquittoHasher) {
        this.deviceRepository = deviceRepository;
        this.householdRepository = householdRepository;
        this.credentialRepository = credentialRepository;
        this.aclRepository = aclRepository;
        this.historyRepository = historyRepository;
        this.mosquittoHasher = mosquittoHasher;
    }

    /** 기기 등록 = 기기 저장 + MQTT 계정 발급 + ACL 생성 + 설치 이력 기록 (한 트랜잭션). */
    @Transactional
    public DeviceDtos.RegisterResponse register(DeviceDtos.RegisterRequest request, String changedBy) {
        if (!householdRepository.existsById(request.houseId())) {
            throw new NotFoundException("가구", request.houseId());
        }
        Device device = deviceRepository.saveAndFlush(new Device(
                request.houseId(), request.deviceType(), request.location(), request.firmwareVer()));

        // MQTT 계정: 평문 비밀번호는 이 응답에서 1회만 노출, DB에는 해시만
        String username = "dev_%s_%s%d".formatted(
                request.houseId().toLowerCase(Locale.ROOT),
                request.deviceType().name().toLowerCase(Locale.ROOT),
                device.getDeviceId());
        String plainPassword = generatePassword();
        credentialRepository.save(new DeviceCredential(
                device.getDeviceId(), username,
                passwordEncoder.encode(plainPassword),
                mosquittoHasher.hash(plainPassword)));

        // ACL: 자기 가구 토픽에만 발행 가능
        String topic = TOPIC_PATTERN.formatted(request.houseId());
        aclRepository.save(new DeviceAcl(device.getDeviceId(), topic, DeviceAcl.Permission.PUBLISH));

        // 접속 상태(LWT) 토픽 — 자기 기기 번호의 것만 쓸 수 있다.
        // 남의 기기를 오프라인으로 위장하지 못하게 기기별로 좁힌다.
        String statusTopic = STATUS_TOPIC_PATTERN.formatted(device.getDeviceId());
        aclRepository.save(new DeviceAcl(device.getDeviceId(), statusTopic, DeviceAcl.Permission.PUBLISH));

        historyRepository.save(new InstallHistory(device.getDeviceId(),
                InstallHistory.EventType.INSTALLED, null, request.location(), null, changedBy));

        return new DeviceDtos.RegisterResponse(
                DeviceDtos.Response.from(device), username, plainPassword, List.of(topic, statusTopic));
    }

    public DeviceDtos.Response get(Long deviceId) {
        return DeviceDtos.Response.from(findDevice(deviceId));
    }

    public List<DeviceDtos.Response> listByHouse(String houseId) {
        return deviceRepository.findByHouseId(houseId).stream()
                .map(DeviceDtos.Response::from)
                .toList();
    }

    /** 상태 전이 — 도메인 규칙 위반 시 400, RETIRED 전이 시 MQTT 계정 자동 폐기. */
    @Transactional
    public DeviceDtos.Response changeStatus(Long deviceId, DeviceDtos.StatusChangeRequest request,
                                            String changedBy) {
        Device device = findDevice(deviceId);
        DeviceStatus from = device.transitionTo(request.status());

        InstallHistory.EventType eventType = request.status() == DeviceStatus.RETIRED
                ? InstallHistory.EventType.RETIRED
                : InstallHistory.EventType.STATUS_CHANGED;
        historyRepository.save(new InstallHistory(deviceId, eventType,
                from.name(), request.status().name(), request.reason(), changedBy));

        if (request.status() == DeviceStatus.RETIRED) {
            credentialRepository.findById(deviceId).ifPresent(DeviceCredential::revoke);
        }
        return DeviceDtos.Response.from(device);
    }

    /**
     * MQTT 계정 비밀번호 로테이션 — username 유지, 해시만 교체.
     * RETIRED 기기는 불가. 새 평문은 응답에서 1회만 노출되며,
     * 브로커 반영은 별도의 passwd 동기화(/admin/mqtt/sync)로 수행한다.
     */
    @Transactional
    public DeviceDtos.CredentialRotateResponse rotateCredential(Long deviceId, String changedBy) {
        Device device = findDevice(deviceId);
        if (device.getStatus() == DeviceStatus.RETIRED) {
            throw new com.nilm.device.common.InvalidOperationException(
                    "폐기된 기기의 계정은 로테이션할 수 없습니다: " + deviceId);
        }
        DeviceCredential credential = credentialRepository.findById(deviceId)
                .orElseThrow(() -> new NotFoundException("MQTT 계정", deviceId));

        String plainPassword = generatePassword();
        credential.rotate(passwordEncoder.encode(plainPassword), mosquittoHasher.hash(plainPassword));
        historyRepository.save(new InstallHistory(deviceId,
                InstallHistory.EventType.CREDENTIAL_ROTATED, null, null, "비밀번호 로테이션", changedBy));

        return new DeviceDtos.CredentialRotateResponse(
                deviceId, credential.getMqttUsername(), plainPassword);
    }

    public List<DeviceDtos.HistoryResponse> history(Long deviceId) {
        findDevice(deviceId); // 존재 검증
        return historyRepository.findByDeviceIdOrderByChangedAtDesc(deviceId).stream()
                .map(DeviceDtos.HistoryResponse::from)
                .toList();
    }

    private Device findDevice(Long deviceId) {
        return deviceRepository.findById(deviceId)
                .orElseThrow(() -> new NotFoundException("기기", deviceId));
    }

    private String generatePassword() {
        byte[] bytes = new byte[18];
        random.nextBytes(bytes);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }
}
