package com.nilm.device.service;

import com.nilm.device.domain.DeviceCredential;
import com.nilm.device.domain.DeviceStatus;
import com.nilm.device.repository.DeviceAclRepository;
import com.nilm.device.repository.DeviceCredentialRepository;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * DB(원본) → Mosquitto passwd 파일(파생물) 동기화.
 *
 * 규칙:
 * - 이 서비스가 관리하는 계정은 "dev_" 접두사 네임스페이스뿐이다.
 *   기존 파일의 다른 계정(simulator_user, 브릿지 등)은 그대로 보존한다.
 * - 동기화 대상: 폐기되지 않은 계정 중 기기 상태가 REGISTERED/ACTIVE인 것.
 *   (SUSPENDED/RETIRED는 파일에서 빠짐 = 브로커 접속 차단)
 * - 원자적 쓰기: 임시 파일에 쓴 뒤 원본 위로 이동.
 */
@Service
public class MqttPasswdSyncService {

    private static final Logger log = LoggerFactory.getLogger(MqttPasswdSyncService.class);
    private static final String MANAGED_PREFIX = "dev_";
    private static final Set<DeviceStatus> SYNC_STATUSES =
            Set.of(DeviceStatus.REGISTERED, DeviceStatus.ACTIVE);

    public record SyncResult(int managedAccounts, int preservedAccounts, String passwdPath,
                             int aclEntries, String aclPath,
                             boolean reloaded, String note) {
    }

    private final DeviceCredentialRepository credentialRepository;
    private final DeviceAclRepository aclRepository;
    private final Path passwdPath;
    private final Path aclPath;
    private final boolean reloadEnabled;
    private final String staticAcl;
    private final List<String> reloadCommand;

    public MqttPasswdSyncService(
            DeviceCredentialRepository credentialRepository,
            DeviceAclRepository aclRepository,
            @Value("${app.mqtt.passwd-path:../../infrastructure/mqtt/config/passwd}") String passwdPath,
            @Value("${app.mqtt.acl-path:../../infrastructure/mqtt/config/acl.device}") String aclPath,
            @Value("${app.mqtt.reload-enabled:false}") boolean reloadEnabled,
            @Value("${app.mqtt.static-acl:}") String staticAcl,
            @Value("${app.mqtt.reload-command:docker kill --signal=HUP mosquitto-broker}") String reloadCommand) {
        this.credentialRepository = credentialRepository;
        this.aclRepository = aclRepository;
        this.passwdPath = Path.of(passwdPath).toAbsolutePath().normalize();
        this.aclPath = Path.of(aclPath).toAbsolutePath().normalize();
        this.reloadEnabled = reloadEnabled;
        this.staticAcl = staticAcl;
        this.reloadCommand = List.of(reloadCommand.split("\\s+"));
    }

    @Transactional(readOnly = true)
    public SyncResult sync() throws IOException {
        // 1. 기존 파일에서 우리 관리 밖 계정 라인 보존
        List<String> preserved = new ArrayList<>();
        if (Files.exists(passwdPath)) {
            for (String line : Files.readAllLines(passwdPath, StandardCharsets.UTF_8)) {
                if (!line.isBlank() && !line.startsWith(MANAGED_PREFIX)) {
                    preserved.add(line);
                }
            }
        }

        // 2. DB에서 동기화 대상 계정 라인 생성
        List<DeviceCredential> targets = credentialRepository.findSyncTargets(SYNC_STATUSES);
        List<String> lines = new ArrayList<>(preserved);
        int managed = 0;
        for (DeviceCredential c : targets) {
            if (c.getMosquittoHash() == null) {
                log.warn("mosquitto_hash 없음 (V3 이전 발급 계정) — 재발급 필요: {}", c.getMqttUsername());
                continue;
            }
            lines.add(c.getMqttUsername() + ":" + c.getMosquittoHash());
            managed++;
        }

        // 3. 원자적 쓰기 (임시 파일 → 이동)
        atomicWrite(passwdPath, lines);
        log.info("passwd 동기화 완료: 관리 계정 {}건, 보존 계정 {}건 -> {}", managed, preserved.size(), passwdPath);

        // 3-1. ACL 파일 생성 (전체 재생성 — 우리 소유 파일이라 보존 대상 없음)
        int aclEntries = writeAclFile();

        // 4. 브로커 리로드 (옵션)
        boolean reloaded = false;
        String note = "reload-disabled: 수동 리로드 필요 -> " + String.join(" ", reloadCommand);
        if (reloadEnabled) {
            note = runReload();
            reloaded = note == null;
            if (reloaded) {
                note = "브로커 SIGHUP 리로드 완료";
            }
        }
        return new SyncResult(managed, preserved.size(), passwdPath.toString(),
                aclEntries, aclPath.toString(), reloaded, note);
    }

    /**
     * device_acl → mosquitto acl_file 형식으로 전체 재생성.
     *
     * <p>mosquitto의 acl_file은 <b>파일에 없는 사용자의 모든 토픽을 막는다.</b>
     * 기기가 아닌 공용 계정(시뮬레이터·브릿지)은 {@code device_acl}에 없으므로
     * 여기서 정적 항목으로 먼저 써 준다. 이게 없으면 ACL을 켜는 순간
     * 수집 파이프라인 전체가 끊긴다.
     */
    private int writeAclFile() throws IOException {
        List<com.nilm.device.repository.AclSyncRow> rows = aclRepository.findSyncTargets(SYNC_STATUSES);
        List<String> lines = new ArrayList<>();
        lines.add("# 이 파일은 iot-device-service가 DB에서 생성한다 — 직접 수정 금지");
        lines.add("# 공용 계정은 app.mqtt.static-acl 설정에서, 기기 계정은 device_acl 테이블에서 온다.");
        int entries = writeStaticAcl(lines);
        String currentUser = null;
        for (var row : rows) {
            if (!row.mqttUsername().equals(currentUser)) {
                currentUser = row.mqttUsername();
                lines.add("");
                lines.add("user " + currentUser);
            }
            String access = row.permission() == com.nilm.device.domain.DeviceAcl.Permission.PUBLISH
                    ? "write" : "read";
            lines.add("topic " + access + " " + row.topicPattern());
            entries++;
        }
        atomicWrite(aclPath, lines);
        log.info("ACL 동기화 완료: {}건 -> {}", entries, aclPath);
        return entries;
    }

    /**
     * 기기가 아닌 공용 계정의 ACL을 쓴다.
     *
     * <p>형식: {@code 사용자명:read|write:토픽} — 쉼표로 여러 개.
     * 예: {@code simulator_user:write:v1/power/sim/#,kafka_bridge_user:read:v1/power/sim/#}
     *
     * <p>DB가 아니라 설정에 두는 이유는 이 계정들이 기기 생명주기를 따르지 않기 때문이다.
     * 기기는 등록·정지·폐기되지만 브릿지는 인프라의 일부다.
     */
    private int writeStaticAcl(List<String> lines) {
        if (staticAcl == null || staticAcl.isBlank()) {
            return 0;
        }
        int entries = 0;
        String currentUser = null;
        for (String raw : staticAcl.split(",")) {
            String rule = raw.trim();
            if (rule.isEmpty()) {
                continue;
            }
            String[] parts = rule.split(":", 3);
            if (parts.length != 3) {
                log.warn("static-acl 항목 형식이 잘못되어 건너뜁니다: {}", rule);
                continue;
            }
            String user = parts[0].trim();
            String access = parts[1].trim();
            String topic = parts[2].trim();
            if (!"read".equals(access) && !"write".equals(access) && !"readwrite".equals(access)) {
                log.warn("static-acl 권한이 read/write/readwrite가 아니어서 건너뜁니다: {}", rule);
                continue;
            }
            if (!user.equals(currentUser)) {
                currentUser = user;
                lines.add("");
                lines.add("# 공용 계정 (설정 주입)");
                lines.add("user " + user);
            }
            lines.add("topic " + access + " " + topic);
            entries++;
        }
        return entries;
    }

    private void atomicWrite(Path target, List<String> lines) throws IOException {
        Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
        Files.write(tmp, lines, StandardCharsets.UTF_8);
        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
    }

    /** 성공 시 null, 실패 시 사유 반환. */
    private String runReload() {
        try {
            Process p = new ProcessBuilder(reloadCommand).redirectErrorStream(true).start();
            if (!p.waitFor(10, TimeUnit.SECONDS)) {
                p.destroyForcibly();
                return "리로드 명령 타임아웃";
            }
            if (p.exitValue() != 0) {
                return "리로드 명령 실패(exit " + p.exitValue() + "): "
                        + new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
            }
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "리로드 중단됨";
        } catch (IOException e) {
            return "리로드 명령 실행 불가: " + e.getMessage();
        }
    }
}
