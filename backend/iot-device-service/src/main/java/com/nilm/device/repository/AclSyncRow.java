package com.nilm.device.repository;

import com.nilm.device.domain.DeviceAcl;

/** ACL 파일 동기화용 프로젝션 — MQTT 계정명과 토픽 권한의 조인 결과 한 줄 */
public record AclSyncRow(String mqttUsername, String topicPattern, DeviceAcl.Permission permission) {
}
