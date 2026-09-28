package com.nilm.device.service;

import com.nilm.device.domain.Device;
import jakarta.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * 기기 접속 상태 수신 — MQTT LWT(Last Will and Testament).
 *
 * <p>안부 확인 시스템에서 <b>데이터가 끊긴 것 자체가 신호</b>인데, 그 원인이 정전인지
 * 네트워크 장애인지 사람이 활동을 안 하는 것인지 구분하지 못하면
 * Wi-Fi가 끊길 때마다 보호자에게 위험 알림이 가는 오경보 시스템이 된다.
 *
 * <p>LWT는 기기가 접속할 때 "내가 끊기면 이 메시지를 대신 보내 달라"고 브로커에
 * 맡겨 두는 기능이다. 기기가 스스로 알릴 수 없는 상황에서도 브로커가 알려 준다.
 *
 * <h2>기기 쪽 규약</h2>
 * <pre>
 * 접속 시  LWT 등록: topic v1/device/{deviceId}/status, payload "offline", retain=true
 * 접속 후  발행:     topic v1/device/{deviceId}/status, payload "online",  retain=true
 * </pre>
 * retain을 쓰는 이유는 이 서비스가 나중에 붙어도 <b>마지막 상태를 즉시 받기</b> 위해서다.
 *
 * <p>브로커에 붙지 못해도 서비스는 정상 기동한다. 접속 상태는 부가 정보이고,
 * 이것 때문에 기기 등록·인증이 막히면 안 된다.
 */
@Component
@ConditionalOnProperty(name = "app.mqtt.connection-listener.enabled", havingValue = "true")
public class DeviceConnectionListener {

    private static final Logger log = LoggerFactory.getLogger(DeviceConnectionListener.class);

    /** v1/device/{deviceId}/status 에서 기기 번호만 뽑는다 */
    private static final Pattern TOPIC = Pattern.compile("^v1/device/(\\d+)/status$");
    private static final String ONLINE = "online";
    private static final String OFFLINE = "offline";

    private final DeviceConnectionUpdater updater;
    private final String brokerUrl;
    private final String username;
    private final String password;
    private final String topicFilter;

    private MqttClient client;

    public DeviceConnectionListener(
            DeviceConnectionUpdater updater,
            @Value("${app.mqtt.broker-url:tcp://localhost:1883}") String brokerUrl,
            @Value("${app.mqtt.connection-listener.username:}") String username,
            @Value("${app.mqtt.connection-listener.password:}") String password,
            @Value("${app.mqtt.connection-listener.topic:v1/device/+/status}") String topicFilter) {
        this.updater = updater;
        this.brokerUrl = brokerUrl;
        this.username = username;
        this.password = password;
        this.topicFilter = topicFilter;
    }

    /**
     * 기동이 끝난 뒤 붙는다. 브로커가 없어도 서비스는 살아 있어야 하므로
     * 실패는 경고로만 남긴다.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void connect() {
        try {
            client = new MqttClient(brokerUrl, "iot-device-service-conn", new MemoryPersistence());
            MqttConnectionOptions options = new MqttConnectionOptions();
            options.setAutomaticReconnect(true);
            options.setCleanStart(false);
            if (!username.isBlank()) {
                options.setUserName(username);
                options.setPassword(password.getBytes(StandardCharsets.UTF_8));
            }
            client.setCallback(new StatusCallback());
            client.connect(options);
            // QoS 1 — 접속·종료 통지는 놓치면 상태가 어긋난 채 남는다
            client.subscribe(topicFilter, 1);
            log.info("기기 접속 상태 구독 시작: {} ({})", topicFilter, brokerUrl);
        } catch (Exception e) {
            log.warn("MQTT 접속 상태 구독 실패 — 기기 온·오프라인 추적 없이 계속합니다: {}", e.getMessage());
        }
    }

    @PreDestroy
    public void disconnect() {
        if (client == null) {
            return;
        }
        try {
            client.disconnect();
            client.close();
        } catch (Exception e) {
            log.debug("MQTT 종료 중 무시 가능한 오류: {}", e.getMessage());
        }
    }

    /**
     * 토픽·페이로드를 해석해 반영을 위임한다.
     * 알 수 없는 기기나 형식은 조용히 버리지 않고 로그로 남긴다 —
     * 토픽 규약이 어긋나면 그 사실이 드러나야 한다.
     *
     * <p>DB 반영을 {@link DeviceConnectionUpdater}에 맡기는 이유는 여기서 직접 하면
     * 자기 호출이 되어 트랜잭션이 열리지 않기 때문이다.
     */
    public void apply(String topic, String payload) {
        Matcher m = TOPIC.matcher(topic);
        if (!m.matches()) {
            log.warn("규약에 맞지 않는 상태 토픽: {}", topic);
            return;
        }
        Device.ConnectionStatus next = switch (payload.trim().toLowerCase()) {
            case ONLINE -> Device.ConnectionStatus.ONLINE;
            case OFFLINE -> Device.ConnectionStatus.OFFLINE;
            default -> null;
        };
        if (next == null) {
            log.warn("알 수 없는 상태 값: topic={}, payload={}", topic, payload);
            return;
        }
        updater.apply(Long.parseLong(m.group(1)), next);
    }

    private class StatusCallback implements org.eclipse.paho.mqttv5.client.MqttCallback {
        @Override
        public void messageArrived(String topic, MqttMessage message) {
            try {
                apply(topic, new String(message.getPayload(), StandardCharsets.UTF_8));
            } catch (Exception e) {
                // 한 건의 실패가 구독 자체를 끊지 않도록 막는다
                log.error("접속 상태 처리 실패: topic={}", topic, e);
            }
        }

        @Override
        public void disconnected(org.eclipse.paho.mqttv5.client.MqttDisconnectResponse response) {
            log.warn("브로커 연결 끊김 — 자동 재연결 시도: {}", response.getReasonString());
        }

        @Override
        public void mqttErrorOccurred(org.eclipse.paho.mqttv5.common.MqttException exception) {
            log.warn("MQTT 오류: {}", exception.getMessage());
        }

        @Override
        public void deliveryComplete(org.eclipse.paho.mqttv5.client.IMqttToken token) {
            // 수신 전용이라 발행 완료 통지는 쓰지 않는다
        }

        @Override
        public void connectComplete(boolean reconnect, String serverUri) {
            if (reconnect) {
                try {
                    client.subscribe(topicFilter, 1);
                    log.info("재연결 후 구독 복구: {}", topicFilter);
                } catch (Exception e) {
                    log.error("재연결 후 구독 실패", e);
                }
            }
        }

        @Override
        public void authPacketArrived(int reasonCode, org.eclipse.paho.mqttv5.common.packet.MqttProperties properties) {
            // 확장 인증을 쓰지 않는다
        }
    }
}
