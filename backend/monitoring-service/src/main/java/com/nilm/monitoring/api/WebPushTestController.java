package com.nilm.monitoring.api;
import com.nilm.monitoring.service.WebPushTestService;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.*;
@Profile("local")
@ConditionalOnProperty(name = "app.push.enabled", havingValue = "true")
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/monitoring")
public class WebPushTestController {
    private final WebPushTestService service;
    @PostMapping("/push-test")
    public Map<String, Object> sendTest() { return service.sendTest(); }
}

