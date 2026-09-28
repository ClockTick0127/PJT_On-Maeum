package com.nilm.monitoring.config;

import java.security.GeneralSecurityException;
import java.security.Security;
import nl.martijndwars.webpush.PushService;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

@Configuration
@ConditionalOnProperty(name = "app.push.enabled", havingValue = "true")
public class WebPushConfig {

    @Bean
    public PushService pushService(
            @Value("${app.push.vapid-public-key}") String publicKey,
            @Value("${app.push.vapid-private-key}") String privateKey,
            @Value("${app.push.vapid-subject}") String subject
    ) throws GeneralSecurityException {

        if (Security.getProvider("BC") == null) {
            Security.addProvider(new BouncyCastleProvider());
        }

        return new PushService(publicKey, privateKey, subject);
    }
}
