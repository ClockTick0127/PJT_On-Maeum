package com.nilm.device.service;

import java.security.SecureRandom;
import java.util.Base64;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import org.springframework.stereotype.Component;

/**
 * Mosquitto passwd 파일 호환 해시 생성기.
 * 기존 파일과 동일한 파라미터를 사용한다: $7$1000$&lt;salt b64&gt;$&lt;hash b64&gt;
 * (PBKDF2-HMAC-SHA512, 반복 1,000회, 솔트·키 64바이트)
 */
@Component
public class MosquittoPasswordHasher {

    private static final int ITERATIONS = 1000;
    private static final int SALT_BYTES = 64;
    private static final int KEY_BYTES = 64;

    private final SecureRandom random = new SecureRandom();

    public String hash(String plainPassword) {
        byte[] salt = new byte[SALT_BYTES];
        random.nextBytes(salt);
        try {
            PBEKeySpec spec = new PBEKeySpec(
                    plainPassword.toCharArray(), salt, ITERATIONS, KEY_BYTES * 8);
            byte[] key = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA512")
                    .generateSecret(spec).getEncoded();
            Base64.Encoder b64 = Base64.getEncoder();
            return "$7$%d$%s$%s".formatted(ITERATIONS, b64.encodeToString(salt), b64.encodeToString(key));
        } catch (Exception e) {
            throw new IllegalStateException("mosquitto 해시 생성 실패", e);
        }
    }
}
