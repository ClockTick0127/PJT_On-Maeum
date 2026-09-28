package com.nilm.device.service;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Base64;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class MosquittoPasswordHasherTest {

    private final MosquittoPasswordHasher hasher = new MosquittoPasswordHasher();

    @Test
    @DisplayName("mosquitto passwd 형식($7$1000$salt$hash)을 지킨다")
    void producesMosquittoFormat() {
        String hash = hasher.hash("test1234");
        String[] parts = hash.split("\\$");
        // ["", "7", "1000", salt, hash]
        assertEquals("7", parts[1]);
        assertEquals("1000", parts[2]);
        assertEquals(64, Base64.getDecoder().decode(parts[3]).length, "솔트 64바이트");
        assertEquals(64, Base64.getDecoder().decode(parts[4]).length, "키 64바이트");
    }

    @Test
    @DisplayName("같은 비밀번호라도 솔트가 달라 해시가 매번 다르다")
    void saltIsRandom() {
        assertNotEquals(hasher.hash("same"), hasher.hash("same"));
    }

    @Test
    @DisplayName("해시가 PBKDF2-HMAC-SHA512 계산과 일치한다 (재현 검증)")
    void hashIsReproducibleWithSameSalt() throws Exception {
        String produced = hasher.hash("secret");
        String[] parts = produced.split("\\$");
        byte[] salt = Base64.getDecoder().decode(parts[3]);

        PBEKeySpec spec = new PBEKeySpec("secret".toCharArray(), salt, 1000, 64 * 8);
        byte[] expected = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA512")
                .generateSecret(spec).getEncoded();

        assertArrayEquals(expected, Base64.getDecoder().decode(parts[4]));
        assertTrue(produced.startsWith("$7$1000$"));
    }
}
