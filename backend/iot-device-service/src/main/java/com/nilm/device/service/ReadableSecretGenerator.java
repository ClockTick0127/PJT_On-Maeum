package com.nilm.device.service;

import java.security.SecureRandom;
import org.springframework.stereotype.Component;

/**
 * 사람이 불러줄 수 있는 비밀값 생성기.
 *
 * <p>대상자 초기 비밀번호는 담당자가 어르신에게 <b>구두나 서면으로 전달</b>한다.
 * 난수 그대로면({@code a7Kf9#mQ2x}) 전달 자체가 불가능하므로,
 * 초대 코드와 같은 규칙으로 혼동하기 쉬운 문자를 뺀 대문자·숫자만 쓴다.
 *
 * <p>글자당 경우의 수가 31이므로 12자리면 약 2^59로, 온라인 추측 공격에는 충분하다.
 * 어차피 첫 로그인 후 본인이 바꾸도록 유도한다.
 */
@Component
public class ReadableSecretGenerator {

    /** 0/O, 1/I/L 처럼 듣고 헷갈리는 문자는 뺀다. */
    private static final String ALPHABET = "ABCDEFGHJKMNPQRSTUVWXYZ23456789";
    private static final int DEFAULT_LENGTH = 12;
    /** 네 글자마다 끊어 읽기 쉽게 한다: ABCD-EFGH-JKMN */
    private static final int GROUP_SIZE = 4;

    private final SecureRandom random = new SecureRandom();

    public String generate() {
        return generate(DEFAULT_LENGTH);
    }

    public String generate(int length) {
        StringBuilder sb = new StringBuilder(length + length / GROUP_SIZE);
        for (int i = 0; i < length; i++) {
            if (i > 0 && i % GROUP_SIZE == 0) {
                sb.append('-');
            }
            sb.append(ALPHABET.charAt(random.nextInt(ALPHABET.length())));
        }
        return sb.toString();
    }
}
