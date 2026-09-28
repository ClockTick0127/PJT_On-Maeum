package com.nilm.monitoring.config.enums;

/**
 * 실시간 분석 서비스(analysis_db)가 사용하는 가전 카테고리 코드.
 * 화면 문구는 서비스마다 다시 만들지 않도록 여기에서만 관리한다.
 */
public enum ApplianceType {

    KETTLE("전기포트"),
    INDUCTION("인덕션"),
    IRON("다리미"),
    MICROWAVE("전자레인지"),
    HAIR_DRYER("헤어드라이어"),
    VACUUM_CLEANER("청소기");

    private final String label;

    ApplianceType(String label) {
        this.label = label;
    }

    public String label() {
        return label;
    }

    /**
     * 분석 서비스가 코드표에 없는 값을 보내더라도 화면이 비지 않도록
     * 알 수 없는 코드는 원본 문자열을 그대로 돌려준다.
     */
    public static String labelOf(String code) {
        if (code == null || code.isBlank()) {
            return "가전";
        }
        try {
            return valueOf(code).label();
        } catch (IllegalArgumentException e) {
            return code;
        }
    }
}
