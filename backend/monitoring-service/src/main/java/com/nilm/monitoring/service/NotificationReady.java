package com.nilm.monitoring.service;
public record NotificationReady(Long notificationId, String title, String body) {

    /** 응답을 요구하는 알림의 질문. 응답 "예"는 위험(도움 요청)으로 저장된다. */
    public static final String RESPONSE_QUESTION = "현재 위험하신가요?";
}

