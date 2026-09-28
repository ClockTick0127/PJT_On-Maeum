package com.nilm.device;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
// 담당자 가입 아웃박스를 주기적으로 발행한다.
@EnableScheduling
public class IotDeviceServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(IotDeviceServiceApplication.class, args);
    }
}
