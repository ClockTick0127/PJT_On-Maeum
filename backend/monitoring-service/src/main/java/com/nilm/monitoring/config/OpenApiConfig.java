package com.nilm.monitoring.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI openAPI() {
        return new OpenAPI().info(new Info()
                .title("Monitoring Service API")
                .description("""
                        가구 상태·이벤트·대응 이력 관리 서비스.

                        위험 등급의 최종 주체는 모니터링이다. 화면에 나가는 riskLevel은
                        모니터링 자체 평가 등급과 분석 서비스 이벤트가 세운 등급 중 높은 쪽이며,
                        어느 쪽에서 나왔는지는 riskSource(ASSESSMENT/EVENT/NONE)로 구분한다.

                        assessmentStatus는 자체 평가가 성립했는지를 뜻한다.
                        VALID가 아니면 riskLevel은 마지막으로 성립한 평가의 값이고,
                        평가 불가를 0점·정상으로 바꿔 보여주지 않는다.
                        confidence는 그 평가의 신뢰도이며 점수에 곱한 값이 아니다.
                        """)
                .version("v0.0.1"));
    }
}
