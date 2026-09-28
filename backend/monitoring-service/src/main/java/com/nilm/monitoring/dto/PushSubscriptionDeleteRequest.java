package com.nilm.monitoring.dto;


import jakarta.validation.constraints.NotBlank;
import org.hibernate.validator.constraints.URL;

public record PushSubscriptionDeleteRequest (
        @NotBlank @URL(protocol = "https") String endpoint
) {

}
