package com.nilm.monitoring.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;
import org.hibernate.validator.constraints.URL;

public record PushSubscriptionRequest(

        @NotBlank
        @URL(protocol = "https")
        String endpoint,

        @PositiveOrZero
        Long expirationTime,

        @NotNull
        @Valid
        Keys keys

) {

    public record Keys(
            @NotBlank String p256dh,
            @NotBlank String auth
    ) {
    }
}