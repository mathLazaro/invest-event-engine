package com.github.mathlazaro.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;

@JsonIgnoreProperties(ignoreUnknown = true)
public record Subscriptions(
        @JsonProperty("user_id")
        String userId,

        @JsonProperty("ticker")
        Ticker ticker,

        @JsonProperty("sector")
        Sector sector,

        @JsonProperty("higher_than")
        BigDecimal higherThan,

        @JsonProperty("smaller_than")
        BigDecimal smallerThan
) {

}
