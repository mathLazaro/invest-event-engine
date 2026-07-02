package com.github.mathlazaro.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;

import static java.util.Objects.nonNull;

@JsonIgnoreProperties(ignoreUnknown = true)
public record Investments(
        @JsonProperty("ticker")
        Ticker ticker,

        @JsonProperty("price")
        BigDecimal price,

        @JsonProperty("sector")
        Sector sector,

        @JsonProperty("timestamp")
        String timestamp
) {

    public boolean isValid() {

        return nonNull(ticker) && nonNull(price) && nonNull(sector);
    }

}
