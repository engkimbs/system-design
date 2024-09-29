package com.engkimbs.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;

@Builder
public record SensorRecord(
        @JsonProperty
        String eqpId,
        @JsonProperty
        String paramIndex,
        @JsonProperty
        String lotId,
        @JsonProperty
        String stepId,
        @JsonProperty
        double sensorValue
) {
}
