package com.engkimbs.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;

@Builder
public record SensorAverageResult(
        @JsonProperty
        String paramIndex,
        @JsonProperty
        String eqpId,
        @JsonProperty
        String lotId,
        @JsonProperty
        String stepId,
        @JsonProperty
        double averageSensorValue
) {
}
