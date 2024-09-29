package com.engkimbs.datacollection.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;

import java.util.Map;

@Builder
public record Trace(
        @JsonProperty
        String eqpId,
        @JsonProperty
        String lotId,
        @JsonProperty
        String stepId,
        @JsonProperty
        Map<String, Double> sensorInfo) {
}