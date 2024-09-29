package com.engkimbs.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Accessors(fluent = true)
@Setter
public class AggregateState {
    @JsonProperty
    String eqpId;
    @JsonProperty
    String prevStepId;
    @JsonProperty
    String prevLotId;
    @JsonProperty
    String processingStepId;
    @JsonProperty
    String processingLotId;
    @JsonProperty
    double sumOfStepIdValueOfStepProcess;
    @JsonProperty
    long countOfStepProcess;
    @JsonProperty
    double averageSensorValue;
    @JsonProperty
    boolean stepChanged;

    // Constructors
    public AggregateState() {
        this.eqpId = "";
        this.prevStepId = "";
        this.prevLotId = "";
        this.processingLotId = "";
        this.processingStepId = "";
        this.sumOfStepIdValueOfStepProcess = 0.0;
        this.countOfStepProcess = 0;
        this.averageSensorValue = 0.0;
        this.stepChanged = false;
    }

    public void addSensorValue(double sensorValue) {
        this.sumOfStepIdValueOfStepProcess += sensorValue;
        this.countOfStepProcess += 1;
    }

    public void reset(SensorRecord newRecord) {
        this.eqpId = newRecord.eqpId();
        this.prevStepId = this.processingStepId;
        this.prevLotId = this.processingLotId;
        this.processingStepId = newRecord.stepId();
        this.processingLotId = newRecord.lotId();
        this.sumOfStepIdValueOfStepProcess = newRecord.sensorValue();
        this.countOfStepProcess = 1;
    }

    public void calculateAverageSensorValue() {
        this.averageSensorValue = (this.sumOfStepIdValueOfStepProcess / this.countOfStepProcess);
    }
}
