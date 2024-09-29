package com.engkimbs.streams;

import com.engkimbs.model.AggregateState;
import com.engkimbs.model.SensorAverageResult;
import com.engkimbs.model.SensorRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

@Component
public class SummaryStream {

    @Autowired
    public KStream<String, SensorAverageResult> processor(
            StreamsBuilder streamsBuilder
    ) {
        Serde<String> stringSerde = Serdes.String();
        Serde<SensorRecord> sensorRecordSerde = new JsonSerde<>(SensorRecord.class);
        Serde<AggregateState> aggregateStateSerde = new JsonSerde<>(AggregateState.class);
        Serde<SensorAverageResult> sensorAverageResultSerde = new JsonSerde<>(SensorAverageResult.class);

        KStream<String, SensorRecord> stream =
                streamsBuilder.stream("data-collection", Consumed.with(stringSerde, sensorRecordSerde));

        KGroupedStream<String, SensorRecord> groupedStream =
                stream.groupByKey(Grouped.with(Serdes.String(), sensorRecordSerde));

        KTable<String , AggregateState> aggregateTable = groupedStream.aggregate(
                AggregateState::new,
                (key, newRecord, aggState) -> {
                    // Detect change in stepId or lotId
                    if (!newRecord.stepId().equals(aggState.processingStepId())) {
                        // Initialization when new key is arrived
                        if (aggState.countOfStepProcess() == 0) {
                            aggState.reset(newRecord);
                            aggState.stepChanged(false);
                        } else {
                            // Reset aggregation with new record
                            aggState.calculateAverageSensorValue();
                            aggState.reset(newRecord);
                            aggState.stepChanged(true);
                        }
                    } else {
                        // Accumulate sensor values
                        aggState.addSensorValue(newRecord.sensorValue());
                        aggState.stepChanged(false);
                    }
                    return aggState;
                },
                Materialized.with(stringSerde, aggregateStateSerde)
        );

        // Convert the KTable to KStream and filter only records where stepChanged is true
        KStream<String, SensorAverageResult> outputStream = aggregateTable.toStream()
                .filter((key, aggState) -> aggState.stepChanged())
                .mapValues((key, aggState) ->
                        new SensorAverageResult(
                            key,
                            aggState.eqpId(),
                            aggState.prevLotId(),
                            aggState.prevStepId(),
                            aggState.averageSensorValue())
                );

        // Write the output stream to the output topic
        outputStream.to("summary", Produced.with(stringSerde, sensorAverageResultSerde));

        return outputStream;
    }
}
