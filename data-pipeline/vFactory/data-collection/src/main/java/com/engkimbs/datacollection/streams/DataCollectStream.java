package com.engkimbs.datacollection.streams;

import com.engkimbs.datacollection.model.SensorRecord;
import com.engkimbs.datacollection.model.Trace;
import com.engkimbs.datacollection.serializer.JsonSerde;
import com.engkimbs.datacollection.serializer.JsonSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class DataCollectStream {

    @Autowired
    public KStream<String, SensorRecord> processor(StreamsBuilder builder) {
        JsonSerde<Trace> inputSerde = new JsonSerde<>(Trace.class);
        JsonSerde<SensorRecord> outputJsonSerde = new JsonSerde<>(SensorRecord.class);

        KStream<String, Trace> stream = builder.stream("trace", Consumed.with(Serdes.String(), inputSerde));

        KStream<String, SensorRecord> flatMappedStream = stream.flatMap((key, trace) -> {
            String eqpId = trace.eqpId();
            String lotId = trace.lotId();
            String stepId = trace.stepId();
            Map<String, Double> sensorInfo = trace.sensorInfo();

            return sensorInfo.entrySet().stream()
                    .map(sensor -> {
                        SensorRecord sensorRecord = SensorRecord.builder()
                                .eqpId(eqpId)
                                .paramIndex(sensor.getKey())
                                .lotId(lotId)
                                .stepId(stepId)
                                .sensorValue(sensor.getValue())
                                .build();
                        return new KeyValue<>(key, sensorRecord);
                    })
                    .toList();
        });

        flatMappedStream.to("data-collection", Produced.with(Serdes.String(), outputJsonSerde));

        return flatMappedStream;
    }
}
