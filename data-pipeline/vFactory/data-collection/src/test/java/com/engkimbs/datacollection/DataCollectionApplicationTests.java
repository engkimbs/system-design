package com.engkimbs.datacollection;

import com.engkimbs.datacollection.model.SensorRecord;
import com.engkimbs.datacollection.model.Trace;
import com.engkimbs.datacollection.serializer.JsonSerde;
import com.engkimbs.datacollection.streams.DataCollectStream;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataCollectionApplicationTests {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Trace> inputTopic;
    private TestOutputTopic<String, SensorRecord> outputTopic;

    private final Serde<Trace> traceJsonSerde = new JsonSerde<>(Trace.class);
    private final Serde<SensorRecord> sensorRecordSerde = new JsonSerde<>(SensorRecord.class);

    @BeforeEach
    public void setup() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-sensor-info-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Build the topology using the SensorStreams configuration
        StreamsBuilder builder = new StreamsBuilder();
        DataCollectStream summaryStream = new DataCollectStream();
        summaryStream.processor(builder);
        Topology topology = builder.build();

        testDriver = new TopologyTestDriver(topology, props);

        inputTopic = testDriver.createInputTopic("trace", Serdes.String().serializer(), traceJsonSerde.serializer());
        outputTopic = testDriver.createOutputTopic("data-collection", Serdes.String().deserializer(), sensorRecordSerde.deserializer());
    }

    @Test
    public void testSensorInfoProcessing() {
        // Given: A Trace record structure
        Map<String, Double> sensorInfo = new HashMap<>();
        sensorInfo.put("1", 0.1);
        sensorInfo.put("2", 0.4);
        sensorInfo.put("5", 12.4);
        sensorInfo.put("6", -2.4);

        Trace trace = new Trace("6IRN3901", "TILIX", "STEP1", sensorInfo);

        // When: The data is processed by the Kafka Stream
        inputTopic.pipeInput(trace);

        // Then: The output topic should receive the flat-mapped data in SensorRecord format
        SensorRecord output1 = outputTopic.readValue();
        SensorRecord output2 = outputTopic.readValue();
        SensorRecord output3 = outputTopic.readValue();
        SensorRecord output4 = outputTopic.readValue();

        // Assert the SensorRecord structure and values
        assertEquals("1", output1.paramIndex());
        assertEquals(0.1, output1.sensorValue());
        assertEquals("6IRN3901", output1.eqpId());
        assertEquals("TILIX", output1.lotId());
        assertEquals("STEP1", output1.stepId());

        assertEquals("2", output2.paramIndex());
        assertEquals(0.4, output2.sensorValue());
        assertEquals("6IRN3901", output2.eqpId());
        assertEquals("TILIX", output2.lotId());
        assertEquals("STEP1", output2.stepId());

        assertEquals("5", output3.paramIndex());
        assertEquals(12.4, output3.sensorValue());
        assertEquals("6IRN3901", output3.eqpId());
        assertEquals("TILIX", output3.lotId());
        assertEquals("STEP1", output3.stepId());

        assertEquals("6", output4.paramIndex());
        assertEquals(-2.4, output4.sensorValue());
        assertEquals("6IRN3901", output4.eqpId());
        assertEquals("TILIX", output4.lotId());
        assertEquals("STEP1", output4.stepId());

        assertTrue(outputTopic.isEmpty());
    }
}