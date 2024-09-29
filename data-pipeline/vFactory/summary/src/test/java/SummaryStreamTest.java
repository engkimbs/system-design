import com.engkimbs.model.SensorAverageResult;
import com.engkimbs.model.SensorRecord;
import com.engkimbs.serializer.JsonSerde;
import com.engkimbs.streams.SummaryStream;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class SummaryStreamTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, SensorRecord> inputTopic;
    private TestOutputTopic<String, SensorAverageResult> outputTopic;

    // Define Serdes
    private final Serde<String> stringSerde = Serdes.String();
    private final Serde<SensorRecord> sensorRecordSerde = new JsonSerde<>(SensorRecord.class);
    private final Serde<SensorAverageResult> sensorAverageResultSerde = new JsonSerde<>(SensorAverageResult.class);

    @BeforeEach
    void setup() {
        // Define the properties for the test driver
        Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test-sensor-streams-app");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092"); // Dummy for test
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Build the topology using the SensorStreams configuration
        StreamsBuilder builder = new StreamsBuilder();
        SummaryStream summaryStream = new SummaryStream();
        summaryStream.processor(builder);
        Topology topology = builder.build();

        // Initialize the test driver
        testDriver = new TopologyTestDriver(topology, props);

        // Define input and output topic names
        String inputTopicName = "data-collection";
        String outputTopicName = "summary";

        // Create test input and output topics
        inputTopic = testDriver.createInputTopic(
                inputTopicName,
                stringSerde.serializer(),
                sensorRecordSerde.serializer()
        );

        outputTopic = testDriver.createOutputTopic(
                outputTopicName,
                stringSerde.deserializer(),
                sensorAverageResultSerde.deserializer()
        );
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void testAverageEmittedOnStepChange() {
        // Prepare test data
        // First Step: step1 with sensorValues 10.0 and 20.0, paramIndex "param1"
        SensorRecord record1 = new SensorRecord("eqp1", "param1", "lotA", "step1", 10.0);
        SensorRecord record2 = new SensorRecord("eqp1", "param1", "lotA", "step1", 20.0);

        // Step Change to step2 with sensorValues 30.0 and 40.0, paramIndex "param1"
        SensorRecord record3 = new SensorRecord("eqp1", "param1", "lotA", "step2", 30.0);
        SensorRecord record4 = new SensorRecord("eqp1", "param1", "lotA", "step2", 40.0);

        // Step Change to step3 with sensorValue 50.0, paramIndex "param1"
        SensorRecord record5 = new SensorRecord("eqp1", "param1", "lotA", "step3", 50.0);

        // Send first two records (step1)
        inputTopic.pipeInput(record1.paramIndex(), record1);
        inputTopic.pipeInput(record2.paramIndex(), record2);

        // At this point, no step change has occurred yet, so no output should be emitted
        assertThat(outputTopic.isEmpty()).isTrue();

        // Send third record (step2), which is a step change
        inputTopic.pipeInput(record3.paramIndex(), record3);

        // Now, an average for step1 should be emitted: (10 + 20) / 2 = 15.0
        assertThat(outputTopic.isEmpty()).isFalse();
        SensorAverageResult output1 = outputTopic.readValue();
        assertThat(output1.eqpId()).isEqualTo("eqp1");
        assertThat(output1.paramIndex()).isEqualTo("param1");
        assertThat(output1.lotId()).isEqualTo("lotA");
        assertThat(output1.stepId()).isEqualTo("step1");
        assertThat(output1.averageSensorValue()).isEqualTo(15.0);

        // Send fourth record (step2), no step change
        inputTopic.pipeInput(record4.paramIndex(), record4);

        // No new output should be emitted
        assertThat(outputTopic.isEmpty()).isTrue();

        // Send fifth record (step3), which is a step change
        inputTopic.pipeInput(record5.paramIndex(), record5);

        // Now, an average for step2 should be emitted: (30 + 40) / 2 = 35.0
        assertThat(outputTopic.isEmpty()).isFalse();
        SensorAverageResult output2 = outputTopic.readValue();
        assertThat(output2.eqpId()).isEqualTo("eqp1");
        assertThat(output2.paramIndex()).isEqualTo("param1");
        assertThat(output2.lotId()).isEqualTo("lotA");
        assertThat(output2.stepId()).isEqualTo("step2");
        assertThat(output2.averageSensorValue()).isEqualTo(35.0);

        // The last record (step3) may not emit immediately unless another step change occurs
        assertThat(outputTopic.isEmpty()).isTrue();
    }

    @Test
    void testNoStepChangeNoEmission() {
        // Prepare test data with no step changes
        SensorRecord record1 = new SensorRecord("eqp2", "param2", "lotB", "step1", 5.0);
        SensorRecord record2 = new SensorRecord("eqp2", "param2", "lotB", "step1", 15.0);
        SensorRecord record3 = new SensorRecord("eqp2", "param2", "lotB", "step1", 25.0);

        // Send all records with the same stepId and paramIndex "param2"
        inputTopic.pipeInput(record1.paramIndex(), record1);
        inputTopic.pipeInput(record2.paramIndex(), record2);
        inputTopic.pipeInput(record3.paramIndex(), record3);

        // No step change has occurred, so no output should be emitted
        assertThat(outputTopic.isEmpty()).isTrue();
    }

    @Test
    void testMultipleParamIndices() {
        // Prepare test data for multiple paramIndices
        SensorRecord param1Record1 = new SensorRecord("eqp1", "param1", "lotA", "step1", 10.0);
        SensorRecord param1Record2 = new SensorRecord("eqp1", "param1", "lotA", "step1", 20.0);
        SensorRecord param1Record3 = new SensorRecord("eqp1", "param1", "lotA", "step2", 30.0);

        SensorRecord param2Record1 = new SensorRecord("eqp2", "param2", "lotB", "step1", 5.0);
        SensorRecord param2Record2 = new SensorRecord("eqp2", "param2", "lotB", "step2", 15.0);

        // Send records for paramIndex "param1"
        inputTopic.pipeInput(param1Record1.paramIndex(), param1Record1);
        inputTopic.pipeInput(param1Record2.paramIndex(), param1Record2);

        // No emission yet
        assertThat(outputTopic.isEmpty()).isTrue();

        // Send step change for paramIndex "param1"
        inputTopic.pipeInput(param1Record3.paramIndex(), param1Record3);

        // Expect average for param1 step1: (10 + 20) / 2 = 15.0
        assertThat(outputTopic.isEmpty()).isFalse();
        SensorAverageResult output1 = outputTopic.readValue();
        assertThat(output1.eqpId()).isEqualTo("eqp1");
        assertThat(output1.paramIndex()).isEqualTo("param1");
        assertThat(output1.lotId()).isEqualTo("lotA");
        assertThat(output1.stepId()).isEqualTo("step1");
        assertThat(output1.averageSensorValue()).isEqualTo(15.0);

        // Send records for paramIndex "param2"
        SensorRecord param2Record3 = new SensorRecord("eqp2", "param2", "lotB", "step1", 10.0);
        inputTopic.pipeInput(param2Record1.paramIndex(), param2Record1);
        inputTopic.pipeInput(param2Record3.paramIndex(), param2Record3);

        // No emission yet for paramIndex "param2"
        assertThat(outputTopic.isEmpty()).isTrue();

        // Send step change for paramIndex "param2"
        inputTopic.pipeInput(param2Record2.paramIndex(), param2Record2);

        // Expect average for param2 step1: (5 + 10) / 2 = 7.5
        assertThat(outputTopic.isEmpty()).isFalse();
        SensorAverageResult output2 = outputTopic.readValue();
        assertThat(output2.eqpId()).isEqualTo("eqp2");
        assertThat(output2.paramIndex()).isEqualTo("param2");
        assertThat(output2.lotId()).isEqualTo("lotB");
        assertThat(output2.stepId()).isEqualTo("step1");
        assertThat(output2.averageSensorValue()).isEqualTo(7.5);
    }

    @Test
    void testSingleRecordEmissionOnStepChange() {
        // Prepare a single record and then a step change
        SensorRecord record1 = new SensorRecord("eqp3", "param3", "lotC", "step1", 50.0);
        SensorRecord record2 = new SensorRecord("eqp3", "param3", "lotC", "step2", 60.0);

        // Send first record (step1)
        inputTopic.pipeInput(record1.paramIndex(), record1);

        // No emission yet
        assertThat(outputTopic.isEmpty()).isTrue();

        // Send second record (step2), which is a step change
        inputTopic.pipeInput(record2.paramIndex(), record2);

        // Expect average for step1: 50.0
        assertThat(outputTopic.isEmpty()).isFalse();
        SensorAverageResult output1 = outputTopic.readValue();
        assertThat(output1.eqpId()).isEqualTo("eqp3");
        assertThat(output1.lotId()).isEqualTo("lotC");
        assertThat(output1.stepId()).isEqualTo("step1");
        assertThat(output1.averageSensorValue()).isEqualTo(50.0);
    }

    @Test
    void testNoEmissionWithoutStepChange() {
        // Prepare records without step changes
        SensorRecord record1 = new SensorRecord("eqp4", "param4", "lotD", "step1", 100.0);
        SensorRecord record2 = new SensorRecord("eqp4", "param4", "lotD", "step1", 200.0);
        SensorRecord record3 = new SensorRecord("eqp4", "param4", "lotD", "step1", 300.0);

        // Send all records with the same stepId and paramIndex "param4"
        inputTopic.pipeInput(record1.paramIndex(), record1);
        inputTopic.pipeInput(record2.paramIndex(), record2);
        inputTopic.pipeInput(record3.paramIndex(), record3);

        // No emission should occur
        assertThat(outputTopic.isEmpty()).isTrue();
    }
}
