import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Properties;

public class RainingTriggerKafkaProcessor {
    public final Properties props;

    public RainingTriggerKafkaProcessor() {
        props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "raining-trigger-processor"); // Set the application ID
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Set the Kafka server
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, org.apache.kafka.common.serialization.Serdes.String().getClass()); // Set the default key serde class
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, org.apache.kafka.common.serialization.Serdes.String().getClass()); // Set the default value serde class
    }

    public void process() {

        StreamsBuilder builder = new StreamsBuilder();
        // Create a stream from the "weather-station-topic" topic
        KStream<String, String> stream = builder.stream("weather-station-topic", org.apache.kafka.streams.kstream.Consumed.with(org.apache.kafka.common.serialization.Serdes.String(), org.apache.kafka.common.serialization.Serdes.String()));

        ObjectMapper objectMapper = new ObjectMapper();

        stream.filter((key, value) -> { // Filter out (removing) the messages with humidity less than 70
            try {
                JsonNode jsonNode = objectMapper.readTree(value);
                return jsonNode.get("weather").get("humidity").asInt() > 70; // Check if the humidity is greater than 70
            } catch (Exception e) {
                return false;
            }
        }).mapValues(value -> { // Set the humidity field to "raining"
            try {
                // Parse the JSON string to a JsonNode object
                JsonNode jsonNode = objectMapper.readTree(value);
                ((ObjectNode) jsonNode.get("weather")).put("humidity", "raining");
                return objectMapper.writeValueAsString(jsonNode);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }).to("raining-trigger-topic", org.apache.kafka.streams.kstream.Produced.with(org.apache.kafka.common.serialization.Serdes.String(), org.apache.kafka.common.serialization.Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props); // Start the Kafka Streams application
        streams.start(); // Start the Kafka Streams application
    }
}
