import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RainingTriggerKafkaProcessor {
    public final Properties props;

    public RainingTriggerKafkaProcessor() {
        props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "raining-trigger-processor"); // Set the application ID
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092"); // Set the Kafka server
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, org.apache.kafka.common.serialization.Serdes.String().getClass()); // Set the default key serde class
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, org.apache.kafka.common.serialization.Serdes.String().getClass()); // Set the default value serde class
    }

    public void process() {

        StreamsBuilder builder = new StreamsBuilder();

        // Create a stream from the "weather-station-topic" topic
        KStream<String, String> stream = builder.stream("weather-station-topic", Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> rainEvents = stream.filter((key, value) -> { // Filter out (removing) the messages with humidity less than 70
            try {
                Pattern pattern = Pattern.compile("humidity=(\\d+)"); // Create a pattern to match the humidity field
                Matcher matcher = pattern.matcher(value);
                if (matcher.find()) {
                    int humidity = Integer.parseInt(matcher.group(1));
                    System.out.println("Processing value with humidity " + humidity);
                    return humidity > 70; // Check if the humidity is greater than 70
                } else {
                    System.out.println("No humidity found in value " + value);
                    return false;
                }
            } catch (Exception e) {
                System.out.println("Error processing value " + value);
                return false;
            }
        }).mapValues(value -> { // Set the humidity field to "raining"
            try {
                Pattern pattern = Pattern.compile("humidity=(\\d+)");
                Matcher matcher = pattern.matcher(value);
                if (matcher.find()) {
                    return value.replace(matcher.group(0), "humidity='raining'");
                } else {
                    throw new RuntimeException("No humidity found in value " + value);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).peek(
                (key, value) -> System.out.println(value)
        );

        // Send the filtered messages to the "raining-trigger-topic" topic
        rainEvents.to("raining-trigger-topic", org.apache.kafka.streams.kstream.Produced.with(org.apache.kafka.common.serialization.Serdes.String(), org.apache.kafka.common.serialization.Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props); // Start the Kafka Streams application
        streams.start(); // Start the Kafka Streams application

        // Block the main thread to keep the application running
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
