

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class KafkaConsumer {

    private final Properties props;

    public KafkaConsumer() {// This method initializes the Kafka consumer
        props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "weather-station-consumer-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }

    // Consume messages from Kafka
    public List<String> consumeMessages() {
        Consumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
        List<String> message = new ArrayList<>();// Initialize a list to store messages

        String topic = "weather-station-topic";
        consumer.subscribe(Collections.singletonList(topic));// Subscribe to the topic

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));// Poll for messages

        for (ConsumerRecord<String, String> record : records) {
            System.out.println("Message received: " + record.value());
            message.add(record.value());// Add the message to the list
        }

        consumer.close();// Close the consumer
        return message;
    }
}
