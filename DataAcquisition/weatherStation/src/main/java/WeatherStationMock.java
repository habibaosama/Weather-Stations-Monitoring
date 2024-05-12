
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONArray;


import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Random;


public class WeatherStationMock {
    private static final Random RANDOM = new Random();
    private final Properties properties;
    private final String stationId;
    private final String latitude;
    private final String longitude;
    private final static String TOPIC_NAME = "weather-station-topic";

    public WeatherStationMock(String stationId, String latitude, String longitude) {
        this.stationId = stationId;
        this.latitude = latitude;
        this.longitude = longitude;
        properties = new Properties();
        // Load Kafka properties from file
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("kafka-config.properties")) {
            properties.load(inputStream);
            System.out.println("kafka broker -> " + properties.getProperty("bootstrap.servers"));
        } // Set up Kafka producer properties
        catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * Randomly drop messages
     *
     * @return true if the message should be dropped, false otherwise
     */
    private static boolean isDrop() {
        int rand = RANDOM.nextInt(10);
        return rand == 1;
    }

    /**
     * Generate a weather status message and send it to Kafka
     */
    public void produce() throws IOException {
        // Initialize a counter for the number of messages sen
        long s_no = 0;
        long timeStamp = 0L;
        int humidity = 0;
        double temperature = 0.0;
        double windSpeed = 0.0;
        // Create a Kafka producer that uses the configuration of properties
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        MockMessageBuilder message = new MockMessageBuilder(this.stationId);

        // message generation and sending
        while (true) {

            s_no++;
            if (isDrop()) {
                // checks if it's time to update the weather data every 24 iterations
                // to ensure that the data being sent to Kafka is relatively fresh and reflects the latest weather conditions.
                if ((s_no % 24) == 1) {
                    message = new MockMessageBuilder(this.stationId);

                }
                continue;
            }
            message.generateWeatherStatusMessage(s_no, timeStamp, temperature, humidity, windSpeed);

            String value = message.toString();
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, value);
            producer.send(record);
            System.out.println("Sent message: " + value);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.getCause();
            }
            //after the message sending, resets the count and updates the weather data if it's the 24th message.
            if ((s_no % 24) == 1) {
                message = new MockMessageBuilder(this.stationId);
            }
        }
    }


}