import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BaseCentralStation {
    private static final Pattern messagePattern = Pattern.compile("\\{station_id=(\\d+), s_no=(\\d+), battery_status='(\\w+)', status_timestamp=(\\d+), weather=\\{humidity=(\\d+(?:\\.\\d+)?), temperature=(-?\\d+(?:\\.\\d+)?), wind_speed=(\\d+(?:\\.\\d+)?)}}");

    public static HashMap<String, String> parse(String input) {// Parse the input string and return a HashMap
        Matcher matcher = messagePattern.matcher(input);
        HashMap<String, String> weatherStatus = new HashMap<>();
        if (matcher.find()) {
            weatherStatus.put("station_id", matcher.group(1));
            weatherStatus.put("s_no", matcher.group(2));
            weatherStatus.put("battery_status", matcher.group(3));
            weatherStatus.put("status_timestamp", matcher.group(4));
            weatherStatus.put("humidity", matcher.group(5));
            weatherStatus.put("temperature", matcher.group(6));
            weatherStatus.put("wind_speed", matcher.group(7));
        }
        return weatherStatus;
    }

    // Consume messages from Kafka and write them to a Parquet file
    public static void consume() throws Exception {
        // Create a Kafka consumer
        KafkaConsumer kafkaAPI = new KafkaConsumer();
        BitCask bitCask = new BitCask();
        bitCask.open("src/main/java/test");
        // Create a Parquet file writer
        ParquetFileWriter stationParquetFileWriter = new ParquetFileWriter();
        while (true) {
            List<String> records = kafkaAPI.consumeMessages();// Consume messages from Kafka
            System.out.println("Records received: " + records.size());
            for (String record : records) {
                if (messagePattern.matcher(record).matches()) {// Check if the record matches the pattern
                    WeatherMessage weatherStatus = new WeatherMessage(parse(record));// Parse the record
                    bitCask.put(Integer.valueOf(weatherStatus.getStationId()), weatherStatus.toString());// Write the record to the BitCask
                    stationParquetFileWriter.write(weatherStatus);// Write the record to a Parquet file

                }
            }
        }

    }
}
