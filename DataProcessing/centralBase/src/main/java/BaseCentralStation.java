import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BaseCentralStation {
    private static final Pattern messagePattern = Pattern.compile("\\{station_id=(\\d+), s_no=(\\d+), battery_status='(\\w+)', status_timestamp=(\\d+), weather=\\{humidity=(\\d+(?:\\.\\d+)?), temperature=(-?\\d+(?:\\.\\d+)?), wind_speed=(\\d+(?:\\.\\d+)?)}}");    public static HashMap<String, String> parse(String input) {
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

    public static void consume() throws Exception {
        KafkaConsumer kafkaAPI = new KafkaConsumer();

        ParquetFileWriter stationParquetFileWriter = new ParquetFileWriter();
        while (true) {
            List<String> records = kafkaAPI.consumeMessages();
            System.out.println("Records received: " + records.size());
            for (String record : records) {
                if (messagePattern.matcher(record).matches()) {
                    System.out.println("Message Received Successfully & is valid");
                    WeatherMessage weatherStatus = new WeatherMessage(parse(record));
                    stationParquetFileWriter.write(weatherStatus);
                }
            }
        }
    }
}
