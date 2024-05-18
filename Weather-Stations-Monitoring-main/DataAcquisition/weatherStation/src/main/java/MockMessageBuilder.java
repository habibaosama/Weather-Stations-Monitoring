import java.util.Random;

public class MockMessageBuilder extends WeatherMessageBuilder {
    private static final Random RANDOM = new Random();

    public MockMessageBuilder(String station_id) {
        super(station_id);
    }

    @Override
    public void generateWeatherStatusMessage(long sNo, long statusTimestamp, Double temperature, int humidity, Double windSpeed) {
        super.s_no = sNo;
        super.status_timestamp = generateRandomTimeStamp();
        super.battery_status = WeatherMessageBuilder.getBatteryStatus();
        super.temperature = generateRandomTemperature();
        super.humidity = generateRandomHumidity();
        super.wind_speed = generateRandomWindSpeed();
    }

    private int generateRandomHumidity() {
        return RANDOM.nextInt(100); // Generate a random integer between 0 and 99 (inclusive)
    }

    private double generateRandomTemperature() {
        return RANDOM.nextInt(180) - 50; // Generate a random integer between -50 and 129 (inclusive)
    }

    private double generateRandomWindSpeed() {
        return RANDOM.nextInt(408); // Generate a random integer between 0 and 407 (inclusive)
    }
    private long generateRandomTimeStamp() {
        return System.currentTimeMillis() / 1000L; // Set timestamp to the current time in seconds
    }

}
