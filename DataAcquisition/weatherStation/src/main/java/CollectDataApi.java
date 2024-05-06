import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Scanner;

public class CollectDataApi {

    private final String apiUrl;

    public CollectDataApi(String latitude, String longitude) {
        this.apiUrl = buildMeteoApiUrl(latitude, longitude);
    }

    private String buildMeteoApiUrl(String latitude, String longitude) {
        return String.format("https://api.open-meteo.com/v1/forecast?latitude=%s&longitude=%s" +
                        "&hourly=relativehumidity_2m,windspeed_80m,temperature_80m&current_weather=true" +
                        "&temperature_unit=fahrenheit&timeformat=unixtime&forecast_days=1&timezone=Africa%%2FCairo",
                latitude, longitude);
    }

    /**
     * Fetches the data from the API.
     *
     * @return String that contains the data from the API.
     */
    private String fetchDataFromOpenMeteo() throws IOException {
        URL url = new URL(apiUrl);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Accept", "application/json");

        StringBuilder responseBody = new StringBuilder();
        try (Scanner scanner = new Scanner(conn.getInputStream())) {
            while (scanner.hasNext()) {
                responseBody.append(scanner.nextLine());
            }
        }
        return responseBody.toString();
    }


    /**
     * Gets the data from the API. And parses it into a Weather object.
     * Gets data hourly.
     *
     * @return Weather object that contains the data from the API.
     */
    public Weather getData() throws IOException {
        String responseBody = this.fetchDataFromOpenMeteo();

        JSONObject jsonObject = new JSONObject(responseBody);
        JSONObject currentWeatherDaily = jsonObject.getJSONObject("hourly");
        ///
        JSONArray timeStamp = currentWeatherDaily.getJSONArray("time");
        ///
        JSONArray relativeHumidity_2m = currentWeatherDaily.getJSONArray("relativehumidity_2m");
        JSONArray windSpeed_80m = currentWeatherDaily.getJSONArray("windspeed_80m");
        JSONArray temperature_2m = currentWeatherDaily.getJSONArray("temperature_80m");

        return new Weather(timeStamp,temperature_2m, relativeHumidity_2m, windSpeed_80m);
    }
}
