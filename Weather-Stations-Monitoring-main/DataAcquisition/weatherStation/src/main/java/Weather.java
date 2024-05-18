import lombok.Getter;
import lombok.Setter;
import org.json.JSONArray;

@Getter
@Setter
public class Weather {
    private final JSONArray timeStamp;
    private final JSONArray temperature;
    private final JSONArray humidity;
    private final JSONArray windSpeed;

    public Weather(JSONArray timeStamp,JSONArray temperature, JSONArray humidity, JSONArray windSpeed) {
        this.timeStamp =timeStamp;
        this.temperature = temperature;
        this.humidity = humidity;
        this.windSpeed = windSpeed;
    }


}
