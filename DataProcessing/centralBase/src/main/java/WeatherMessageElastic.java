import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.schema.MessageType;

import java.io.Serializable;
import java.util.HashMap;

@Data
public class WeatherMessage implements Serializable {
    private String stationId;
    private String sNo;
    private String batteryStatus;
    private String statusTimestamp;
    private String humidity;
    private String temperature;
    private String windSpeed;

    // Initialize the WeatherMessage
    public WeatherMessage(HashMap<String, String> msg) {
        this.stationId = msg.get("station_id");
        this.sNo = msg.get("s_no");
        this.batteryStatus = msg.get("battery_status");
        this.statusTimestamp = msg.get("status_timestamp");
        this.humidity = msg.get("humidity");
        this.temperature = msg.get("temperature");
        this.windSpeed = msg.get("wind_speed");
    }

    // Group the weather message
    public Group grouping(MessageType schema) {
        GroupFactory groupFactory = new SimpleGroupFactory(schema); // Create a group factory
        Group weatherStatusGroup = groupFactory.newGroup();

        weatherStatusGroup.add("station_id", Long.parseLong(stationId));
        weatherStatusGroup.add("s_no", Long.parseLong(sNo));
        weatherStatusGroup.add("battery_status", batteryStatus);
        weatherStatusGroup.add("status_timestamp", Long.parseLong(statusTimestamp));

        Group weatherGroup = weatherStatusGroup.addGroup("weather");
        weatherGroup.add("humidity", (int) Float.parseFloat(humidity));
        weatherGroup.add("temperature", (int) Float.parseFloat(temperature));
        weatherGroup.add("wind_speed", (int) Float.parseFloat(windSpeed));

        return weatherStatusGroup;
    }

    // Convert the weather message to a string
    @Override
    public String toString() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(JsonGenerator.Feature.WRITE_NUMBERS_AS_STRINGS, false);
        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return "{}";
        }
    }
}