//import message_utils.message_builder.AdapterMessageBuilder;
//import message_utils.message_builder.Builder;
//import message_utils.message_builder.Director;
//import message_utils.message_builder.MockMessageBuilder;
//import station_utils.IDGenerator;
import java.io.IOException;
import java.util.Objects;

public class StationFactory {
    public static void generateStation(String[] parameters) throws IOException {
        String  stationID = parameters[1] ;
        System.out.println("Creating Station with ID: " + stationID);

        if(Objects.equals(parameters[0], "Mock")) {
            System.out.println("Mock Station Created.... :)");
           WeatherStationMock mock = new WeatherStationMock(stationID);
           mock.produce();

        }
        else if(Objects.equals(parameters[0], "Meteo")) {
            System.out.println("Open Meteo Adapter Station Created with station ID.... :)"+ stationID);
            //take arguments from parameters array
            WeatherStationAdapter produce = new WeatherStationAdapter(stationID, parameters[2], parameters[3]);
            produce.produce();

        }

    }
}