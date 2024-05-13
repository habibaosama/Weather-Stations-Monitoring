import java.io.IOException;
import java.util.Objects;


public class Main {
    /**
     * Main method to run the WeatherStationProducer.
     * Arguments: type, station number, latitude and longitude.
     * type : Mock ,Meteo
     * station number: 1 -> 10.
     * latitude: -90 -> 90.
     * longitude: -180 -> 180.
     *
     * @param args command line arguments
     */
    public static void main(String[] args) throws IOException {
        if ((Objects.equals(args[0], "Meteo") && args.length < 4)) {
            System.out.println("Usage: java Main <type> <station_number> <latitude> <longitude>");
            return;
        }else if((Objects.equals(args[0], "Mock") && args.length < 2)){
            System.out.println("Usage: java Main <type> <station_number>");
            return;
        }

        // Extracting arguments from command line
        String type = args[0];
        int stationNumber = Integer.parseInt(args[1]);
        // Print arguments (optional)
        System.out.println("Type: " + type);
        System.out.println("Station Number: " + stationNumber);
        if((Objects.equals(args[0], "Meteo") )){
            double latitude = Double.parseDouble(args[2]);
            double longitude = Double.parseDouble(args[3]);
            System.out.println("Latitude: " + latitude);
            System.out.println("Longitude: " + longitude);
        }



        StationFactory.generateStation(args);
        //WeatherStationAdapter produce = new WeatherStationAdapter("1", "50", "60");
        //produce.produce();
    }
}