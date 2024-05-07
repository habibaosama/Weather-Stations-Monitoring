import java.io.IOException;

public class Main {
    /**
     * Main method to run the WeatherStationProducer.
     * Arguments: station number, latitude and longitude.
     * station number: 1 -> 10.
     * latitude: -90 -> 90.
     * longitude: -180 -> 180.
     *
     * @param args command line arguments
     */
    public static void main(String[] args) throws IOException {
        // i will take station type
        //System.out.println(args[0] + " " + args[1] + " " + args[2]);

        //String [] ar = {"Mock","1","52","13"};
        String [] ar = {"meteo","1","52","13"};
        StationFactory.generateStation(ar);
        //WeatherStationAdapter produce = new WeatherStationAdapter("1", "50", "60");
        //produce.produce();
    }
}