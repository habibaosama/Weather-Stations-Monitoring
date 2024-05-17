
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpHost;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.schema.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class ParquetFileWriter {

    private static final int FLUSH_THRESHOLD = 30;// Write records in batches (10000 records/batch) to the parquet to avoid blocking on IO frequently
    private static final HashMap<String, Integer> parquetRecordSize = new HashMap<>();// Keep track of the number of records written to the parquet file
    private static final HashMap<String, Integer> parquetVersion = new HashMap<>();// Keep track of the version of the parquet file
    private static final Map<Path, ParquetWriter<Group>> parquetWriter = new HashMap<>();// Keep track of the parquet writer
    private Long timestamp;
    private HashMap<String, ArrayList<String>> jsonMessages = new HashMap<>();
    private final Configuration hadoopConfig;// Hadoop configuration
    private static final MessageType parquetSchema = createParquetSchema();// Parquet schema

    private static MessageType createParquetSchema() {// Create a Parquet schema
        return Types.buildMessage()
                .addField(createField(PrimitiveType.PrimitiveTypeName.INT64, "station_id"))
                .addField(createField(PrimitiveType.PrimitiveTypeName.INT64, "s_no"))
                .addField(createField(PrimitiveType.PrimitiveTypeName.BINARY, "battery_status", OriginalType.UTF8))
                .addField(createField(PrimitiveType.PrimitiveTypeName.INT64, "status_timestamp"))
                .addField(createField(PrimitiveType.PrimitiveTypeName.INT32, "humidity"))
                .addField(createField(PrimitiveType.PrimitiveTypeName.INT32, "temperature"))
                .addField(createField(PrimitiveType.PrimitiveTypeName.INT32, "wind_speed"))
                .named("WeatherStatus");
    }

    private static Type createField(PrimitiveType.PrimitiveTypeName type, String name) {// Create a field for the Parquet schema
        return Types.required(type).named(name);
    }

    private static Type createField(PrimitiveType.PrimitiveTypeName type, String name, OriginalType originalType) {// Create a field for the Parquet schema
        return Types.required(type).as(originalType).named(name);
    }

    // Initialize the ParquetFileWriter
    public ParquetFileWriter() throws IOException {
        timestamp = System.currentTimeMillis();
        this.hadoopConfig = new Configuration();
        FileSystem parquetFile = FileSystem.get(this.hadoopConfig);
        Path rootDirectory = new Path("ParquetFiles");

        if (!parquetFile.exists(rootDirectory)) {
            parquetFile.mkdirs(rootDirectory);
        }

        for (int i = 1; i <= 10; i++) {// Initialize the parquet version and record size
            parquetVersion.put(String.valueOf(i), 1);
            parquetRecordSize.put(String.valueOf(i), 0);
        }
    }

    // Write a record to the Parquet file
    public void write(WeatherMessage weatherStatus) throws IOException {
        Path parquetPath = createParquetPath(weatherStatus);// Create a path for the Parquet file

        ParquetWriter<Group> writer = getOrCreateWriter(parquetPath);// Get or create a Parquet writer
        writer.write(weatherStatus.grouping(parquetSchema));// Write the record to the Parquet file

        // Get the ArrayList for the station ID or create a new one if it doesn't exist
        ArrayList<String> stationMessages = jsonMessages.getOrDefault(weatherStatus.getStationId(), new ArrayList<>());
        stationMessages.add(weatherStatus.toString());
        jsonMessages.put(weatherStatus.getStationId(), stationMessages);// Add the JSON message to the list

        updateRecordSize(weatherStatus);// Update the record size
        flushIfNeeded(weatherStatus, writer, parquetPath);// Flush the Parquet file if threshold is reached

    }

    // Create a path for the Parquet file
    private Path createParquetPath(WeatherMessage weatherStatus) {
        String currDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        return new Path("ParquetFiles/", currDate + "/Station_" + weatherStatus.getStationId() + "/"
                + "Version_" + parquetVersion.get(weatherStatus.getStationId()) + "_S" + weatherStatus.getStationId() + "_"+ timestamp + ".parquet");
    }

    private Path createJSONPath(WeatherMessage weatherStatus) {
        String currDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        return new Path("JSONFiles/", currDate + "/Station_" + weatherStatus.getStationId() + "/"
                + "Version_" + parquetVersion.get(weatherStatus.getStationId()) + "_S" + weatherStatus.getStationId() + ".json");
    }



    private void writeJSON(ArrayList<String> jsonMessages, Path jsonPath) throws IOException {
        // Convert the list of JSON messages to a single string
        String json = String.join(",", jsonMessages);

        FileSystem fs = FileSystem.get(hadoopConfig);
        if (fs.exists(jsonPath)) {
            // If the file already exists, read the existing content
            try (org.apache.hadoop.fs.FSDataInputStream inputStream = fs.open(jsonPath)) {
                byte[] buffer = new byte[inputStream.available()];
                inputStream.readFully(buffer);
                String existingContent = new String(buffer, StandardCharsets.UTF_8);
                // Append the new content to the existing content
                json = existingContent + "," + json;
            }
        }

        // Write the content to the file
        try (org.apache.hadoop.fs.FSDataOutputStream outputStream = fs.create(jsonPath, true)) {
            outputStream.writeBytes(json);
        }

        // Create an Elasticsearch client
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http")));

        // Index each JSON message in Elasticsearch
        for (String message : jsonMessages) {
            IndexRequest request = new IndexRequest("weather");
            request.source(message, XContentType.JSON);

            // Send the index to Elasticsearch
            client.index(request, RequestOptions.DEFAULT);
        }

        // Close the Elasticsearch client
        client.close();
    }

    // Get or create a Parquet writer
    private ParquetWriter<Group> getOrCreateWriter(Path parquetPath) throws IOException {
        ParquetWriter<Group> writer = parquetWriter.get(parquetPath);// Get the Parquet writer
        if (writer == null) {
            // Create a new Parquet writer
            writer = ParquetWriterCustomization.builder(HadoopOutputFile.fromPath(parquetPath, hadoopConfig))
                    .withType(parquetSchema)
                    .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                    .withWriteMode(org.apache.parquet.hadoop.ParquetFileWriter.Mode.CREATE)
                    .build();
            parquetWriter.put(parquetPath, writer);// Add the Parquet writer to the map
        }
        return writer;
    }

    // Update the record size
    private void updateRecordSize(WeatherMessage weatherStatus) {
        parquetRecordSize.put(weatherStatus.getStationId(), parquetRecordSize.get(weatherStatus.getStationId()) + 1);
    }

    // Flush the Parquet file if threshold is reached
    private void flushIfNeeded(WeatherMessage weatherStatus, ParquetWriter<Group> writer, Path parquetPath) throws IOException {
        Path jsonPath = createJSONPath(weatherStatus);
        if (parquetRecordSize.get(weatherStatus.getStationId()) >= FLUSH_THRESHOLD) {
            System.out.println("Flushing the parquet file");
            writer.close();// Close the Parquet writer
            // Remove the Parquet writer, reset the record size and update the record version
            parquetRecordSize.put(weatherStatus.getStationId(), 0);
            parquetVersion.put(weatherStatus.getStationId(), parquetVersion.get(weatherStatus.getStationId()) + 1);
            parquetWriter.remove(parquetPath);

            // Write JSON messages to file
            ArrayList<String> stationMessages = jsonMessages.get(weatherStatus.getStationId());
            if (stationMessages != null) {
                writeJSON(stationMessages, jsonPath);
                stationMessages.clear(); // Clear the list after writing to file
            }
        }
    }



}
