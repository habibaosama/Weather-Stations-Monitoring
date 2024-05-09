
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.schema.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ParquetFileWriter {

    private static final int FLUSH_THRESHOLD = 10000;// Write records in batches (10000 records/batch) to the parquet to avoid blocking on IO frequently
    private static final HashMap<String, Integer> parquetRecordSize = new HashMap<>();// Keep track of the number of records written to the parquet file
    private static final HashMap<String, Integer> parquetVersion = new HashMap<>();// Keep track of the version of the parquet file
    private static final Map<Path, ParquetWriter<Group>> parquetWriter = new HashMap<>();// Keep track of the parquet writer
    private static final Long timestamp = System.currentTimeMillis();
    private final Configuration hadoopConfig;// Hadoop configuration
    private static final MessageType parquetSchema = createParquetSchema();// Parquet schema

    private static MessageType createParquetSchema() {// Create a Parquet schema
        return Types.buildMessage()
                .addField(createField(PrimitiveType.PrimitiveTypeName.INT64, "station_id"))
                .addField(createField(PrimitiveType.PrimitiveTypeName.INT64, "s_no"))
                .addField(createField(PrimitiveType.PrimitiveTypeName.BINARY, "battery_status"))
                .addField(createField(PrimitiveType.PrimitiveTypeName.INT64, "status_timestamp"))
                .addField(createField(PrimitiveType.PrimitiveTypeName.INT32, "humidity"))
                .addField(createField(PrimitiveType.PrimitiveTypeName.INT32, "temperature"))
                .addField(createField(PrimitiveType.PrimitiveTypeName.INT32, "wind_speed"))
                .named("WeatherStatus");
    }

    private static Type createField(PrimitiveType.PrimitiveTypeName type, String name) {// Create a field for the Parquet schema
        return Types.required(type).named(name);
    }

    // Initialize the ParquetFileWriter
    public ParquetFileWriter() throws IOException {
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

        updateRecordSize(weatherStatus);// Update the record size
        flushIfNeeded(weatherStatus, writer, parquetPath);// Flush the Parquet file if threshold is reached
    }

    // Create a path for the Parquet file
    private Path createParquetPath(WeatherMessage weatherStatus) {
        return new Path("ParquetFiles/", "Station_" + weatherStatus.getStationId() + "/"
                + "Version_" + parquetVersion.get(weatherStatus.getStationId()) + "_" + timestamp + ".parquet");
    }

    // Get or create a Parquet writer
    private ParquetWriter<Group> getOrCreateWriter(Path parquetPath) throws IOException {
        ParquetWriter<Group> writer = parquetWriter.get(parquetPath);// Get the Parquet writer
        if (writer == null) {
            // Create a new Parquet writer
            writer = ParquetWriterCustomization.builder(HadoopOutputFile.fromPath(parquetPath, hadoopConfig))
                    .withType(parquetSchema)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
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
        if (parquetRecordSize.get(weatherStatus.getStationId()) >= FLUSH_THRESHOLD) {
            System.out.println("Flushing the parquet file");
            writer.close();// Close the Parquet writer

            // Remove the Parquet writer, reset the record size and update the record version
            parquetRecordSize.put(weatherStatus.getStationId(), 0);
            parquetVersion.put(weatherStatus.getStationId(), parquetVersion.get(weatherStatus.getStationId()) + 1);
            parquetWriter.remove(parquetPath);
        }
    }


}
