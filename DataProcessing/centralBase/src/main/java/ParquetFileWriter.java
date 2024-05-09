
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

    private static final int FLUSH_THRESHOLD = 10000;
    private static final HashMap<String, Integer> parquetRecordSize = new HashMap<>();
    private static final HashMap<String, Integer> parquetVersion = new HashMap<>();
    private static final Map<Path, ParquetWriter<Group>> parquetWriter = new HashMap<>();
    private static final Long timestamp = System.currentTimeMillis();
    private final Configuration configuration;
    private static final MessageType parquetSchema = createParquetSchema();

    private static MessageType createParquetSchema() {
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

    private static Type createField(PrimitiveType.PrimitiveTypeName type, String name) {
        return Types.required(type).named(name);
    }

    public ParquetFileWriter() throws IOException {
        this.configuration = new Configuration();
        FileSystem parquetFile = FileSystem.get(this.configuration);
        Path rootDirectory = new Path("ParquetFiles");

        if (!parquetFile.exists(rootDirectory)) {
            parquetFile.mkdirs(rootDirectory);
        }

        for (int i = 1; i <= 10; i++) {
            parquetVersion.put(String.valueOf(i), 1);
            parquetRecordSize.put(String.valueOf(i), 0);
        }
    }

    public void write(WeatherMessage weatherStatus) throws IOException {
        Path parquetPath = createParquetPath(weatherStatus);

        ParquetWriter<Group> writer = getOrCreateWriter(parquetPath);
        writer.write(weatherStatus.grouping(parquetSchema));

        updateRecordSize(weatherStatus);
        flushIfNeeded(weatherStatus, writer, parquetPath);
    }

    private Path createParquetPath(WeatherMessage weatherStatus) {
        return new Path("ParquetFiles/", "Station_" + weatherStatus.getStationId() + "/"
                + "Version_" + parquetVersion.get(weatherStatus.getStationId()) + "_" + timestamp + ".parquet");
    }

    private ParquetWriter<Group> getOrCreateWriter(Path parquetPath) throws IOException {
        ParquetWriter<Group> writer = parquetWriter.get(parquetPath);
        if (writer == null) {
            writer = ParquetWriterCustomization.builder(HadoopOutputFile.fromPath(parquetPath, configuration))
                    .withType(parquetSchema)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withWriteMode(org.apache.parquet.hadoop.ParquetFileWriter.Mode.CREATE)
                    .build();
            parquetWriter.put(parquetPath, writer);
        }
        return writer;
    }

    private void updateRecordSize(WeatherMessage weatherStatus) {
        parquetRecordSize.put(weatherStatus.getStationId(), parquetRecordSize.get(weatherStatus.getStationId()) + 1);
    }

    private void flushIfNeeded(WeatherMessage weatherStatus, ParquetWriter<Group> writer, Path parquetPath) throws IOException {
        if (parquetRecordSize.get(weatherStatus.getStationId()) >= FLUSH_THRESHOLD) {
            System.out.println("Flushing the parquet file");
            writer.close();
            parquetRecordSize.put(weatherStatus.getStationId(), 0);
            parquetVersion.put(weatherStatus.getStationId(), parquetVersion.get(weatherStatus.getStationId()) + 1);
            parquetWriter.remove(parquetPath);
        }
    }


}
