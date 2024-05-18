// This is a custom ParquetWriter that allows to write Parquet files with a custom schema.
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;


public class ParquetWriterCustomization extends ParquetWriter<Group> {

    // This class is a customization of the ParquetWriter class that allows to write Parquet files with a custom schema.
    ParquetWriterCustomization(Path file, WriteSupport<Group> writeSupport,
                               CompressionCodecName compressionCodecName,
                               int blockSize, int pageSize, boolean enableDictionary,
                               boolean enableValidation,
                               ParquetProperties.WriterVersion writerVersion,
                               Configuration conf)
            throws IOException {
        super(file, writeSupport, compressionCodecName, blockSize, pageSize,
                pageSize, enableDictionary, enableValidation, writerVersion, conf);
    }



    public static Builder builder(OutputFile file) { // This method returns a new instance of the Builder class
        return new Builder(file);
    }

    public static class Builder extends ParquetWriter.Builder<Group, Builder> { // This class is a builder for the ParquetWriterCustomization class
        private final Map<String, String> extraMetaData = new HashMap<>();
        private MessageType type = null;

        private Builder(OutputFile file) {
            super(file);
        }// This method initializes the Builder class

        public Builder withType(MessageType type) {// This method sets the schema of the Parquet file
            this.type = type;
            return this;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        protected WriteSupport<Group> getWriteSupport(Configuration conf) {
            return new WriteSupportCustomization(type, extraMetaData);
        }

    }

    public static class WriteSupportCustomization extends WriteSupport<Group> {// This class is a customization of the WriteSupport class that allows to write Parquet files with a custom schema

        public static final String PARQUET_EXAMPLE_SCHEMA = "parquet.example.schema";
        private final Map<String, String> extraMetaData;
        private MessageType schema;
        private GroupWriter groupWriter;

        WriteSupportCustomization(MessageType schema, Map<String, String> extraMetaData) {// This method initializes the WriteSupportCustomization class
            this.schema = schema;
            this.extraMetaData = extraMetaData;
        }

        public static MessageType getSchema(Configuration configuration) {// This method returns the schema of the Parquet file
            return parseMessageType(Objects.requireNonNull(configuration.get(PARQUET_EXAMPLE_SCHEMA), PARQUET_EXAMPLE_SCHEMA));
        }

        @Override
        public String getName() {
            return "custom";
        }// This method returns the name of the WriteSupportCustomization class

        @Override
        public WriteContext init(Configuration configuration) { // This method initializes the WriteContext class
            if (schema == null) {
                schema = getSchema(configuration);
            }
            return new WriteContext(schema, this.extraMetaData);
        }

        @Override
        public void prepareForWrite(RecordConsumer recordConsumer) { // This method prepares the WriteSupportCustomization class for writing
            groupWriter = new GroupWriter(recordConsumer, schema);
        }

        @Override
        public void write(Group record) { // This method writes a record to the Parquet file
            groupWriter.write(record);
        }
    }
}