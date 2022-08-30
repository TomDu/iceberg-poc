package sidu.poc;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

public class SimpleParquetWriter {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleParquetWriter.class);

    public static void write(Schema schema, String filePath, Collection<GenericRecord> records) throws IOException {
        try (ParquetWriter<GenericRecord> parquetWriter =
                     AvroParquetWriter.<GenericRecord>builder(new Path(filePath))
                             .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                             .withSchema(schema)
                             .build()) {

            for (GenericRecord record : records) {
                parquetWriter.write(record);
            }

            LOG.info("Write records to file {} done.", filePath);
        }
    }
}
