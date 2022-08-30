package sidu.poc;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    @Test
    public void testWriteDatafile() throws IOException {
        Schema schema = SchemaBuilder
                .builder()
                .record("table")
                .fields()
                .requiredString("class")
                .requiredInt("id")
                .requiredString("name")
                .endRecord();

        String fileName = "sample-data.parquet";

        List<GenericRecord> records = new ArrayList<>();
        for (int i = 0; i < 3; ++i) {
            GenericRecord record = new GenericRecordBuilder(schema)
                    .set("class", "A")
                    .set("id", i)
                    .set("name", "Foo" + i)
                    .build();
            records.add(record);
        }

        SimpleParquetWriter.write(schema, fileName, records);
    }

    @Test
    public void testWriteDeleteFileOfPositionDeletes() throws IOException {
        Schema schema = SchemaBuilder
                .builder()
                .record("table")
                .fields()
                .requiredString("class")
                .requiredString("file_path")
                .requiredLong("pos")
                .endRecord();

        String fileName = "sample-position-delete-file.parquet";

        List<GenericRecord> records = new ArrayList<>();
        for (int i = 0; i < 1; ++i) {
            GenericRecord record = new GenericRecordBuilder(schema)
                    .set("class", "A")
                    .set("file_path", "/home/iceberg/warehouse/dev/iceberg-poc/sample-data.parquet")
                    .set("pos", 0L)
                    .build();
            records.add(record);
        }

        SimpleParquetWriter.write(schema, fileName, records);
    }

    @Test
    public void testWriteDeleteFileOfEqualityDeletes() throws IOException {
        Schema schema = SchemaBuilder
                .builder()
                .record("table")
                .fields()
                .requiredString("class")
                .requiredInt("id")
                .endRecord();

        String fileName = "sample-equality-delete-file.parquet";

        List<GenericRecord> records = new ArrayList<>();
        for (int i = 0; i < 1; ++i) {
            GenericRecord record = new GenericRecordBuilder(schema)
                    .set("class", "A")
                    .set("id", 0)
                    .build();
            records.add(record);
        }

        SimpleParquetWriter.write(schema, fileName, records);
    }
}
