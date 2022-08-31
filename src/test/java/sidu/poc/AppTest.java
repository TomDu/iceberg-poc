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
                .requiredString("contact_class")
                .requiredInt("contact_id")
                .requiredString("contact_name")
                .endRecord();

        String fileName = "sample-data.parquet";

        List<GenericRecord> records = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            GenericRecord record = new GenericRecordBuilder(schema)
                    .set("contact_class", "A")
                    .set("contact_id", i)
                    .set("contact_name", "Foo" + i)
                    .build();
            records.add(record);
        }

        SimpleParquetWriter.write(schema, fileName, records);
    }

    @Test
    public void testWriteDeleteFileOfPositionDeletes() throws IOException {
        Schema rowSchema = SchemaBuilder
                .builder()
                .record("row")
                .fields()
                .requiredString("contact_class")
                .requiredInt("contact_id")
                .requiredString("contact_name")
                .endRecord();

        Schema schema = SchemaBuilder
                .builder()
                .record("table")
                .fields()
                .requiredString("file_path")
                .requiredLong("pos")
                .name("row")
                    .type(rowSchema)
                    .noDefault()
                .endRecord();

        String fileName = "sample-position-delete-file.parquet";

        GenericRecord rowRecord = new GenericRecordBuilder(rowSchema)
                .set("contact_class", "A")
                .set("contact_id", 8)
                .set("contact_name", "Foo" + 8)
                .build();

        GenericRecord record = new GenericRecordBuilder(schema)
                .set("file_path", "/home/iceberg/warehouse/dev/iceberg-poc/sample-data.parquet")
                .set("pos", 8L)
                .set("row", rowRecord)
                .build();

        List<GenericRecord> records = new ArrayList<>();
        records.add(record);

        SimpleParquetWriter.write(schema, fileName, records);
    }

    @Test
    public void testWriteDeleteFileOfEqualityDeletes() throws IOException {
        Schema schema = SchemaBuilder
                .builder()
                .record("table")
                .fields()
                .requiredString("contact_class")
                .requiredInt("contact_id")
                .endRecord();

        String fileName = "sample-equality-delete-file.parquet";

        List<GenericRecord> records = new ArrayList<>();
        for (int i = 3; i <= 5; ++i) {
            GenericRecord record = new GenericRecordBuilder(schema)
                    .set("contact_class", "A")
                    .set("contact_id", i)
                    .build();
            records.add(record);
        }

        SimpleParquetWriter.write(schema, fileName, records);
    }
}
