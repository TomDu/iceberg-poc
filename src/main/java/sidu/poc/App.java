package sidu.poc;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class App
{
    private static final Logger LOG = LoggerFactory.getLogger(App.class);
    private static final Properties CONFIG = Settings.getConfig();

    public static void main( String[] args ) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        LOG.info( "--- START ---" );
        App app = new App();

        for (String arg : args) {
            System.out.println();
            LOG.info("--- {} ---", arg);
            Method method = App.class.getMethod(arg);
            method.invoke(app);
        }

        LOG.info( "--- END ---" );
    }

    private final JdbcCatalog catalog;
    private final TableIdentifier tableIdentifier;

    public App() {
        Map<String, String> properties = new HashMap<>();

        properties.put(CatalogProperties.CATALOG_IMPL, JdbcCatalog.class.getName());
        properties.put(CatalogProperties.URI, CONFIG.getProperty("jdbcCatalogConnString"));
        properties.put(JdbcCatalog.PROPERTY_PREFIX + "user", CONFIG.getProperty("jdbcCatalogUser"));
        properties.put(JdbcCatalog.PROPERTY_PREFIX + "password", CONFIG.getProperty("jdbcCatalogPassword"));
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, CONFIG.getProperty("warehouseLocation"));
        properties.put(CatalogProperties.FILE_IO_IMPL, HadoopFileIO.class.getName());

        Configuration conf = new Configuration();

        catalog = new JdbcCatalog();
        catalog.setConf(conf);
        catalog.initialize("demo", properties);
        LOG.info("Catalog name: {}", catalog.name());

        Namespace nyc = Namespace.of("main");
        tableIdentifier = TableIdentifier.of(nyc, CONFIG.getProperty("tableName"));
    }

    public void create() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "contact_class", Types.StringType.get()),
                Types.NestedField.required(2, "contact_id", Types.IntegerType.get()),
                Types.NestedField.required(3, "contact_name", Types.StringType.get()));
        LOG.info("Schema: {}", schema);

        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity("contact_class")
                .build();
        LOG.info("PartitionSpec: {}", spec);

        Map<String, String> properties = new HashMap<>();
        properties.put(TableProperties.FORMAT_VERSION, "2");

        catalog.createTable(tableIdentifier, schema, spec, properties);
    }

    public void drop() {
        catalog.dropTable(tableIdentifier);
    }

    public void scan() {
        Table table = catalog.loadTable(tableIdentifier);

        LOG.info("Scan all records:");
        CloseableIterable<Record> result = IcebergGenerics.read(table).build();
        for (Record r: result) {
            LOG.info("Path: {}; Position: {}; Content: {}", r.getField(MetadataColumns.FILE_PATH.name()), r.getField(MetadataColumns.ROW_POSITION.name()), r);
        }

        LOG.info("Scan all records where contact_id == 9:");
        result = IcebergGenerics.read(table).where(Expressions.equal("contact_id", 9)).build();
        for (Record r: result) {
            LOG.info("Path: {}; Position: {}; Content: {}", r.getField(MetadataColumns.FILE_PATH.name()), r.getField(MetadataColumns.ROW_POSITION.name()), r);
        }

        LOG.info("TableScan with contact_class == A:");
        TableScan scan = table.newScan();
        TableScan filteredScan = scan.filter(Expressions.equal("contact_class", "A")).select("name");
        Iterable<CombinedScanTask> result2 = filteredScan.planTasks();
        CombinedScanTask task = result2.iterator().next();
        DataFile dataFile = task.files().iterator().next().file();
        LOG.info(dataFile.toString());
    }

    public void updateSchema() {
        Table table = catalog.loadTable(tableIdentifier);

        LOG.info("Update schema - add column");
        table.updateSchema().addColumn("count", Types.LongType.get()).commit();
        LOG.info("Schema: {}", table.schema());

        LOG.info("Update schema - remove column");
        table.updateSchema().deleteColumn("count").commit();
        LOG.info("Schema: {}", table.schema());
    }

    public void append() {
        Table table = catalog.loadTable(tableIdentifier);
        PartitionSpec spec = table.spec();

        DataFile dataFile = DataFiles.builder(spec)
                .withPath("/home/iceberg/warehouse/dev/iceberg-poc/sample-data.parquet")
                .withFileSizeInBytes(2048)
                .withPartitionPath("contact_class=A") // easy way to set partition data for now
                .withRecordCount(1)
                .build();

        LOG.info("Append data file");
        table.newAppend().appendFile(dataFile).commit();
    }

    public void append2() throws IOException {
        Table table = catalog.loadTable(tableIdentifier);
        Schema schema = table.schema();
        PartitionSpec partitionSpec = table.spec();

        GenericAppenderFactory genericAppenderFactory = new GenericAppenderFactory(schema, partitionSpec);

        OutputFileFactory outputFileFactory =
                OutputFileFactory.builderFor(table, 998, 999).format(FileFormat.AVRO).build();

        PartitionKey partitionKey = new PartitionKey(partitionSpec, schema);
        partitionKey.set(0, "A");
        EncryptedOutputFile outputFile = outputFileFactory.newOutputFile();
        DataWriter<Record> dataWriter = genericAppenderFactory.newDataWriter(outputFile, FileFormat.AVRO, partitionKey);

        try (DataWriter<Record> closeableWriter = dataWriter) {
            GenericRecord record = GenericRecord.create(schema);
            for (int i = 0; i < 10; ++i) {
                closeableWriter.write(record.copy(ImmutableMap.of("contact_class", "A", "contact_id", i, "contact_name", "Bar" + i)));
            }
        }

        DataFile dataFile = dataWriter.toDataFile();
        LOG.info("Append data file");
        table.newAppend().appendFile(dataFile).commit();
    }

    public void delete() {
        Table table = catalog.loadTable(tableIdentifier);
        LOG.info("Delete data file");
        table.newDelete().deleteFile("/home/iceberg/warehouse/dev/iceberg-poc/sample-data.parquet").commit();
    }

    public void rowDeltaAddRows() {
        Table table = catalog.loadTable(tableIdentifier);
        PartitionSpec spec = table.spec();

        DataFile dataFile = DataFiles.builder(spec)
                .withPath("/home/iceberg/warehouse/dev/iceberg-poc/sample-data.parquet")
                .withFileSizeInBytes(2048)
                .withPartitionPath("contact_class=A") // easy way to set partition data for now
                .withRecordCount(1)
                .build();

        LOG.info("Row delta");
        table.newRowDelta().addRows(dataFile).commit();
    }

    public void deleteByPosition() {
        Table table = catalog.loadTable(tableIdentifier);
        PartitionSpec spec = table.spec();

        DeleteFile deleteFile = FileMetadata.deleteFileBuilder(spec)
                .ofPositionDeletes()
                .withPath("/home/iceberg/warehouse/dev/iceberg-poc/sample-position-delete-file.parquet")
                .withFileSizeInBytes(2048)
                .withPartitionPath("contact_class=A") // easy way to set partition data for now
                .withRecordCount(1)
                .build();

        LOG.info("Row delta - deleteByPosition");
        table.newRowDelta().addDeletes(deleteFile).commit();
    }

    public void deleteByPosition2() throws IOException {
        Table table = catalog.loadTable(tableIdentifier);
        Schema schema = table.schema();
        PartitionSpec partitionSpec = table.spec();

        // Schema pathPosSchema = DeleteSchemaUtil.pathPosSchema();
        GenericAppenderFactory genericAppenderFactory = new GenericAppenderFactory(schema, partitionSpec);

        OutputFileFactory outputFileFactory =
                OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PARQUET).build();

        PartitionKey partitionKey = new PartitionKey(partitionSpec, schema);
        partitionKey.set(0, "A");
        EncryptedOutputFile outputFile = outputFileFactory.newOutputFile();
        PositionDeleteWriter<Record> positionDeleteWriter =
                genericAppenderFactory.newPosDeleteWriter(outputFile, FileFormat.PARQUET, partitionKey);

        try (PositionDeleteWriter<Record> closeableWriter = positionDeleteWriter) {
            closeableWriter.write(PositionDelete.<Record>create().set("/home/iceberg/warehouse/dev/iceberg-poc/sample-data.parquet", 0L, null));
            closeableWriter.write(PositionDelete.<Record>create().set("/home/iceberg/warehouse/dev/iceberg-poc/sample-data.parquet", 1L, null));
            closeableWriter.write(PositionDelete.<Record>create().set("/home/iceberg/warehouse/dev/iceberg-poc/sample-data.parquet", 2L, null));
        }

        DeleteFile deleteFile = positionDeleteWriter.toDeleteFile();
        LOG.info("Row delta - deleteByPosition2");
        table.newRowDelta().addDeletes(deleteFile).commit();
    }

    public void deleteByEquality() {
        Table table = catalog.loadTable(tableIdentifier);
        PartitionSpec spec = table.spec();

        DeleteFile deleteFile = FileMetadata.deleteFileBuilder(spec)
                .ofEqualityDeletes(1, 2)
                .withPath("/home/iceberg/warehouse/dev/iceberg-poc/sample-equality-delete-file.parquet")
                .withFileSizeInBytes(2048)
                .withPartitionPath("contact_class=A") // easy way to set partition data for now
                .withRecordCount(3)
                .build();

        LOG.info("Row delta - deleteByEquality");
        table.newRowDelta().addDeletes(deleteFile).commit();
    }

    public void deleteByEquality2() throws IOException {
        Table table = catalog.loadTable(tableIdentifier);
        Schema schema = table.schema();
        PartitionSpec partitionSpec = table.spec();

        Schema eqDeleteRowSchema = schema.select("contact_class", "contact_id");
        GenericAppenderFactory genericAppenderFactory = new GenericAppenderFactory(
                schema, partitionSpec, new int[] { 1, 2 }, eqDeleteRowSchema, null);

        OutputFileFactory outputFileFactory =
                OutputFileFactory.builderFor(table, 10000, 10086).format(FileFormat.PARQUET).build();

        PartitionKey partitionKey = new PartitionKey(partitionSpec, schema);
        partitionKey.set(0, "A");
        EncryptedOutputFile outputFile = outputFileFactory.newOutputFile();
        EqualityDeleteWriter<Record> equalityDeleteWriter =
                genericAppenderFactory.newEqDeleteWriter(outputFile, FileFormat.PARQUET, partitionKey);

        try (EqualityDeleteWriter<Record> closeableWriter = equalityDeleteWriter) {
            GenericRecord record = GenericRecord.create(eqDeleteRowSchema);
            for (int i = 4; i <= 6; ++i) {
                closeableWriter.write(record.copy(ImmutableMap.of("contact_class", "A", "contact_id", i)));
            }
        }

        DeleteFile deleteFile = equalityDeleteWriter.toDeleteFile();
        LOG.info("Row delta - deleteByEquality2");
        table.newRowDelta().addDeletes(deleteFile).commit();
    }

    public void showSchema() {
        Table table = catalog.loadTable(tableIdentifier);
        LOG.info("Schema: {}", table.schema());
    }

    public void updateTableProperties() {
        Table table = catalog.loadTable(tableIdentifier);

        LOG.info("Update table format-version");
        table.updateProperties().set(TableProperties.FORMAT_VERSION, "2").commit();
    }
}
