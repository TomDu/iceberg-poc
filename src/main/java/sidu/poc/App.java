package sidu.poc;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class App
{
    private static final Logger LOG = LoggerFactory.getLogger(App.class);
    private static final Properties CONFIG = Settings.getConfig();

    public static void main( String[] args )
    {
        LOG.info( "--- START ---" );
        App app = new App();

        for (String arg : args) {
            System.out.println();
            LOG.info("--- {} ---", arg);
            switch (arg) {
                case "create":
                    app.createTable();
                    break;
                case "scan":
                    app.scanTable();
                    break;
                case "updateTable":
                    app.updateTable();
                    break;
                case "drop":
                    app.dropTable();
                    break;
                case "append":
                    app.append();
                    break;
                case "delete":
                    app.delete();
                    break;
                case "addRows":
                    app.rowDeltaAddRows();
                    break;
                case "deleteByPosition":
                    app.deleteByPosition();
                    break;
                case "deleteByEquality":
                    app.deleteByEquality();
                    break;
                case "updateTableProperties":
                    app.updateTableProperties();
                    break;
                default:
                    LOG.info("Unknown command '{}'", arg);
                    break;
            }
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
        tableIdentifier = TableIdentifier.of(nyc, "contacts");
    }

    public void createTable() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "class", Types.StringType.get()),
                Types.NestedField.required(2, "id", Types.IntegerType.get()),
                Types.NestedField.required(3, "name", Types.StringType.get()));
        LOG.info("Schema: {}", schema);

        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity("class")
                .build();
        LOG.info("PartitionSpec: {}", spec);

        Map<String, String> properties = new HashMap<>();
        properties.put(TableProperties.FORMAT_VERSION, "2");

        catalog.createTable(tableIdentifier, schema, spec, properties);
    }

    public void dropTable() {
        catalog.dropTable(tableIdentifier);
    }

    public void scanTable() {
        Table table = catalog.loadTable(tableIdentifier);

        LOG.info("Scan all records:");
        CloseableIterable<Record> result = IcebergGenerics.read(table).build();
        for (Record r: result) {
            LOG.info(r.toString());
        }

        LOG.info("Scan all records where class == A:");
        result = IcebergGenerics.read(table).where(Expressions.equal("class", "A")).build();
        for (Record r: result) {
            LOG.info(r.toString());
        }

        LOG.info("TableScan with class == A:");
        TableScan scan = table.newScan();
        TableScan filteredScan = scan.filter(Expressions.equal("class", "A")).select("name");
        Iterable<CombinedScanTask> result2 = filteredScan.planTasks();
        CombinedScanTask task = result2.iterator().next();
        DataFile dataFile = task.files().iterator().next().file();
        LOG.info(dataFile.toString());
    }

    public void updateTable() {
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
                .withPartitionPath("class=A") // easy way to set partition data for now
                .withRecordCount(1)
                .build();

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
                .withPartitionPath("class=A") // easy way to set partition data for now
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
                .withPartitionPath("class=A") // easy way to set partition data for now
                .withRecordCount(1)
                .build();

        LOG.info("Row delta - deleteByPosition");
        table.newRowDelta().addDeletes(deleteFile).commit();
    }

    public void deleteByEquality() {
        Table table = catalog.loadTable(tableIdentifier);
        PartitionSpec spec = table.spec();

        DeleteFile deleteFile = FileMetadata.deleteFileBuilder(spec)
                .ofEqualityDeletes(1, 2)
                .withPath("/home/iceberg/warehouse/dev/iceberg-poc/sample-equality-delete-file.parquet")
                .withFileSizeInBytes(2048)
                .withPartitionPath("class=A") // easy way to set partition data for now
                .withRecordCount(1)
                .build();

        LOG.info("Row delta - deleteByEquality");
        table.newRowDelta().addDeletes(deleteFile).commit();
    }

    public void updateTableProperties() {
        Table table = catalog.loadTable(tableIdentifier);

        LOG.info("Update table format-version");
        table.updateProperties().set(TableProperties.FORMAT_VERSION, "2").commit();
    }
}
