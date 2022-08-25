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
        new App().run();
        LOG.info( "--- END ---" );
    }

    private final JdbcCatalog catalog;

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
    }

    public void run() {
        LOG.info("Catalog name: {}", catalog.name());

        Namespace nyc = Namespace.of("nyc");
        TableIdentifier name = TableIdentifier.of(nyc, "logs");
        Table table = catalog.loadTable(name);

        LOG.info("Scan all records:");
        CloseableIterable<Record> result = IcebergGenerics.read(table).build();
        for (Record r: result) {
            LOG.info(r.toString());
        }

        LOG.info("Scan all records where level == error:");
        result = IcebergGenerics.read(table).where(Expressions.equal("level", "error")).build();
        for (Record r: result) {
            LOG.info(r.toString());
        }

        LOG.info("TableScan with level == info:");
        TableScan scan = table.newScan();
        TableScan filteredScan = scan.filter(Expressions.equal("level", "info")).select("message");
        Iterable<CombinedScanTask> result2 = filteredScan.planTasks();
        CombinedScanTask task = result2.iterator().next();
        DataFile dataFile = task.files().iterator().next().file();
        LOG.info(dataFile.toString());
    }
}
