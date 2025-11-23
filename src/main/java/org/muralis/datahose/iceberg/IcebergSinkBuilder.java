package org.muralis.datahose.iceberg;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Wraps the code to initialize an Iceberg sink that uses Glue Data Catalog as catalog.
 * Automatically creates tables if they don't exist, or loads existing tables.
 * Based on AWS Managed Flink examples for Flink 1.20.0 and Iceberg 1.9.1
 */
public class IcebergSinkBuilder {

    /**
     * Creates an Iceberg sink builder that writes Avro records to an Iceberg table
     * stored in AWS Glue Data Catalog with data in S3.
     * 
     * If the table doesn't exist, it will be created automatically with the schema derived
     * from the Avro schema and partitioned by the fields specified in partition.fields property.
     * 
     * Accepts both GenericRecord and SpecificRecord (generated Avro classes).
     * 
     * @param icebergProperties Configuration properties containing:
     *   - bucket.prefix: S3 bucket prefix (e.g., "s3://my-bucket/iceberg") - REQUIRED
     *   - catalog.db: Glue database name - REQUIRED
     *   - catalog.table: Table name - REQUIRED
     *   - operation: "append", "upsert", or "overwrite" - REQUIRED
     *   - partition.fields: Comma-separated partition field names (default: "year,month,day,hour")
     *   - upsert.equality.fields: For upsert, comma-separated list of equality fields (optional)
     * @param dataStream The DataStream of Avro records (GenericRecord or SpecificRecord)
     * @param avroSchema The Avro schema
     * @return FlinkSink.Builder configured for Iceberg
     */
    public static <T extends GenericRecord> FlinkSink.Builder createBuilder(Properties icebergProperties, 
                                                  DataStream<T> dataStream, 
                                                  org.apache.avro.Schema avroSchema) {
        // Retrieve configuration from application parameters - all required
        String s3BucketPrefix = Preconditions.checkNotNull(
            icebergProperties.getProperty("bucket.prefix"), 
            "Iceberg S3 bucket prefix not defined");

        String glueDatabase = Preconditions.checkNotNull(
            icebergProperties.getProperty("catalog.db"), 
            "Iceberg Glue database name not defined");
        
        String glueTable = Preconditions.checkNotNull(
            icebergProperties.getProperty("catalog.table"), 
            "Iceberg table name not defined");

        // Iceberg supports Appends, Upserts and Overwrites
        String icebergOperation = Preconditions.checkNotNull(
            icebergProperties.getProperty("operation"), 
            "Iceberg operation not defined");
        Preconditions.checkArgument(
            icebergOperation.equals("append") || icebergOperation.equals("upsert") || icebergOperation.equals("overwrite"), 
            "Invalid Iceberg Operation: " + icebergOperation);

        // If operation is upsert, we need to specify the fields that will be used for equality in the upsert operation
        // If the table is partitioned, we must include the partition fields
        String upsertEqualityFields = icebergProperties.getProperty("upsert.equality.fields", "");
        List<String> equalityFieldsList = upsertEqualityFields.isEmpty() ? 
            Arrays.asList() : 
            Arrays.asList(upsertEqualityFields.split("[, ]+"));

        // Catalog properties for using Glue Data Catalog
        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put("type", "iceberg");
        catalogProperties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        catalogProperties.put("warehouse", s3BucketPrefix);

        // Load Glue Data Catalog
        CatalogLoader glueCatalogLoader = CatalogLoader.custom(
                "glue",
                catalogProperties,
                new org.apache.hadoop.conf.Configuration(),
                "org.apache.iceberg.aws.glue.GlueCatalog");
        
        // Table Object that represents the table in the Glue Data Catalog
        TableIdentifier outputTable = TableIdentifier.of(glueDatabase, glueTable);
        
        // Load the table from the catalog
        TableLoader tableLoader = TableLoader.fromCatalog(glueCatalogLoader, outputTable);
        
        // Load the table to get its schema, or create it if it doesn't exist
        tableLoader.open();
        org.apache.iceberg.Table table;
        
        // Load catalog to check table existence and create if needed
        org.apache.iceberg.catalog.Catalog catalog = glueCatalogLoader.loadCatalog();
        
        if (!catalog.tableExists(outputTable)) {
            // Table doesn't exist - create it from Avro schema
            System.out.println("Table not found, creating new Iceberg table: " + outputTable);
            
            // Convert unshaded Avro schema to shaded Avro schema for Iceberg
            org.apache.iceberg.shaded.org.apache.avro.Schema shadedSchema = 
                new org.apache.iceberg.shaded.org.apache.avro.Schema.Parser().parse(avroSchema.toString());
            
            // Convert shaded Avro schema to Iceberg schema
            org.apache.iceberg.Schema icebergSchema = 
                org.apache.iceberg.avro.AvroSchemaUtil.toIceberg(shadedSchema);
            
            // Extract partition fields from properties (year, month, day, hour)
            String partitionFields = icebergProperties.getProperty("partition.fields", "year,month,day,hour");
            
            // Build partition spec
            org.apache.iceberg.PartitionSpec.Builder partitionSpecBuilder = 
                org.apache.iceberg.PartitionSpec.builderFor(icebergSchema);
            
            for (String partField : partitionFields.split(",")) {
                partField = partField.trim();
                if (!partField.isEmpty()) {
                    partitionSpecBuilder.identity(partField);
                }
            }
            
            org.apache.iceberg.PartitionSpec partitionSpec = partitionSpecBuilder.build();
            Map<String, String> tableProperties = new HashMap<>();
            tableProperties.put("write.format.default", "avro");
            // Create table with schema and partition spec
            org.apache.iceberg.Table icebergTable = catalog.createTable(outputTable, icebergSchema, partitionSpec, tableProperties);
            
            // Upgrade to format version 2 for upsert support (if needed in future)
            org.apache.iceberg.TableOperations tableOperations = 
                ((org.apache.iceberg.BaseTable) icebergTable).operations();
            org.apache.iceberg.TableMetadata currentMetadata = tableOperations.current();
            org.apache.iceberg.TableMetadata upgradedMetadata = currentMetadata.upgradeToFormatVersion(2);
            tableOperations.commit(currentMetadata, upgradedMetadata);
            
            System.out.println("Successfully created Iceberg table with partitioning: " + outputTable);
            table = icebergTable;
        } else {
            // Table exists - load it
            table = tableLoader.loadTable();
            System.out.println("Loaded existing Iceberg table: " + outputTable);
        }
        
        RowType rowType = FlinkSchemaUtil.convert(table.schema());

        // Create a MapFunction that manually converts SpecificRecord to RowData
        // This avoids shaded/unshaded Avro class mismatches
        MapFunction<T, RowData> mapFunction = new MapFunction<T, RowData>() {
            @Override
            public RowData map(T avroRecord) throws Exception {
                try {
                    // Get the Avro schema from the record
                    org.apache.avro.Schema schema = avroRecord.getSchema();
                    List<org.apache.avro.Schema.Field> fields = schema.getFields();
                    
                    // Create GenericRowData with correct field count
                    GenericRowData rowData = new GenericRowData(fields.size());
                    
                    // Map each field by position
                    for (int i = 0; i < fields.size(); i++) {
                        Object value = avroRecord.get(i);
                        
                        if (value == null) {
                            rowData.setField(i, null);
                        } else if (value instanceof CharSequence) {
                            // Convert String/Utf8 to StringData
                            rowData.setField(i, StringData.fromString(value.toString()));
                        } else {
                            // Primitives (int, long, double, etc.) go directly
                            rowData.setField(i, value);
                        }
                    }
                    
                    return rowData;
                } catch (Exception e) {
                    System.err.println("Error converting Avro record to RowData: " + e.getMessage());
                    e.printStackTrace();
                    throw e;
                }
            }
        };

        // Iceberg DataStream sink builder  
        FlinkSink.Builder flinkSinkBuilder = FlinkSink.<T>builderFor(
                        dataStream,
                        mapFunction,
                        FlinkCompatibilityUtil.toTypeInfo(rowType))
                .tableLoader(tableLoader)
                .set("write.format.default", "avro");        

        // Returns the builder for the selected operation
        switch (icebergOperation) {
            case "upsert":
                // If operation is "upsert" we need to set up the equality fields
                return flinkSinkBuilder.equalityFieldColumns(equalityFieldsList).upsert(true);
            case "overwrite":
                return flinkSinkBuilder.overwrite(true);
            default:
                return flinkSinkBuilder; // append is default
        }
    }
}
