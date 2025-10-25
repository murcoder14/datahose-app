package org.muralis.datahose;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.connector.file.src.FileSource.forRecordStreamFormat;

/**
 * Streaming Application - reads from a S3 table, processes the datasets and
 * writes them to another S3 table.
 */
public class StreamingApp {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingApp.class);
    public static final String KINESIS_SOURCE = "KinesisSource";
    public static final String AWS_REGION = "aws.region";
    public static final String S3_SOURCE = "S3Source";
    public static final String S3_SINK = "S3Sink";
    public static final String INPUT_BUCKET = "input-bucket";
    public static final String OUTPUT_BUCKET = "output-bucket";
    public static final String TABLE = "table";
    public static final String DEFAULT_INPUT_TABLE = "datafall";
    public static final String DEFAULT_TARGET_TABLE = "datalake";


    /*
     * Main entry point for the streaming application.
     */

    public static void main(String[] args) throws Exception {

        LOG.info("Analyzing a file - reading from S3 and writing to S3");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        // NOTE: Checkpointing is automatically handled by AWS Managed Flink every 60s.

        String region = null;
        String s3InputPath = null;
        String s3OutputPath = null;
        try {
            Map<String, Properties> appProps = KinesisAnalyticsRuntime.getApplicationProperties();
            if (appProps != null) {

                if (appProps.containsKey(KINESIS_SOURCE)) {
                    region = appProps.get(KINESIS_SOURCE).getProperty(AWS_REGION);
                }

                if (appProps.containsKey(S3_SOURCE)) {
                    String inputS3bucket = appProps.get(StreamingApp.S3_SOURCE).getProperty(INPUT_BUCKET);
                    String table = appProps.get(S3_SOURCE).getProperty(TABLE, DEFAULT_INPUT_TABLE);
                    if (StringUtils.isNotBlank(inputS3bucket)) {
                        s3InputPath = "s3://" + inputS3bucket + "/" + table;
                    }
                }

                if (appProps.containsKey(S3_SINK)) {
                    String outputS3bucket = appProps.get(S3_SINK).getProperty(OUTPUT_BUCKET);
                    String table = appProps.get(S3_SINK).getProperty(TABLE, DEFAULT_TARGET_TABLE);
                    if (StringUtils.isNotBlank(outputS3bucket)) {
                        s3OutputPath = "s3://" + outputS3bucket + "/" + table;
                    }
                }

            }
        } catch (IOException e) {
            LOG.warn("Could not load application properties from KinesisAnalyticsRuntime: {}", e.getMessage());
        }

        if (StringUtils.isBlank(region) || StringUtils.isBlank(s3InputPath) || StringUtils.isBlank(s3OutputPath)) {
            throw new RuntimeException(
                    "One or more application properties or environment variable were not set and hence the application cannot run.");
        }

        LOG.info("S3 Input Path: {}", s3InputPath);
        LOG.info("S3 Output Path: {}", s3OutputPath);
        LOG.info("Region: {}", region);

        FileSource<String> fileSource = forRecordStreamFormat(new TextLineInputFormat(), new Path(s3InputPath)).build();
        DataStream<String> sourceRecords = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(),
                "S3-Data-Source");

        // Parse the data to obtain a Tuple of (name,1) for each record
        DataStream<Tuple2<String, Integer>> parsedData = sourceRecords
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String record, Collector<Tuple2<String, Integer>> out) {
                        String[] elements = record.trim().split(",");
                        if (elements.length == 2) {
                            // Emit (name,1) for each record
                            out.collect(new Tuple2<>(elements[0].trim(), 1));
                        }
                    }
                });
        LOG.info("Parsed Data: {}", parsedData);

        // Convert to Table for proper batch aggregation
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table table = tableEnv.fromDataStream(parsedData);

        // Perform SQL aggregation
        Table result = tableEnv.sqlQuery("SELECT f0 as name, SUM(f1) as total FROM " + table + " GROUP BY f0");

        // Convert back to DataStream and map to String
        DataStream<Row> aggregated = tableEnv.toDataStream(result);

        DataStream<String> results = aggregated.map(new MapFunction<Row, String>() {
            @Override
            public String map(Row row) {
                return row.getField(0) + " visited the gym " + row.getField(1) + " times";
            }
        });

        // Your existing FileSink code
        FileSink<String> s3Sink = FileSink
                .forRowFormat(new Path(s3OutputPath), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofSeconds(5))
                                .withInactivityInterval(Duration.ofSeconds(3))
                                .build())
                .build();

        results.sinkTo(s3Sink);
        env.execute("S3 to S3 - Analytical Use Case");
    }

}
