package org.muralis.datahose;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.guava31.com.google.common.collect.Maps;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

// For AWS Managed Flink runtime property loading
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;

/**
 * Streaming Application - reads from Kinesis Data Stream, transforms to uppercase, and writes to S3
 */
public class StreamingApp {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingApp.class);
    
    // S3 output path will be determined dynamically
    private static String getS3OutputPath() {
        // 1. Try application properties (AWS Managed Flink)
        try {
            Map<String, Properties> appProps = KinesisAnalyticsRuntime.getApplicationProperties();
            if (appProps != null && appProps.containsKey("S3Sink")) {
                Properties s3Props = appProps.get("S3Sink");
                String bucket = s3Props.getProperty("bucket");
                String table = s3Props.getProperty("table", "datafall");
                if (bucket != null && !bucket.isEmpty()) {
                    return "s3://" + bucket + "/" + table;
                }
            }
        } catch (Exception e) {
            LOG.warn("Could not load S3 sink properties from KinesisAnalyticsRuntime: {}", e.getMessage());
        }
        // 2. Try environment variable
        String bucket = System.getenv("DATA_BUCKET");
        String table = System.getenv("S3_TABLE");
        if (bucket != null && !bucket.isEmpty()) {
            if (table == null || table.isEmpty()) table = "datafall";
            return "s3://" + bucket + "/" + table;
        }
        throw new RuntimeException("S3 output bucket not found in application properties or environment variable DATA_BUCKET");
    }

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Flink Streaming Application - MINIMAL TEST VERSION");
        
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // NOTE: Checkpointing is handled by AWS Managed Flink automatically
        LOG.info("Environment configured");
        
        // --- DYNAMIC KINESIS STREAM ARN LOADING ---
        String streamArn = null;
        String region = "us-east-2";
        try {
            Map<String, Properties> appProps = KinesisAnalyticsRuntime.getApplicationProperties();
            if (appProps != null && appProps.containsKey("KinesisSource")) {
                Properties kinesisProps = appProps.get("KinesisSource");
                streamArn = kinesisProps.getProperty("stream.arn");
                region = kinesisProps.getProperty("aws.region", region);
                LOG.info("Loaded Kinesis Stream ARN from application properties: {}", streamArn);
            }
        } catch (Exception e) {
            LOG.warn("Could not load application properties from KinesisAnalyticsRuntime: {}", e.getMessage());
        }
        // Fallback: try environment variable
        if (streamArn == null || streamArn.isEmpty()) {
            streamArn = System.getenv("KINESIS_STREAM_ARN");
            if (streamArn != null && !streamArn.isEmpty()) {
                LOG.info("Loaded Kinesis Stream ARN from environment: {}", streamArn);
            } else {
                throw new RuntimeException("Kinesis Stream ARN not found in application properties or environment variable KINESIS_STREAM_ARN");
            }
        }
    String s3OutputPath = getS3OutputPath();
    LOG.info("S3 Output Path: {}", s3OutputPath);

        // Create properties for Kinesis source configuration
        Properties kinesisProperties = new Properties();
        kinesisProperties.setProperty("stream.arn", streamArn);
        kinesisProperties.setProperty("aws.region", region);

        // Create Kinesis Data Streams source with proper configuration
        KinesisStreamsSource<String> kinesisSource = KinesisStreamsSource.<String>builder()
                .setStreamArn(streamArn)
                .setSourceConfig(Configuration.fromMap(Maps.fromProperties(kinesisProperties)))
                .setDeserializationSchema(new SimpleStringSchema())
                .build();

        // Read from Kinesis Data Stream with explicit TypeInformation
        DataStream<String> inputStream = env.fromSource(
            kinesisSource,
            WatermarkStrategy.noWatermarks(),
            "Kinesis Data Stream Source",
            TypeInformation.of(String.class)
        );
        
        // Transform data to uppercase
        DataStream<String> transformedStream = inputStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value.toUpperCase();
            }
        }).name("Transform to Uppercase");
        

        // Configure S3 sink with rolling policy
        FileSink<String> s3Sink = FileSink
            .forRowFormat(
                new Path(s3OutputPath),
                new SimpleStringEncoder<String>("UTF-8")
            )
            .withRollingPolicy(
                DefaultRollingPolicy.builder()
                    .withRolloverInterval(Duration.ofMinutes(2))
                    .withInactivityInterval(Duration.ofSeconds(30))
                    .build()
            )
            .build();

        transformedStream.sinkTo(s3Sink).name("S3 File Sink");
        env.execute("Kinesis to S3 Uppercase Transformation");
    }
}