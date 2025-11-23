package org.muralis.datahose;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.muralis.datahose.avro.Claim;
import org.muralis.datahose.avro.LeaveRequest;
import org.muralis.datahose.dto.KinesisMessage;
import org.muralis.datahose.processors.ClaimsProcessor;
import org.muralis.datahose.processors.LeaveProcessor;
import org.muralis.datahose.processors.MessageRouter;
import org.muralis.datahose.iceberg.IcebergSinkBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Multi-Pipeline Streaming Application using Kinesis Data Stream and Side Outputs.
 * Reads messages from Kinesis, routes them to appropriate processing pipelines (Claims and Leave Requests),
 * and writes results to Iceberg tables in the data lake. Unknown messages are logged to S3 for investigation.
 */
public class StreamingApp {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingApp.class);
    
    // Kinesis configuration property group and keys
    public static final String KINESIS_SOURCE = "KinesisSource";
    public static final String AWS_REGION = "aws.region";
    public static final String KINESIS_STREAM_ARN = "stream.arn";
    
    // S3 configuration property group and keys (for unknown messages)
    public static final String S3_SINK = "s3sink";  
    public static final String DEFAULT_OUTPUT_BUCKET = "default-output-bucket";
    
    // Iceberg configuration property groups (for claims and leave requests)
    public static final String ICEBERG_CLAIMS = "IcebergClaims";
    public static final String ICEBERG_LEAVE = "IcebergLeave";

    public static void main(String[] args) throws Exception {

        LOG.info("Starting Multi-Pipeline Streaming Application with Kinesis and Side Outputs");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Enable checkpointing for Iceberg commits (every 30 seconds)
        env.enableCheckpointing(30000); // 30 seconds
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(15000); // 15 seconds minimum pause
        env.getCheckpointConfig().setCheckpointTimeout(600000); // 10 minutes timeout
        LOG.info("Checkpointing enabled with 30-second interval");

        String region = "us-east-2";
        String kinesisStreamArn = null;
        String s3DefaultOutputBucket = null;
        
        Properties icebergClaimsProps = new Properties();
        Properties icebergLeaveProps = new Properties();
        
        try {
            Map<String, Properties> appProps = KinesisAnalyticsRuntime.getApplicationProperties();
            if (appProps != null) {

                if (appProps.containsKey(KINESIS_SOURCE)) {
                    region = appProps.get(KINESIS_SOURCE).getProperty(AWS_REGION, region);
                    kinesisStreamArn = appProps.get(KINESIS_SOURCE).getProperty(KINESIS_STREAM_ARN);
                }
                
                if (appProps.containsKey(S3_SINK)) {
                    s3DefaultOutputBucket = appProps.get(S3_SINK).getProperty(DEFAULT_OUTPUT_BUCKET);
                }
                
                // Load Iceberg configuration
                if (appProps.containsKey(ICEBERG_CLAIMS)) {
                    icebergClaimsProps = appProps.get(ICEBERG_CLAIMS);
                }
                if (appProps.containsKey(ICEBERG_LEAVE)) {
                    icebergLeaveProps = appProps.get(ICEBERG_LEAVE);
                }
            }
        } catch (IOException e) {
            LOG.error("Failed to load application properties: {}", e.getMessage(), e);
            throw new IllegalConfigurationException("Failed to load application properties. Please verify the runtime configuration.", e);
        }

        // Validate required configuration - terminate gracefully if missing
        validateRequiredConfiguration(kinesisStreamArn, KINESIS_STREAM_ARN,"Kinesis Stream ARN (" + KINESIS_SOURCE + "." + KINESIS_STREAM_ARN + ")");
        validateRequiredConfiguration(s3DefaultOutputBucket, DEFAULT_OUTPUT_BUCKET,"S3 Default-Output-Bucket (" + S3_SINK + "." + DEFAULT_OUTPUT_BUCKET + ")");

        LOG.info("=== Configuration Validation Passed ===");
        LOG.info("Region: {}", region);
        LOG.info("Kinesis Stream ARN: {}", kinesisStreamArn);
        LOG.info("S3 Default Output Bucket: {}", s3DefaultOutputBucket);
        LOG.info("Claims and Leave Requests: Using Iceberg warehouse");
        LOG.info("========================================");

        // Configure Kinesis Source with stream ARN from properties
        Properties kinesisSourceProperties = new Properties();
        kinesisSourceProperties.setProperty("aws.region", region);
        kinesisSourceProperties.setProperty("stream.arn", kinesisStreamArn);

        // Build KinesisStreamsSource using stream ARN
        KinesisStreamsSource<String> kinesisSource = KinesisStreamsSource.<String>builder()
                .setStreamArn(kinesisStreamArn)
                .setSourceConfig(Configuration.fromMap(createConfigMap(kinesisSourceProperties)))
                .setDeserializationSchema(new SimpleStringSchema())
                .build();

        // Create DataStream from the new Source API with WatermarkStrategy
        DataStream<String> kinesisStream = env.fromSource(kinesisSource,WatermarkStrategy.noWatermarks(),"Kinesis-Source",TypeInformation.of(String.class))
                .uid("kinesis-source-operator");

        // Route messages using Side Outputs
        SingleOutputStreamOperator<KinesisMessage> mainStream = kinesisStream.process(new MessageRouter())
                .uid("message-router-operator");

        // Extract side output streams for each message type
        DataStream<KinesisMessage> claimsStream = mainStream.getSideOutput(MessageRouter.CLAIMS_TAG);
        DataStream<KinesisMessage> leaveStream = mainStream.getSideOutput(MessageRouter.LEAVE_TAG);
        DataStream<KinesisMessage> unknownStream = mainStream.getSideOutput(MessageRouter.UNKNOWN_TAG);

        // Set up processing pipelines for Claims and Leave Requests using Iceberg
        configureClaimsPipeline(claimsStream, icebergClaimsProps);
        configureLeavePipeline(leaveStream, icebergLeaveProps);

        // UNKNOWN MESSAGES: Log to separate location for investigation
        DataStream<String> unknownOutputs = unknownStream.map(msg -> "UNKNOWN: " + msg.toString())
                .name("Format-Unknown")
                .uid("format-unknown-operator");
        FileSink<String> unknownSink = FileSink
                .forRowFormat(new Path("s3://" + s3DefaultOutputBucket + "/unknown/"), 
                        new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();
        unknownOutputs.sinkTo(unknownSink)
                .name("Unknown-S3-Sink")
                .uid("unknown-s3-sink-operator");

        LOG.info("Starting execution of multi-pipeline streaming application");
        env.execute("Kinesis Multi-Pipeline Application with Side Outputs");
    }

    /**
     * Validates that a required configuration parameter is not null or empty.
     * Throws IllegalConfigurationException if validation fails.
     *
     * @param value The configuration value to validate
     * @param parameterName The name of the parameter for error messaging
     * @param description Human-readable description of the parameter
     * @throws IllegalConfigurationException if the value is null or empty
     */
    private static void validateRequiredConfiguration(String value, String parameterName, String description) {
        if (value == null || value.trim().isEmpty()) {
            String errorMessage = String.format("Required configuration parameter missing: %s.The application cannot start without this parameter. Please verify your runtime configuration.",
                description);
            LOG.error(errorMessage);
            throw new IllegalConfigurationException(errorMessage);
        }
    }

    /**
     * Creates a configuration map from the given properties.
     *
     * @param properties The properties to convert to a map
     * @return A Map containing the properties as String key-value pairs
     */
    private static Map<String, String> createConfigMap(Properties properties) {
        Map<String, String> configMap = new HashMap<>();
        properties.forEach((key, value) -> configMap.put(key.toString(), value.toString()));
        return configMap;
    }

    /**
     * Sets up the claims processing pipeline with Iceberg output.
     *
     * @param claimsStream       The input stream of Kinesis messages for claims
     * @param icebergProperties Properties for Iceberg configuration
     */
    private static void configureClaimsPipeline(DataStream<KinesisMessage> claimsStream, Properties icebergProperties) {
        // Convert Kinesis messages to Claim objects
        DataStream<Claim> claimResults = claimsStream.flatMap(new ClaimsProcessor())
                .name("Claims-Processor")
                .uid("claims-processor-operator");
        
        // Build and attach Iceberg sink - append() returns DataStreamSink
        // Pass Claim objects directly (SpecificRecord extends GenericRecord)
        IcebergSinkBuilder
                .createBuilder(icebergProperties, claimResults, Claim.getClassSchema())
                .append()
                .name("Iceberg-Claims-Sink")
                .uid("iceberg-claims-sink-operator");
    }

    /**
     * Sets up the leave of absence processing pipeline with Iceberg output.
     *
     * @param leaveStream       The input stream of Kinesis messages for leave requests
     * @param icebergProperties Properties for Iceberg configuration
     */
    private static void configureLeavePipeline(DataStream<KinesisMessage> leaveStream, Properties icebergProperties) {
        // Convert Kinesis messages to LeaveRequest objects
        DataStream<LeaveRequest> leaveResults = leaveStream.flatMap(new LeaveProcessor())
                .name("Leave-Processor")
                .uid("leave-processor-operator");
        
        // Build and attach Iceberg sink - append() returns DataStreamSink
        // Pass LeaveRequest objects directly (SpecificRecord extends GenericRecord)
        IcebergSinkBuilder
                .createBuilder(icebergProperties, leaveResults, LeaveRequest.getClassSchema())
                .append()
                .name("Iceberg-Leave-Sink")
                .uid("iceberg-leave-sink-operator");
    }

}
