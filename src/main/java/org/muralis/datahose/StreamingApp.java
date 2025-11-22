package org.muralis.datahose;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.muralis.datahose.avro.Claim;
import org.muralis.datahose.avro.LeaveRequest;
import org.muralis.datahose.dto.KinesisMessage;
import org.muralis.datahose.processors.ClaimsProcessor;
import org.muralis.datahose.processors.LeaveProcessor;
import org.muralis.datahose.processors.MessageRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Multi-Pipeline Streaming Application using Kinesis Data Stream and Side Outputs.
 * Reads messages from Kinesis, routes them to appropriate processing pipelines, and writes results to separate S3 locations.
 */
public class StreamingApp {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingApp.class);
    public static final String KINESIS_SOURCE = "KinesisSource";
    public static final String AWS_REGION = "aws.region";
    public static final String KINESIS_STREAM_ARN = "stream.arn";
    
    public static final String S3_SOURCE = "s3source";
    public static final String VISITS_INPUT_BUCKET = "visits-input-bucket";
    public static final String VISITS_INPUT_KEY = "visits-input-key";
    
    public static final String S3_SINK = "s3sink";  
    public static final String VISITS_OUTPUT_BUCKET = "visits-output-bucket";
    public static final String CLAIMS_OUTPUT_BUCKET = "claims-output-bucket";
    public static final String LEAVE_OUTPUT_BUCKET = "leaverequests-output-bucket";
    public static final String DEFAULT_OUTPUT_BUCKET = "default-output-bucket";

    public static void main(String[] args) throws Exception {

        LOG.info("Starting Multi-Pipeline Streaming Application with Kinesis and Side Outputs");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String region = "us-east-2";
        String kinesisStreamArn = null;
        String s3VisitsInputBucket = null;
        String s3VisitsOutputBucket = null;
        String s3ClaimsOutputBucket = null;
        String s3LeaveOutputBucket = null;
        String s3DefaultOutputBucket = null;
        
        try {
            Map<String, Properties> appProps = KinesisAnalyticsRuntime.getApplicationProperties();
            if (appProps != null) {

                if (appProps.containsKey(KINESIS_SOURCE)) {
                    region = appProps.get(KINESIS_SOURCE).getProperty(AWS_REGION, region);
                    kinesisStreamArn = appProps.get(KINESIS_SOURCE).getProperty(KINESIS_STREAM_ARN);
                }
                
                if (appProps.containsKey(S3_SOURCE)) {
                    s3VisitsInputBucket = appProps.get(S3_SOURCE).getProperty(VISITS_INPUT_BUCKET);
                }
                
                if (appProps.containsKey(S3_SINK)) {
                    s3VisitsOutputBucket = appProps.get(S3_SINK).getProperty(VISITS_OUTPUT_BUCKET);
                    s3ClaimsOutputBucket = appProps.get(S3_SINK).getProperty(CLAIMS_OUTPUT_BUCKET);
                    s3LeaveOutputBucket = appProps.get(S3_SINK).getProperty(LEAVE_OUTPUT_BUCKET);
                    s3DefaultOutputBucket = appProps.get(S3_SINK).getProperty(DEFAULT_OUTPUT_BUCKET);
                }
            }
        } catch (IOException e) {
            LOG.error("Failed to load application properties: {}", e.getMessage(), e);
            throw new IllegalConfigurationException("Failed to load application properties. Please verify the runtime configuration.", e);
        }

        // Validate required configuration - terminate gracefully if missing
        validateRequiredConfiguration(kinesisStreamArn, KINESIS_STREAM_ARN,"Kinesis Stream ARN (" + KINESIS_SOURCE + "." + KINESIS_STREAM_ARN + ")");
        validateRequiredConfiguration(s3VisitsInputBucket, VISITS_INPUT_BUCKET,"S3 Visits-Input-Bucket (" + S3_SOURCE + "." + VISITS_INPUT_BUCKET + ")");
        validateRequiredConfiguration(s3VisitsOutputBucket, VISITS_OUTPUT_BUCKET,"S3 Visits-Output-Bucket (" + S3_SINK + "." + VISITS_OUTPUT_BUCKET + ")");
        validateRequiredConfiguration(s3ClaimsOutputBucket, CLAIMS_OUTPUT_BUCKET,"S3 Claims-Output-Bucket (" + S3_SINK + "." + CLAIMS_OUTPUT_BUCKET + ")");
        validateRequiredConfiguration(s3LeaveOutputBucket, LEAVE_OUTPUT_BUCKET,"S3 Leave-Output-Bucket (" + S3_SINK + "." + LEAVE_OUTPUT_BUCKET + ")");
        validateRequiredConfiguration(s3DefaultOutputBucket, DEFAULT_OUTPUT_BUCKET,"S3 Default-Output-Bucket (" + S3_SINK + "." + DEFAULT_OUTPUT_BUCKET + ")");

        LOG.info("=== Configuration Validation Passed ===");
        LOG.info("Region: {}", region);
        LOG.info("Kinesis Stream ARN: {}", kinesisStreamArn);
        LOG.info("S3 Visits Input Bucket: {}", s3VisitsInputBucket);
        LOG.info("S3 Visits Output Bucket: {}", s3VisitsOutputBucket);
        LOG.info("S3 Claims Output Bucket: {}", s3ClaimsOutputBucket);
        LOG.info("S3 Leave Output Bucket: {}", s3LeaveOutputBucket);
        LOG.info("S3 Default Output Bucket: {}", s3DefaultOutputBucket);
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
        DataStream<String> kinesisStream = env.fromSource(kinesisSource,WatermarkStrategy.noWatermarks(),"Kinesis-Source",TypeInformation.of(String.class));

        // Route messages using Side Outputs
        SingleOutputStreamOperator<KinesisMessage> mainStream = kinesisStream.process(new MessageRouter());

        // Extract side output streams for each message type
        DataStream<KinesisMessage> claimsStream = mainStream.getSideOutput(MessageRouter.CLAIMS_TAG);
        DataStream<KinesisMessage> leaveStream = mainStream.getSideOutput(MessageRouter.LEAVE_TAG);
        DataStream<KinesisMessage> unknownStream = mainStream.getSideOutput(MessageRouter.UNKNOWN_TAG);

        // Set up processing pipelines for Claims and Leave Requests only
        configureClaimsPipeline(claimsStream, s3ClaimsOutputBucket);
        configureLeavePipeline(leaveStream, s3LeaveOutputBucket);

        // UNKNOWN MESSAGES: Log to separate location for investigation
        DataStream<String> unknownOutputs = unknownStream.map(msg -> "UNKNOWN: " + msg.toString()).name("Format-Unknown");
        FileSink<String> unknownSink = FileSink
                .forRowFormat(new Path("s3://" + s3DefaultOutputBucket + "/unknown/"), 
                        new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();
        unknownOutputs.sinkTo(unknownSink).name("Unknown-S3-Sink");

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
     * Sets up the claims processing pipeline with Avro output.
     *
     * @param claimsStream   The input stream of Kinesis messages for claims
     * @param s3OutputBucket The S3 bucket for output
     */
    private static void configureClaimsPipeline(DataStream<KinesisMessage> claimsStream, String s3OutputBucket) {
        DataStream<Claim> claimResults = claimsStream.flatMap(new ClaimsProcessor()).name("Claims-Processor");
        FileSink<Claim> claimsSink = createAvroS3Sink(s3OutputBucket, "/claims/", Claim.class, ".avro");
        claimResults.sinkTo(claimsSink).name("Claims-S3-Sink");
    }

    /**
     * Sets up the leave of absence processing pipeline with Avro output.
     *
     * @param leaveStream    The input stream of Kinesis messages for leave requests
     * @param s3OutputBucket The S3 bucket for output
     */
    private static void configureLeavePipeline(DataStream<KinesisMessage> leaveStream, String s3OutputBucket) {
        DataStream<LeaveRequest> leaveResults = leaveStream.flatMap(new LeaveProcessor()).name("Leave-Processor");
        FileSink<LeaveRequest> leaveSink = createAvroS3Sink(s3OutputBucket, "/leave-of-absence/", LeaveRequest.class, ".avro");
        leaveResults.sinkTo(leaveSink).name("Leave-S3-Sink");
    }

    /**
     * Creates a FileSink for writing Avro data to S3.
     *
     * @param s3OutputBucket The base S3 bucket name
     * @param pathSuffix    The suffix to append to the base path
     * @param recordClass   The Avro record class
     * @param fileSuffix    The file suffix (e.g., ".avro")
     * @return Configured FileSink instance for Avro
     */
    private static <T extends org.apache.avro.specific.SpecificRecordBase> FileSink<T> createAvroS3Sink(
            String s3OutputBucket, String pathSuffix, Class<T> recordClass, String fileSuffix) {
        return FileSink
                .forBulkFormat(new Path("s3://" + s3OutputBucket + pathSuffix), 
                        AvroWriters.forSpecificRecord(recordClass))
                // Bucketing by date/time
                .withBucketAssigner(new DateTimeBucketAssigner<>("'year='yyyy'/month='MM'/day='dd'/hour='HH/"))
                // Part file rolling - rolls on checkpoint
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartSuffix(fileSuffix)
                        .build())
                .build();
    }

}
