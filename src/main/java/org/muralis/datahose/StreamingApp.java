package org.muralis.datahose;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import java.time.Duration;
import java.time.Instant;

/**
 * Continuous Streaming Application - Write to S3 Sink
 * Generates streaming data continuously and writes to S3
 */
public class StreamingApp {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000); // checkpoint every 60 seconds

        // Create continuous data generator source (unbounded)
        DataGeneratorSource<String> dataGenerator = new DataGeneratorSource<>(
            index -> {
                String timestamp = Instant.now().toString();
                String message = String.format("Record-%d: Data from foams column at %s", index, timestamp);
                return message;
            },
            Long.MAX_VALUE,  // Generate unlimited records
            RateLimiterStrategy.perSecond(2), // Generate 2 records per second
            Types.STRING
        );

        DataStream<String> dataStream = env.fromSource(
            dataGenerator,
            WatermarkStrategy.noWatermarks(),
            "Continuous Data Generator"
        );

        RollingPolicy<String,String> rollingPolicy = DefaultRollingPolicy.builder()
                .withRolloverInterval(Duration.ofMinutes(2))
                .withInactivityInterval(Duration.ofSeconds(30))
                .withMaxPartSize(MemorySize.ofMebiBytes(1))
                .build();

        FileSink<String> s3Sink = FileSink
                .forRowFormat(new Path("s3://tm-data-bucket-20251010/datafall"),new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(rollingPolicy)
                .build();

        dataStream.sinkTo(s3Sink);

        env.execute("Flink S3 Sink Application");
    }

}