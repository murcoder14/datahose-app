package org.muralis.datahose.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Base message received from Kinesis Data Stream.
 * Contains message type and S3 location details.
 */
@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class KinesisMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("messageType")
    private String messageType;

    @JsonProperty("s3Bucket")
    private String s3Bucket;

    @JsonProperty("s3Key")
    private String s3Key;

    @JsonProperty("timestamp")
    private long timestamp;

    @JsonProperty("metadata")
    private String metadata;

    public KinesisMessage(String messageType, String s3Bucket, String s3Key) {
        this.messageType = messageType;
        this.s3Bucket = s3Bucket;
        this.s3Key = s3Key;
        this.timestamp = System.currentTimeMillis();
    }

    /**
     * Gets the message type as an enum value.
     * @return MessageType enum value
     */
    public MessageType getMessageTypeEnum() {
        try {
            return MessageType.valueOf(messageType);
        } catch (IllegalArgumentException e) {
            return MessageType.UNKNOWN;
        }
    }

    @Override
    public String toString() {
        return "KinesisMessage{" +
                "messageType='" + messageType + '\'' +
                ", s3Bucket='" + s3Bucket + '\'' +
                ", s3Key='" + s3Key + '\'' +
                ", timestamp=" + timestamp +
                ", metadata='" + metadata + '\'' +
                '}';
    }
}
