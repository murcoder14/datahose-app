package org.muralis.datahose.processors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.muralis.datahose.avro.Claim;
import org.muralis.datahose.dto.KinesisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processes Claim JSON messages from Kinesis and converts to Avro Claim records.
 * Expected JSON: {"claimId":"...", "status":"...", "amount":123.45, "processedAt":"..."}
 */
public class ClaimsProcessor extends RichFlatMapFunction<KinesisMessage, Claim> {
    private static final Logger LOG = LoggerFactory.getLogger(ClaimsProcessor.class);
    private transient ObjectMapper objectMapper;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.objectMapper = new ObjectMapper();
        LOG.info("ClaimsProcessor initialized");
    }

    @Override
    public void flatMap(KinesisMessage message, Collector<Claim> out) throws Exception {
        try {
            // The metadata field contains the JSON payload
            String jsonPayload = message.getMetadata();
            if (jsonPayload == null || jsonPayload.trim().isEmpty()) {
                LOG.warn("Empty JSON payload in message");
                return;
            }
            
            LOG.info("Processing Claim JSON: {}", jsonPayload);
            
            // Parse JSON into JsonNode
            JsonNode jsonNode = objectMapper.readTree(jsonPayload);
            
            // Extract the nested "data" object
            JsonNode dataNode = jsonNode.get("data");
            if (dataNode == null) {
                LOG.warn("Missing 'data' field in JSON payload");
                return;
            }
            
            // Extract fields and create Avro Claim
            String claimId = dataNode.get("claimId").asText();
            String status = dataNode.get("status").asText();
            double amount = dataNode.get("amount").asDouble();
            String processedAt = dataNode.get("processedAt").asText();
            
            // Create Avro Claim using builder pattern
            Claim claim = Claim.newBuilder()
                .setClaimId(claimId)
                .setStatus(status)
                .setAmount(amount)
                .setProcessedAt(processedAt)
                .build();
            
            out.collect(claim);
            LOG.info("Successfully processed Claim: {}", claimId);
            
        } catch (Exception e) {
            LOG.error("Error processing Claim JSON: {}", e.getMessage(), e);
            throw e;
        }
    }
}
