package org.muralis.datahose.processors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.muralis.datahose.dto.KinesisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Routes incoming Kinesis messages to appropriate side outputs based on message type.
 * Uses Flink's Side Output feature to split the stream into multiple processing pipelines (Claims, Leave Requests, Unknown).
 * This pattern enables clean separation of concerns and allows each pipeline to be scaled independently.
 */
public class MessageRouter extends ProcessFunction<String, KinesisMessage> {
    private static final Logger LOG = LoggerFactory.getLogger(MessageRouter.class);
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    // Message type constants
    private static final String MSG_TYPE_CLAIM = "CLAIM";
    private static final String MSG_TYPE_LEAVE_REQUEST = "LEAVE_REQUEST";
    private static final String MSG_TYPE_UNKNOWN = "UNKNOWN";
    private static final String MSG_TYPE_PARSE_ERROR = "PARSE_ERROR";
    
    // Field names for message detection
    private static final String FIELD_MESSAGE_TYPE = "messageType";
    private static final String FIELD_DATA = "data";
    private static final String FIELD_CLAIM_ID = "claimId";
    private static final String FIELD_EMPLOYEE_ID = "employeeId";
    
    // Define side output tags for each message type (Claims and Leave Requests only)
    public static final OutputTag<KinesisMessage> CLAIMS_TAG = 
        new OutputTag<KinesisMessage>("claims-output") {};
    
    public static final OutputTag<KinesisMessage> LEAVE_TAG = 
        new OutputTag<KinesisMessage>("leave-output") {};
    
    public static final OutputTag<KinesisMessage> UNKNOWN_TAG = 
        new OutputTag<KinesisMessage>("unknown-output") {};

    @Override
    public void processElement(String value, Context ctx, Collector<KinesisMessage> out) throws Exception {
        try {
            // Parse JSON message
            LOG.info("Received message: {}", value);
            
            JsonNode jsonNode = objectMapper.readTree(value);
            
            // Create KinesisMessage with the JSON payload in metadata field
            KinesisMessage message = new KinesisMessage();
            message.setMetadata(value);  // Store the entire JSON payload
            
            // Check for messageType field first
            if (jsonNode.has(FIELD_MESSAGE_TYPE)) {
                String messageType = jsonNode.get(FIELD_MESSAGE_TYPE).asText();
                
                switch (messageType) {
                    case MSG_TYPE_CLAIM:
                        message.setMessageType(MSG_TYPE_CLAIM);
                        ctx.output(CLAIMS_TAG, message);
                        LOG.debug("Routed to CLAIMS pipeline");
                        break;
                        
                    case MSG_TYPE_LEAVE_REQUEST:
                        message.setMessageType(MSG_TYPE_LEAVE_REQUEST);
                        ctx.output(LEAVE_TAG, message);
                        LOG.debug("Routed to LEAVE_REQUEST pipeline");
                        break;
                        
                    default:
                        message.setMessageType(MSG_TYPE_UNKNOWN);
                        ctx.output(UNKNOWN_TAG, message);
                        LOG.warn("Unknown message type: {}", messageType);
                        break;
                }
            } else {
                // Fallback: check for specific fields in data
                JsonNode dataNode = jsonNode.has(FIELD_DATA) ? jsonNode.get(FIELD_DATA) : jsonNode;
                
                if (dataNode.has(FIELD_CLAIM_ID)) {
                    message.setMessageType(MSG_TYPE_CLAIM);
                    ctx.output(CLAIMS_TAG, message);
                    LOG.debug("Routed to CLAIMS pipeline (by field detection)");
                } else if (dataNode.has(FIELD_EMPLOYEE_ID)) {
                    message.setMessageType(MSG_TYPE_LEAVE_REQUEST);
                    ctx.output(LEAVE_TAG, message);
                    LOG.debug("Routed to LEAVE_REQUEST pipeline (by field detection)");
                } else {
                    message.setMessageType(MSG_TYPE_UNKNOWN);
                    ctx.output(UNKNOWN_TAG, message);
                    LOG.warn("Unknown message type - no matching fields found");
                }
            }
            
        } catch (Exception e) {
            LOG.error("Error parsing message: {}", e.getMessage(), e);
            // Output to unknown for error cases
            KinesisMessage errorMessage = new KinesisMessage();
            errorMessage.setMessageType(MSG_TYPE_PARSE_ERROR);
            errorMessage.setMetadata("Error: " + e.getMessage());
            ctx.output(UNKNOWN_TAG, errorMessage);
        }
    }
}
