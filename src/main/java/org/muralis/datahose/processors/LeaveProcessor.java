package org.muralis.datahose.processors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.muralis.datahose.avro.LeaveRequest;
import org.muralis.datahose.dto.KinesisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processes LeaveRequest JSON messages from Kinesis and converts to Avro LeaveRequest records.
 * Expected JSON: {"employeeId":"...", "leaveType":"...", "startDate":"...", "endDate":"...", "approvalStatus":"..."}
 */
public class LeaveProcessor extends RichFlatMapFunction<KinesisMessage, LeaveRequest> {
    private static final Logger LOG = LoggerFactory.getLogger(LeaveProcessor.class);
    private transient ObjectMapper objectMapper;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.objectMapper = new ObjectMapper();
        LOG.info("LeaveProcessor initialized");
    }

    @Override
    public void flatMap(KinesisMessage message, Collector<LeaveRequest> out) throws Exception {
        try {
            // The metadata field contains the JSON payload
            String jsonPayload = message.getMetadata();
            if (jsonPayload == null || jsonPayload.trim().isEmpty()) {
                LOG.warn("Empty JSON payload in message");
                return;
            }
            
            LOG.info("Processing LeaveRequest JSON: {}", jsonPayload);
            
            // Parse JSON into JsonNode
            JsonNode jsonNode = objectMapper.readTree(jsonPayload);
            
            // Extract the nested "data" object
            JsonNode dataNode = jsonNode.get("data");
            if (dataNode == null) {
                LOG.warn("Missing 'data' field in JSON payload");
                return;
            }
            
            // Extract fields and create Avro LeaveRequest
            String employeeId = dataNode.get("employeeId").asText();
            String leaveType = dataNode.get("leaveType").asText();
            String startDate = dataNode.get("startDate").asText();
            String endDate = dataNode.get("endDate").asText();
            String approvalStatus = dataNode.get("approvalStatus").asText();
            
            // Create Avro LeaveRequest using builder pattern
            LeaveRequest leaveRequest = LeaveRequest.newBuilder()
                .setEmployeeId(employeeId)
                .setLeaveType(leaveType)
                .setStartDate(startDate)
                .setEndDate(endDate)
                .setApprovalStatus(approvalStatus)
                .build();
            
            out.collect(leaveRequest);
            LOG.info("Successfully processed LeaveRequest for employee: {}", employeeId);
            
        } catch (Exception e) {
            LOG.error("Error processing LeaveRequest JSON: {}", e.getMessage(), e);
            throw e;
        }
    }
}
