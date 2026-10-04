package com.practice.grantbatch.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.batch.integration.partition.StepExecutionRequest;

import java.io.IOException;
import java.util.Map;

/**
 * The only thing sent over Kafka for a partition is (stepName, jobExecutionId, stepExecutionId).
 * The worker loads the real ExecutionContext from the BATCH_* tables.
 */
public final class StepRequestSerde {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private StepRequestSerde() {}

    public static class Ser implements Serializer<StepExecutionRequest> {
        @Override
        public byte[] serialize(String topic, StepExecutionRequest r) {
            if (r == null) return null;
            try {
                return MAPPER.writeValueAsBytes(Map.of(
                        "stepName", r.getStepName(),
                        "jobExecutionId", r.getJobExecutionId(),
                        "stepExecutionId", r.getStepExecutionId()));
            } catch (JsonProcessingException e) {
                throw new SerializationException(e);
            }
        }
    }

    public static class De implements Deserializer<StepExecutionRequest> {
        @Override
        public StepExecutionRequest deserialize(String topic, byte[] data) {
            if (data == null) return null;
            try {
                JsonNode n = MAPPER.readTree(data);
                return new StepExecutionRequest(n.get("stepName").asText(),
                        n.get("jobExecutionId").asLong(), n.get("stepExecutionId").asLong());
            } catch (IOException e) {
                throw new SerializationException(e);
            }
        }
    }
}
