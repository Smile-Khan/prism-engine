package com.smile.prism.search.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.smile.prism.search.model.EventDocument;
import com.smile.prism.search.repository.EventSearchRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

import java.util.UUID;

/**
 * Robust Event Projector with built-in fault tolerance.
 * Implements Exponential Backoff Retries and Dead Letter Queueing.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class EventProjector {

    private final EventSearchRepository searchRepository;
    private final ObjectMapper objectMapper;

    /**
     * Consumes CDC events. If a transient error occurs (e.g., Elasticsearch timeout):
     * 1. Retries 3 times.
     * 2. First retry after 2 seconds, then 4 seconds (Exponential).
     * 3. After 3 failures, moves the message to 'cdc.public.events-dlt'.
     */
    @RetryableTopic(
            attempts = "3",
            backoff = @Backoff(delay = 2000, multiplier = 2.0),
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            dltStrategy = org.springframework.kafka.retrytopic.DltStrategy.FAIL_ON_ERROR
    )
    @KafkaListener(topics = "cdc.public.events", groupId = "prism-search-group")
    public void processCdcEvent(String rawMessage) {
        try {
            JsonNode root = objectMapper.readTree(rawMessage);

            // Industrial-grade check for schemaless payload
            if (root.get("op") == null) return;

            String operation = root.get("op").asText();

            // Handle Deletes
            if ("d".equals(operation)) {
                JsonNode before = root.get("before");
                if (before != null && !before.isNull()) {
                    searchRepository.deleteById(UUID.fromString(before.get("id").asText()));
                }
                return;
            }

            // Handle Upserts (Create/Update/Read)
            JsonNode after = root.get("after");
            if (after != null && !after.isNull()) {
                EventDocument doc = mapToDocument(after);

                // SIMULATION: If title contains "FAIL", we force an error to test DLT
                if (doc.getTitle().contains("FAIL")) {
                    throw new RuntimeException("Simulated processing failure for resilience testing");
                }

                searchRepository.save(doc);
                log.info("Projector: Successfully indexed event {}", doc.getId());
            }

        } catch (Exception e) {
            log.error("Projection attempt failed. Retrying... Error: {}", e.getMessage());
            throw new RuntimeException(e); // Throwing exception triggers @RetryableTopic
        }
    }

    /**
     * Final safety net. When all retries fail, the message lands here.
     */
    @DltHandler
    public void handleDeadLetterPayload(String data) {
        log.error("CRITICAL: Message moved to DLT. Manual intervention required: {}", data);
    }

    private EventDocument mapToDocument(JsonNode node) throws Exception {
        String metadataJson = node.get("metadata").asText();
        Object metadataMap = objectMapper.readValue(metadataJson, Object.class);

        return EventDocument.builder()
                .id(UUID.fromString(node.get("id").asText()))
                .title(node.get("title").asText())
                .category(node.get("category").asText())
                .metadata(metadataMap)
                .build();
    }
}