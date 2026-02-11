package com.smile.prism.search.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.smile.prism.search.model.EventDocument;
import com.smile.prism.search.repository.EventSearchRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class EventProjector {

    private final EventSearchRepository searchRepository;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "cdc.public.events", groupId = "prism-search-group")
    public void processCdcEvent(String rawMessage) {
        try {
            log.debug("Received CDC message: {}", rawMessage);
            JsonNode root = objectMapper.readTree(rawMessage);

            // Since we disabled schemas in Debezium, the fields are at the ROOT level
            if (root.get("op") == null) {
                log.warn("Skipping message: No operation 'op' field found at root.");
                return;
            }

            String operation = root.get("op").asText();

            // Handle Deletes
            if ("d".equals(operation)) {
                JsonNode before = root.get("before");
                if (before != null && !before.isNull()) {
                    UUID id = UUID.fromString(before.get("id").asText());
                    searchRepository.deleteById(id);
                    log.info("Projector [DELETE]: Removed event {} from Search Index", id);
                }
                return;
            }

            // Handle Create, Update, and Read (Snapshot)
            JsonNode after = root.get("after");
            if (after != null && !after.isNull()) {
                EventDocument doc = mapToDocument(after);
                searchRepository.save(doc);
                log.info("Projector [{}]: Successfully indexed event {} to Elasticsearch", operation.toUpperCase(), doc.getId());
            }

        } catch (Exception e) {
            log.error("Failed to project Kafka event to Elasticsearch. Raw message: {}", rawMessage, e);
        }
    }

    private EventDocument mapToDocument(JsonNode node) throws Exception {
        // Debezium sends metadata as a JSON string; we convert it to an Object for ES
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