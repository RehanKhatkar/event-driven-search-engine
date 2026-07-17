package com.Project.searchEngine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.messaging.handler.annotation.Payload;
@Slf4j
@Service
@RequiredArgsConstructor
public class ProductEventConsumer {
    private final ObjectMapper objectMapper;
    @KafkaListener(topics = "ecommerce.ecommerce_search.products", groupId = "elasticsearch-indexer-group")
    public void consumeForElasticsearch(@Payload(required = false) String rawMessage) {
        if (rawMessage == null){
            return; // Ignore tombstones
        }
        try {
            JsonNode rootNode = objectMapper.readTree(rawMessage);
            String operation = extractOperation(rootNode);
            if (operation.equals("c") || operation.equals("u")) {
                log.info("[ELASTICSEARCH] Indexing document...");
            } else if (operation.equals("d")) {
                log.info("[ELASTICSEARCH] Removing document from index...");
            }
        } catch (Exception e) {
            log.error("[ELASTICSEARCH] Failed to process event", e);
        }
    }
    @KafkaListener(topics = "ecommerce.ecommerce_search.products",
            groupId = "redis-cache-invalidator-group",
            concurrency = "3")
    public void consumeForRedis(@Payload(required = false) String rawMessage) {
        if (rawMessage == null) {
            return; // Ignore tombstones
        }
        try {
            JsonNode rootNode = objectMapper.readTree(rawMessage);
            String operation = extractOperation(rootNode);
            if (operation.equals("u") || operation.equals("d")) {
                log.info("[REDIS THREAD - {}] Evicting stale cache data...", Thread.currentThread().getName());
            } else if (operation.equals("c")) {
                log.debug("[REDIS] Ignoring create event.");
            }
        } catch (Exception e) {
            log.error("[REDIS] Failed to process event", e);
        }
    }
    private String extractOperation(JsonNode rootNode) {
        String operation = rootNode.path("payload").path("op").asText("");
        if (operation.isEmpty()) {
            operation = rootNode.path("op").asText("");
        }
        return operation;
    }
}