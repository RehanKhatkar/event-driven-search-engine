package com.Project.searchEngine.service;

import com.Project.searchEngine.model.FailedEvent;
import com.Project.searchEngine.model.ProductSearchDocument;
import com.Project.searchEngine.model.ProductSearchDocument.ProductVariant;
import com.Project.searchEngine.repo.FailedEventRepository;
import com.Project.searchEngine.repo.ProductSearchRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.List;
@Slf4j
@Service
@RequiredArgsConstructor
public class ProductEventConsumer {
    private final ObjectMapper objectMapper;
    private final ProductSearchRepository searchRepository;
    private final StringRedisTemplate redisTemplate;
    private final FailedEventRepository failedEventRepository;
    @RetryableTopic(attempts = "4", autoCreateTopics = "true", topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE)
    @KafkaListener(topics = "ecommerce.ecommerce_search.products", groupId = "elasticsearch-indexer-group")
    public void consumeForElasticsearch(@Payload(required = false) String rawMessage, @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String messageKey) {
        if (rawMessage == null){
            return; // Ignore tombstones
        }
        try {
            JsonNode rootNode = objectMapper.readTree(rawMessage);
            String operation = extractOperation(rootNode);
            String documentId = extractIdFromKey(messageKey);
            if (operation.equals("c") || operation.equals("u")) {
                log.info("[ELASTICSEARCH] Indexing document...");
                JsonNode payloadNode = rootNode.path("payload");
                JsonNode afterNode = payloadNode.path("after");
                if (afterNode.isMissingNode()) {
                    afterNode = rootNode.path("after");
                }
                if (!afterNode.isMissingNode() && !afterNode.isNull() && documentId != null) {
                    JsonNode documentNode = objectMapper.readTree(afterNode.asText());
                    String name = documentNode.path("name").asText();
                    String description = documentNode.path("description").asText();
                    String category = documentNode.path("category").asText(null);
                    List<String> tags = new ArrayList<>();
                    JsonNode tagsNode = documentNode.path("tags");
                    if (tagsNode.isArray()) {
                        for (JsonNode tag : tagsNode) {
                            tags.add(tag.asText());
                        }
                    }
                    List<ProductVariant> variantList = new ArrayList<>();
                    JsonNode variantsNode = documentNode.path("variants");
                    if (variantsNode.isArray()) {
                        for (JsonNode variant : variantsNode) {
                            String sku = variant.path("sku").asText();
                            String size = variant.path("size").asText();
                            String color = variant.path("color").asText();
                            Double price = variant.path("price").path("$numberDecimal").asDouble(0.0);
                            Integer stockQuantity = variant.path("inventoryCount").asInt(0);
                            variantList.add(new ProductVariant(sku, size, color, price, stockQuantity));
                        }
                    }
                    ProductSearchDocument product = new ProductSearchDocument(documentId, name, description, category, tags, variantList);
                    searchRepository.save(product);
                    log.info("[ELASTICSEARCH] Successfully saved product ID: {} to search index", documentId);
                }
            } else if (operation.equals("d")) {
                log.info("[ELASTICSEARCH] Removing document from index...");
                if (documentId != null && !documentId.isEmpty()) {
                    searchRepository.deleteById(documentId);
                    log.info("[ELASTICSEARCH] Successfully deleted product ID: {}", documentId);
                } else {
                    log.error("[ELASTICSEARCH-FATAL] Failed to extract ID from Kafka Key during delete! Key: {}", messageKey);
                }
            }
        } catch (Exception e) {
            log.warn("[ELASTICSEARCH] Failed to process event. Triggering retry mechanism...");
            throw new RuntimeException("Elasticsearch processing failed", e);
        }
    }
    @KafkaListener(topics = "ecommerce.ecommerce_search.products", groupId = "redis-cache-invalidator-group", concurrency = "3")
    public void consumeForRedis(@Payload(required = false) String rawMessage) {
        if (rawMessage == null) {
            return; // Ignore tombstones
        }
        try {
            JsonNode rootNode = objectMapper.readTree(rawMessage);
            String operation = extractOperation(rootNode);
            if (operation.equals("u") || operation.equals("d")) {
                JsonNode payloadNode = rootNode.path("payload");
                String documentId = extractIdFromPayload(payloadNode, operation);
                if (documentId != null && !documentId.isEmpty()) {
                    String redisKey = "product:id:" + documentId;
                    Boolean wasDeleted = redisTemplate.delete(redisKey);
                    if (Boolean.TRUE.equals(wasDeleted)) {
                        log.info("[REDIS THREAD - {}] Successfully evicted stale key: {}", Thread.currentThread().getName(), redisKey);
                    } else {
                        log.debug("[REDIS THREAD - {}] Key {} was not in cache (already evicted or never cached).", Thread.currentThread().getName(), redisKey);
                    }
                }
            } else if (operation.equals("c")) {
                log.debug("[REDIS] Ignoring create event.");
            }
        } catch (Exception e) {
            log.error("[REDIS] Failed to process event for cache eviction", e);
        }
    }
    private String extractOperation(JsonNode rootNode) {
        String operation = rootNode.path("payload").path("op").asText("");
        if (operation.isEmpty()) {
            operation = rootNode.path("op").asText("");
        }
        return operation;
    }
    private String extractIdFromKey(String messageKey) throws Exception {
        if (messageKey == null){
            return null;
        }
        JsonNode keyNode = objectMapper.readTree(messageKey);
        JsonNode payloadNode = keyNode.path("payload");
        String idString = payloadNode.isMissingNode() ? keyNode.path("id").asText() : payloadNode.path("id").asText();
        if (idString != null && !idString.isEmpty()) {
            if (idString.startsWith("{")) {
                JsonNode idObj = objectMapper.readTree(idString);
                if (idObj.has("$oid")) {
                    return idObj.path("$oid").asText();
                } else if (idObj.has("_id") && idObj.path("_id").has("$oid")) {
                    return idObj.path("_id").path("$oid").asText();
                }
            }
            return idString.replace("\"", "");
        }
        return null;
    }
    private String extractIdFromPayload(JsonNode payloadNode, String operation) throws Exception {
        JsonNode targetStateNode = payloadNode.path("documentKey");
        if (operation.equals("c") || operation.equals("u")) {
            targetStateNode = payloadNode.path("after");
        }
        if (!targetStateNode.isMissingNode() && !targetStateNode.isNull()) {
            JsonNode documentNode;
            if (targetStateNode.isTextual()) {
                documentNode = objectMapper.readTree(targetStateNode.asText());
            } else {
                documentNode = targetStateNode;
            }
            JsonNode idNode = documentNode.path("_id");
            if (idNode.isMissingNode() && operation.equals("d")) {
                idNode = documentNode;
            }
            if (idNode.has("$oid")) {
                return idNode.path("$oid").asText();
            } else if (idNode.isTextual()) {
                return idNode.asText();
            }
        }
        return null;
    }
    @org.springframework.kafka.annotation.DltHandler
    public void processDltMessage(
            @org.springframework.messaging.handler.annotation.Payload String rawMessage,
            @org.springframework.messaging.handler.annotation.Header(
                    name = org.springframework.kafka.support.KafkaHeaders.ORIGINAL_TOPIC,
                    required = false) String topic,
            @org.springframework.messaging.handler.annotation.Header(
                    name = org.springframework.kafka.support.KafkaHeaders.EXCEPTION_MESSAGE,
                    required = false) String errorMessage) {
        log.error("[DLQ] Max retries reached. Saving failed event to MongoDB.");
        String safeTopic = (topic != null) ? topic : "unknown-topic";
        String safeError = (errorMessage != null) ? errorMessage : "Unknown error occurred";
        try {
            FailedEvent failedEvent = new FailedEvent(null, safeTopic, rawMessage, safeError, java.time.LocalDateTime.now());
            failedEventRepository.save(failedEvent);
            log.info("[DLQ] Successfully persisted failed event to 'failed_kafka_events' collection.");
        } catch (Exception e) {
            log.error("[DLQ-FATAL] Could not save to MongoDB! Data: {}", rawMessage, e);
        }
    }
}