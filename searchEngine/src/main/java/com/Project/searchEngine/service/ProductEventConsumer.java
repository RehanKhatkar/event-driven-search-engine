package com.Project.searchEngine.service;

import com.Project.searchEngine.model.ProductSearchDocument;
import com.Project.searchEngine.model.ProductSearchDocument.ProductVariant;
import com.Project.searchEngine.repo.ProductSearchRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.List;
@Slf4j
@Service
@RequiredArgsConstructor
public class ProductEventConsumer {
    private final ObjectMapper objectMapper;
    private final ProductSearchRepository searchRepository; // 1. Inject the ES Repository
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
                JsonNode payloadNode = rootNode.path("payload");
                JsonNode afterNode = payloadNode.path("after");
                if (afterNode.isMissingNode()) {
                    afterNode = rootNode.path("after");
                }
                if (!afterNode.isMissingNode() && !afterNode.isNull()) {
                    JsonNode documentNode = objectMapper.readTree(afterNode.asText());
                    String id = documentNode.path("_id").path("$oid").asText();
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
                    ProductSearchDocument product = new ProductSearchDocument(id, name, description, category, tags, variantList);
                    searchRepository.save(product);
                    log.info("[ELASTICSEARCH] Successfully saved product ID: {} to search index", id);
                }

            } else if (operation.equals("d")) {
                log.info("[ELASTICSEARCH] Removing document from index...");
            }
        } catch (Exception e) {
            log.error("[ELASTICSEARCH] Failed to process event", e);
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