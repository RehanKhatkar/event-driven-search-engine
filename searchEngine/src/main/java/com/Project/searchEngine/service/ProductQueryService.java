package com.Project.searchEngine.service;

import com.Project.searchEngine.model.ProductSearchDocument;
import com.Project.searchEngine.repo.ProductSearchRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProductQueryService {
    private final ProductSearchRepository searchRepository;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    public Optional<ProductSearchDocument> getProductById(String id) {
        String redisKey = "product:id:" + id;
        try{
            String cachedProduct = redisTemplate.opsForValue().get(redisKey);
            if (cachedProduct != null) {
                log.info("[REDIS] Cache HIT for product: {}", id);
                return Optional.of(objectMapper.readValue(cachedProduct, ProductSearchDocument.class));
            }
        }catch (Exception e){
            log.error("[REDIS] Failed to read from cache, falling back to database", e);
        }
        log.info("[REDIS] Cache MISS for product: {}. Fetching from Elasticsearch...", id);
        Optional<ProductSearchDocument> productOpt = searchRepository.findById(id);
        productOpt.ifPresent(product -> {
            try {
                String jsonProduct = objectMapper.writeValueAsString(product);
                redisTemplate.opsForValue().set(redisKey, jsonProduct, Duration.ofHours(2));
            } catch (Exception e) {
                log.error("[REDIS] Failed to write to cache", e);
            }
        });
        return productOpt;
    }
}
