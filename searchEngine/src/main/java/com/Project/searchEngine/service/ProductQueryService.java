package com.Project.searchEngine.service;

import com.Project.searchEngine.model.ProductDocument;
import com.Project.searchEngine.repo.MongoProductRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import java.time.Duration;
@Slf4j
@Service
@RequiredArgsConstructor
public class ProductQueryService {
    private final MongoProductRepository productRepository;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    public ProductDocument getProductById(String id) {
        String redisKey = "product:id:" + id;
        try {
            String cachedProduct = redisTemplate.opsForValue().get(redisKey);
            if (cachedProduct != null) {
                log.debug("[REDIS] Cache hit for product {}", id);
                return objectMapper.readValue(cachedProduct, ProductDocument.class);
            }
        } catch (RedisConnectionFailureException | QueryTimeoutException e) {
            log.warn("[REDIS-DOWN] Cache unreachable, falling back to MongoDB for product {}", id);
        } catch (Exception e) {
            log.warn("[REDIS-ERROR] Unexpected cache error, falling back to MongoDB", e);
        }
        log.debug("[MONGODB] Querying primary database for product {}", id);
        ProductDocument product = productRepository.findById(id).orElseThrow(() -> new RuntimeException("Product not found"));
        try {
            String productJson = objectMapper.writeValueAsString(product);
            redisTemplate.opsForValue().set(redisKey, productJson, Duration.ofMinutes(30));
        } catch (Exception e) {
            log.warn("[REDIS-DOWN] Could not save to cache, but returning data to user anyway.");
        }
        return product;
    }
}