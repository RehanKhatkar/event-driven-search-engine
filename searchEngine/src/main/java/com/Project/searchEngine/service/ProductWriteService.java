package com.Project.searchEngine.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import com.Project.searchEngine.model.ProductDocument;
import org.springframework.stereotype.Service;
import com.Project.searchEngine.repo.MongoProductRepository;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProductWriteService {
    private final MongoProductRepository repository;
    public ProductDocument createProduct(ProductDocument product) {
        log.info("Ingesting new product to MongoDB: {}", product.getName());
        return repository.save(product);
    }
    public ProductDocument patchProduct(String id, ProductDocument partialUpdate) {
        log.info("Partially updating product in MongoDB with ID: {}", id);
        return repository.findById(id).map(existingProduct -> {
            if (partialUpdate.getName() != null) {
                existingProduct.setName(partialUpdate.getName());
            }
            if (partialUpdate.getDescription() != null) {
                existingProduct.setDescription(partialUpdate.getDescription());
            }
            if (partialUpdate.getCategory() != null) {
                existingProduct.setCategory(partialUpdate.getCategory());
            }
            if (partialUpdate.getTags() != null) {
                existingProduct.setTags(partialUpdate.getTags());
            }
            if (partialUpdate.getVariants() != null) {
                existingProduct.setVariants(partialUpdate.getVariants());
            }
            return repository.save(existingProduct);
        }).orElseThrow(() -> new RuntimeException("Product not found with id: " + id));
    }
    public ProductDocument getProductById(String id) {
        log.info("Fetching product from MongoDB with ID: {}", id);
        return repository.findById(id).orElseThrow(() -> new RuntimeException("Product not found with id: " + id));
    }
    public void deleteProduct(String id) {
        log.info("Deleting product from MongoDB with ID: {}", id);
        if (!repository.existsById(id)) {
            throw new RuntimeException("Product not found with id: " + id);
        }
        repository.deleteById(id);
    }
}
