package com.Project.searchEngine.Controller;

import com.Project.searchEngine.model.ProductSearchDocument;
import com.Project.searchEngine.service.ProductQueryService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/products")
@RequiredArgsConstructor
public class ProductController {
    private final ProductQueryService queryService;
    @GetMapping("/{id}")
    public ResponseEntity<ProductSearchDocument> getProduct(@PathVariable String id) {
        return queryService.getProductById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}