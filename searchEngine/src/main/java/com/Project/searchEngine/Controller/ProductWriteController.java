package com.Project.searchEngine.Controller;

import lombok.RequiredArgsConstructor;
import com.Project.searchEngine.model.ProductDocument;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import com.Project.searchEngine.service.ProductWriteService;
@RestController
@RequestMapping("/api/products")
@RequiredArgsConstructor
public class ProductWriteController {
    private final ProductWriteService productWriteService;
    @PostMapping
    public ResponseEntity<ProductDocument> ingestProduct(@RequestBody ProductDocument product) {
        ProductDocument savedProduct = productWriteService.createProduct(product);
        return new ResponseEntity<>(savedProduct, HttpStatus.CREATED);
    }
    @PatchMapping("/{id}")
    public ResponseEntity<ProductDocument> patchProduct(@PathVariable String id, @RequestBody ProductDocument product) {
        ProductDocument updatedProduct = productWriteService.patchProduct(id, product);
        return ResponseEntity.ok(updatedProduct);
    }
    @GetMapping("/{id}")
    public ResponseEntity<ProductDocument> getProduct(@PathVariable String id) {
        ProductDocument product = productWriteService.getProductById(id);
        return ResponseEntity.ok(product);
    }
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteProduct(@PathVariable String id) {
        productWriteService.deleteProduct(id);
        return ResponseEntity.noContent().build();
    }
}
