package com.Project.searchEngine.Controller;

import com.Project.searchEngine.model.ProductSearchDocument;
import com.Project.searchEngine.repo.ProductSearchRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/products/search")
@RequiredArgsConstructor
public class ProductSearchController {
    private final ProductSearchRepository searchRepository;
    @GetMapping
    public List<ProductSearchDocument> search(@RequestParam(name = "q") String query) {
        return searchRepository.searchByKeywordFuzzy(query);
    }
}
