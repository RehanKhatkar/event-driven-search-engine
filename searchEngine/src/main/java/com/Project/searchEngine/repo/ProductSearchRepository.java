package com.Project.searchEngine.repo;

import com.Project.searchEngine.model.ProductSearchDocument;
import org.springframework.data.elasticsearch.annotations.Query;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ProductSearchRepository extends ElasticsearchRepository<ProductSearchDocument, String> {
    @Query("{\"multi_match\": {\"query\": \"?0\", \"fields\": [\"name\", \"description\", \"category\", \"tags\"], \"fuzziness\": \"AUTO\"}}")
    List<ProductSearchDocument> searchByKeywordFuzzy(String keyword);
}