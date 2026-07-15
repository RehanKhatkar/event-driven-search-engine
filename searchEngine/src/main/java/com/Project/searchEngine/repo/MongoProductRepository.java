package com.Project.searchEngine.repo;
import com.Project.searchEngine.model.ProductDocument;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;
@Repository
public interface MongoProductRepository extends MongoRepository<ProductDocument, String> {

}