package com.Project.searchEngine.repo;

import com.Project.searchEngine.model.FailedEvent;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface FailedEventRepository extends MongoRepository<FailedEvent, String> {

}