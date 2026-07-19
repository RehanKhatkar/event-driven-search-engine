package com.Project.searchEngine.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "failed_kafka_events")
public class FailedEvent {
    @Id
    private String id;
    private String originalTopic;
    private String payload;
    private String errorMessage;
    private LocalDateTime failureTime;
}