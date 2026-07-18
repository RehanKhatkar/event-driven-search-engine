package com.Project.searchEngine.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Document(indexName = "products")
public class ProductSearchDocument {
    @Id
    private String id;
    @Field(type = FieldType.Text)
    private String name;
    @Field(type = FieldType.Text)
    private String description;
    @Field(type = FieldType.Keyword)
    private String category;
    @Field(type = FieldType.Keyword)
    private List<String> tags;
    @Field(type = FieldType.Nested)
    private List<ProductVariant> variants;
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ProductVariant {
        @Field(type = FieldType.Keyword)
        private String sku;
        @Field(type = FieldType.Keyword)
        private String size;
        @Field(type = FieldType.Keyword)
        private String color;
        @Field(type = FieldType.Double)
        private Double price;
        @Field(type = FieldType.Integer)
        private Integer stockQuantity;
    }
}