package com.Project.searchEngine.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.FieldType;

import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProductVariant {
    private String sku;
    private String color;
    private String size;
    @Field(targetType = FieldType.DECIMAL128)
    private BigDecimal price;
    private int inventoryCount;
}
