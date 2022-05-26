package dev.buesing.ksd.common.domain;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.math.BigDecimal;
import lombok.Data;

@Data
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "$type")
public class Product {

    private String sku;
    private BigDecimal price;

}
