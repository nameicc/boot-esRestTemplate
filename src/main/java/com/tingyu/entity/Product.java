package com.tingyu.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.elasticsearch.annotations.Document;

import java.io.Serializable;
import java.util.List;

@Document(indexName = "products")
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Product implements Serializable {

    private String sku;

    private double price;

    private long stock;

    private List<String> platTags;

}
