package com.tingyu.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.elasticsearch.annotations.Document;

import java.io.Serializable;

@Document(indexName = "es-test")
@Data
@AllArgsConstructor
@NoArgsConstructor
public class User implements Serializable {

    private String id;

    private String name;

    private long age;

    private String sex;

    private String address;

}
