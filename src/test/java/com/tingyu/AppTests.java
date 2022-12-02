package com.tingyu;

import com.alibaba.fastjson.JSON;
import com.tingyu.entity.Product;
import com.tingyu.entity.User;
import org.assertj.core.util.Lists;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.*;
import org.springframework.data.elasticsearch.core.document.Document;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.*;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SpringBootTest
class AppTests {

    @Resource
    private ElasticsearchRestTemplate elasticsearchRestTemplate;

    @Value("${elasticsearch.index_name}")
    private String indexName;

    /**
     * 索引创建
     **/
    @Test
    void testIndexCreate() {
        Map<String, Object> settings = new HashMap<>();
        settings.put("number_of_shards", 1);
        settings.put("number_of_replicas", 1);
        String mapStr = "{\"properties\":{\"id\":{\"type\":\"keyword\"},\"name\":{\"type\":\"keyword\"},\"age\":{\"type\":\"long\"},\"sex\":{\"type\":\"text\"},\"address\":{\"type\":\"text\"}}}";
        boolean success = elasticsearchRestTemplate.indexOps(IndexCoordinates.of(indexName)).create(settings, Document.parse(mapStr));
        System.out.println(success);
    }

    /**
     * 索引是否存在
     **/
    @Test
    void testIndexExist() {
        boolean exists = elasticsearchRestTemplate.indexOps(IndexCoordinates.of(indexName)).exists();
        System.out.println(exists);
    }

    /**
     * 索引删除
     **/
    @Test
    void testIndexDelete() {
        boolean success = elasticsearchRestTemplate.indexOps(IndexCoordinates.of(indexName)).delete();
        System.out.println(success);
    }

    /**
     * 文档创建 - 指定索引
     **/
    @Test
    void testDocumentCreate() {
        User user = new User("1", "Allen", 23, "male", "Qingdao");
        User savedUser = elasticsearchRestTemplate.save(user, IndexCoordinates.of(indexName));
        System.out.println(JSON.toJSONString(savedUser));
    }

    /**
     * 文档创建 - 实体上注解指定索引
     **/
    @Test
    void testDocumentCreate2() {
        User user = new User("3", "Cindy", 18, "female", "Dongying");
        User savedUser = elasticsearchRestTemplate.save(user);
        System.out.println(JSON.toJSONString(savedUser));
    }

    /**
     * 文档批量创建 - bulk
     **/
    @Test
    void testDocumentCreateBatch() {
        List<User> users = new ArrayList<>();
        /*users.add(new User("11", "eleven", 11, "male", "Weihai"));
        users.add(new User("12", "twelve", 12, "female", "Binzhou"));
        users.add(new User("13", "thirteen", 13, "male", "Taian"));
        users.add(new User("14", "forteen", 14, "female", "Heze"));
        users.add(new User("15", "fifteen", 15, "male", "Zaozhuang"));
        users.add(new User("16", "sixteen", 16, "female", "Qingdao"));*/
        users.add(new User("21", "张三", 21, "男", "威海"));
        users.add(new User("22", "李四", 22, "女", "上海"));
        users.add(new User("23", "王五", 23, "男", "青岛"));
        users.add(new User("24", "赵六", 24, "女", "青海"));
        users.add(new User("25", "孙七", 25, "男", "辽宁"));
        users.add(new User("26", "胡八", 26, "女", "连云港"));
        // bulk操作
        List<IndexQuery> indexQueries = new ArrayList<>();
        users.forEach(user -> {
            IndexQuery indexQuery = new IndexQuery();
            indexQuery.setObject(user);
            indexQueries.add(indexQuery);
        });
        List<IndexedObjectInformation> informations = elasticsearchRestTemplate.bulkIndex(indexQueries, User.class);
        System.out.println(JSON.toJSONString(informations));
    }

    /**
     * 文档更新 - 根据id
     **/
    @Test
    void testDocumentUpdateById() {
        Document document = Document.parse("{\"age\":39,\"address\":\"Yantai\"}");
//        Document document = Document.create();
//        document.put("age", 23);
//        document.put("address", "Qingdao");
        UpdateQuery updateQuery = UpdateQuery.builder("1").withDocument(document).build();
        UpdateResponse updateResponse = elasticsearchRestTemplate.update(updateQuery, IndexCoordinates.of(indexName));
        System.out.println(JSON.toJSONString(updateResponse));
    }

    /**
     * 文档更新 - 根据查询
     **/
    @Test
    void testDocumentUpdateByQuery() {
        UpdateQuery updateQuery = UpdateQuery.builder(new NativeSearchQuery(QueryBuilders.termQuery("name", "sixteen")))
                .withScriptType(ScriptType.INLINE).withLang("painless").withScript("ctx._source['age']=16").build();
        ByQueryResponse queryResponse = elasticsearchRestTemplate.updateByQuery(updateQuery, IndexCoordinates.of(indexName));
        System.out.println(JSON.toJSONString(queryResponse));
    }

    /**
     * 文档删除 - 根据id
     **/
    @Test
    void testDocumentDeleteById() {
        String response = elasticsearchRestTemplate.delete("3", IndexCoordinates.of(indexName));
        System.out.println(response);
    }

    /**
     * 文档删除 - 根据对象，底层还是根据id
     **/
    @Test
    void testDocumentDeleteByEntity() {
        // 底层实现还是根据id进行删除的
        User user = new User("3", "", 0, "", "");
        String response = elasticsearchRestTemplate.delete(user);
        System.out.println(response);
    }

    /**
     * 文档删除 - 根据查询
     **/
    @Test
    void testDocumentDeleteByQuery() {
        TermQueryBuilder queryBuilder = QueryBuilders.termQuery("sex", "male");
        ByQueryResponse response = elasticsearchRestTemplate.delete(new NativeSearchQuery(queryBuilder), User.class);
        System.out.println(JSON.toJSONString(response));
    }

    /**
     * 单条件查询
     **/
    @Test
    void testDocumentQuery() {
        List<User> users = new ArrayList<>();
        Query query = new NativeSearchQuery(QueryBuilders.matchQuery("address", "Qingdao"));
        SearchHits<User> searchHits = elasticsearchRestTemplate.search(query, User.class);
        if (searchHits.hasSearchHits()) {
            searchHits.getSearchHits().forEach(hit -> {
                users.add(hit.getContent());
            });
        }
        System.out.println(JSON.toJSONString(users));
    }

    /**
     * 复杂查询
     **/
    @Test
    void testDocumentQuery2() {
        List<User> users = new ArrayList<>();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.filter(QueryBuilders.matchQuery("sex", "male"))
                .must(QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("address", "Qingdao"))
                        .should(QueryBuilders.matchQuery("address", "Weihai")));
        System.out.println(boolQueryBuilder.toString());
        SearchHits<User> searchHits = elasticsearchRestTemplate.search(new NativeSearchQuery(boolQueryBuilder), User.class);
        if (searchHits.hasSearchHits()) {
            searchHits.getSearchHits().forEach(hit -> {
                users.add(hit.getContent());
            });
        }
        System.out.println(JSON.toJSONString(users));
    }

    /**
     * 搜索排序
     **/
    @Test
    void testDocumentQueryByOrder() {
        List<User> users = new ArrayList<>();
        NativeSearchQuery query = new NativeSearchQuery(QueryBuilders.matchAllQuery());
        query.addSort(Sort.by(Sort.Direction.DESC, "age"));
        SearchHits<User> searchHits = elasticsearchRestTemplate.search(query, User.class);
        if (searchHits.hasSearchHits()) {
            searchHits.getSearchHits().forEach(hit -> {
                users.add(hit.getContent());
            });
        }
        System.out.println(JSON.toJSONString(users));
    }

    /**
     * 搜索排序分页
     **/
    @Test
    void testDocumentQueryByPager() {
        List<User> users = new ArrayList<>();
        NativeSearchQuery query = new NativeSearchQuery(QueryBuilders.matchAllQuery());
        query.setPageable(PageRequest.of(2, 2, Sort.Direction.DESC, "age"));
        SearchHits<User> searchHits = elasticsearchRestTemplate.search(query, User.class);
        if (searchHits.hasSearchHits()) {
            searchHits.getSearchHits().forEach(userSearchHit -> users.add(userSearchHit.getContent()));
        }
        System.out.println(JSON.toJSONString(users));
    }

    /**
     * 范围查询
     **/
    @Test
    void testDocumentQueryByRange() {
        List<User> users = new ArrayList<>();
        RangeQueryBuilder queryBuilder = QueryBuilders.rangeQuery("age");
        queryBuilder.gte(13);
        queryBuilder.lte(16);
        NativeSearchQuery query = new NativeSearchQuery(queryBuilder);
        query.addSort(Sort.by(Sort.Direction.DESC, "age"));
        SearchHits<User> searchHits = elasticsearchRestTemplate.search(query, User.class);
        if (searchHits.hasSearchHits()) {
            searchHits.getSearchHits().forEach(userSearchHit -> users.add(userSearchHit.getContent()));
        }
        System.out.println(JSON.toJSONString(users));
    }

    /**
     * 搜索高亮
     **/
    @Test
    void testDocumentQueryHighLight() {
        List<User> users = new ArrayList<>();
        // 搜索条件
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("address", "青海");
        // 高亮
        HighlightBuilder highlightBuilder = new HighlightBuilder().field("address").preTags("<p style='color:red'>").postTags("</p>").fragmentSize(1);
        // 搜索
        NativeSearchQuery query = new NativeSearchQueryBuilder().withQuery(matchQueryBuilder).withHighlightBuilder(highlightBuilder).build();
        // 结果
        SearchHits<User> searchHits = elasticsearchRestTemplate.search(query, User.class);
        if (searchHits.hasSearchHits()) {
            searchHits.getSearchHits().forEach(hit -> {
                User user = hit.getContent();
                Map<String, List<String>> highlightFields = hit.getHighlightFields();
                user.setAddress(highlightFields.get("address").get(0));
                users.add(user);
            });
        }
        System.out.println(JSON.toJSONString(users));
    }

    /**
     * Terms查询 - 如果两个列表有交集，就能被搜索到
     **/
    @Test
    void testSaveProducts() {
        List<Product> products = new ArrayList<>();
        products.add(new Product("sku1", 12.1, 22, Lists.newArrayList("1", "2", "3", "4")));
        products.add(new Product("sku2", 12.2, 23, Lists.newArrayList("1", "2")));
        products.add(new Product("sku3", 12.3, 24, Lists.newArrayList("3", "4")));
        products.add(new Product("sku4", 12.4, 25, Lists.newArrayList("2", "3")));
        products.add(new Product("sku5", 12.5, 26, Lists.newArrayList("1", "4")));

        List<IndexQuery> indexQueries = new ArrayList<>();
        products.forEach(product -> {
            IndexQuery indexQuery = new IndexQuery();
            indexQuery.setObject(product);
            indexQueries.add(indexQuery);
        });

        List<IndexedObjectInformation> informations = elasticsearchRestTemplate.bulkIndex(indexQueries, Product.class);
        System.out.println(JSON.toJSONString(informations));
    }

    @Test
    void testQueryProducts() {
        List<Product> products = new ArrayList<>();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.mustNot(QueryBuilders.termsQuery("platTags", Lists.newArrayList("1", "4")));
        System.out.println(boolQueryBuilder.toString());
        SearchHits<Product> searchHits = elasticsearchRestTemplate.search(new NativeSearchQuery(boolQueryBuilder), Product.class);
        if (searchHits.hasSearchHits()) {
            searchHits.getSearchHits().forEach(hit -> {
                products.add(hit.getContent());
            });
        }
        System.out.println(JSON.toJSONString(products));
    }

    /**
     * 聚合查询
     **/
    @Test
    void testDocumentAgg() {
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("address", "青海");
        MaxAggregationBuilder maxAggregationBuilder = new MaxAggregationBuilder("maxAge").field("age");
        AvgAggregationBuilder avgAggregationBuilder = new AvgAggregationBuilder("avgAge").field("age");
        NativeSearchQuery query = new NativeSearchQueryBuilder().withQuery(matchQueryBuilder)
                .withAggregations(maxAggregationBuilder).withAggregations(avgAggregationBuilder).build();
        SearchHits<User> searchHits = elasticsearchRestTemplate.search(query, User.class);
        if (searchHits.hasAggregations()) {
            ElasticsearchAggregations elasticsearchAggregations = (ElasticsearchAggregations) searchHits.getAggregations();
            List<Aggregation> aggregations = elasticsearchAggregations.aggregations().asList();
            aggregations.forEach(aggregation -> {
                if (aggregation instanceof Max) {
                    Max max = (Max) aggregation;
                    System.out.println(max.getName() + " : " + max.getValue());
                }
                if (aggregation instanceof Avg) {
                    Avg avg = (Avg) aggregation;
                    System.out.println(avg.getName() + " : " + avg.getValue());
                }
            });
        }
    }

}
