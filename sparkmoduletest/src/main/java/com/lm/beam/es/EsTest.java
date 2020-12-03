package com.lm.beam.es;

import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Maps;
import com.lm.beam.es.model.Demo;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.indices.IndicesExists;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;
import org.mortbay.jetty.HttpStatus;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @Author: limeng
 * @Date: 2019/9/4 14:46
 */
public class EsTest implements Serializable {

    public void testRead(){
        PipelineOptions options = PipelineOptionsFactory.create();
        // 显式指定PipelineRunner：DirectRunner（Local模式）
        options.setRunner(DirectRunner.class);
        Pipeline pipeline = Pipeline.create(options);
        ElasticsearchIO.ConnectionConfiguration connectionConfiguration = ElasticsearchIO.ConnectionConfiguration.create(new String[]{"http://192.168.100.102:9210","http://192.168.100.103:9210","http://192.168.100.104:9210"}, "kg.group_20190907", "_doc");
        String query = "{\"query\": { \"match\": {\"source_id\":\"D831CF4582E5D85C5D3BEB9D072CF356\"} }}";
        PCollection<String> sss = pipeline.apply(ElasticsearchIO.read().withConnectionConfiguration(connectionConfiguration).withQuery(query));
        sss.apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(10)))).apply(ParDo.of(new PrintFn<String>()));



        pipeline.run();
    }

    @Test
    public void testRead2() throws IOException {

        RestClientBuilder builder = RestClient.builder(HttpHost.create("http://192.168.200.18:9212"));
        builder.setHttpClientConfigCallback(httpClientBuilder -> {
            return httpClientBuilder.setDefaultIOReactorConfig(IOReactorConfig.custom().setIoThreadCount(1).build());
        });
        RestClient restClient = builder.build();

        List<String> indices = new ArrayList<>();
        Request request = new Request("GET", "_cat/indices");
        request.addParameter("format", "JSON");
        Response response = restClient.performRequest(request);
        System.out.println();

//        String ip="192.168.100.102";
//        int port=9210;
//        RestClient restClient = RestClient.builder(new HttpHost(ip, port)).setMaxRetryTimeoutMillis(6000).build();
//        String query = "{\"query\": { \"match_all\": {} }}";
//        HttpEntity queryEntity = new NStringEntity(query, ContentType.APPLICATION_JSON);
//        /**
//         * kd-test
//         * my-type
//         */
//        String endPoint =
//                String.format(
//                        "/%s/%s/_search",
//                        "kg.group_20190907",
//                        "_doc");
//        Map<String, String> params = new HashMap<>();
//        Response response = restClient.performRequest("GET", endPoint, params, queryEntity);
        InputStream content = response.getEntity().getContent();
        stringStream(content);

    }
    @Test
    public void testRead3() throws IOException {
        JestClientFactory factory = new JestClientFactory();
        HttpClientConfig.Builder httpClientConfig = new HttpClientConfig
                .Builder("http://192.168.200.18:9212")
                .setPreemptiveAuth(new HttpHost("http://192.168.200.18:9212"))
                .connTimeout(30000)
                .maxTotalConnection(200)
                .discoveryFrequency(5L, TimeUnit.MINUTES);
        httpClientConfig.setPreemptiveAuth(new HttpHost("http://192.168.200.18:9212"));

        factory.setHttpClientConfig(httpClientConfig.build());
        JestClient jestClient = factory.getObject();
        JestResult rst = jestClient.execute(new IndicesExists.Builder("kg.group_v3_20201203").build());
        System.out.println(rst.getResponseCode());
    }


    public void stringStream(InputStream content) throws IOException {
        //读一个字节数组，一般是1024大小
        int len = 0 ;
        byte[] bys = new byte[1024];
        while ((len = content.read(bys)) != -1) {
            System.out.println(new String(bys,0,len));
            System.out.println("ccc");
        }
        content.close();
    }

    @Test
    public void testWrite(){
        PipelineOptions options = PipelineOptionsFactory.create();
        // 显式指定PipelineRunner：DirectRunner（Local模式）
        options.setRunner(DirectRunner.class);
        Pipeline pipeline = Pipeline.create(options);



        String sql="{\"ids\":\"3\",\"username\":\"测试测试2\",\"tag_list\":[{\"score\": 600,\"title\": \"title6\"},{\"score\": 500,\"title\": \"title5\"}]}";
      // String value="[{\"score\": 600,\"title\": \"title6\"},{\"score\": 500,\"title\": \"title5\"}";

        pipeline.apply(Create.of(sql)).apply(ElasticsearchIO.write().withConnectionConfiguration( ElasticsearchIO
                .ConnectionConfiguration.create(new String[]{"http://192.168.20.118:9200"}, "kd-test", "_doc")).withIdFn(new FieldValueExtractFnID()));

        pipeline.run().waitUntilFinish();
    }

    public static class FieldValueName implements ElasticsearchIO.Write.FieldValueExtractFn {
        private String fileName;

        public FieldValueName(String fileName) {
            this.fileName = fileName;
        }

        @Override
        public String apply(JsonNode input) {
            return input.path(fileName).asText();
        }
    }

    /**
     * 新增文档
     *
     * @throws Exception
     */
    @Test
    public void createDocument2() throws Exception {
//        RestClient restClient = RestClient.builder(
//                new HttpHost("192.168.20.118", 9200, "http")).build();
//
//        String method = "POST";
//        String endpoint = "/kd-test/_doc/_bulk";
//        String sql="{\"id\":4,\"username\":\"测试测试\",\"description\":\"测试测试\"}\\r\\n{\"id\":5,\"username\":\"测试测试\",\"description\":\"测试测试\"}\\r\\n";
//        // JSON格式字符串
//        HttpEntity entity = new NStringEntity(sql, ContentType.APPLICATION_JSON);
//        Request request = new Request(method, endpoint);
//        request.setEntity(entity);
//        Response response = restClient.performRequest(request);
//        //Response response = restClient.performRequest(method, endpoint, Collections.emptyMap(), entity);
//        System.out.println(EntityUtils.toString(response.getEntity()));
        // 返回结果：
        // {"_index":"book","_type":"novel","_id":"1","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":0,"_primary_term":1}
    }
    @Test
    public void delete(){
        String s="--";
        String name="--";
        int length = name.trim().length();
        System.out.println(length);
    }

    @Test
    public void save(){
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("192.168.200.118", 9210, "http")));

        IndexRequest request = new IndexRequest("sstest", "_doc","c2");
        final Demo demo = new Demo();
        demo.setId("id2");
        demo.setMony(200.22);
        String json = JSON.toJSONString(demo);
        request.source(json,XContentType.JSON);
        try {
            client.index(request, new Header[0]);
        } catch (IOException var5) {
           var5.printStackTrace();
        }
    }
    @Test
    public  void querySum(){
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("192.168.200.118", 9210, "http")));
        SearchRequest searchRequest = new SearchRequest(new String[]{"sstest"});
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();


        SumAggregationBuilder fa = AggregationBuilders.sum("mony").field("mony");


        searchSourceBuilder.aggregation(fa);

        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(searchRequest);
            Map<String, Aggregation> subaggmap  = searchResponse.getAggregations().asMap();
           final double mony = ((Sum) subaggmap.get("mony")).getValue();

            System.out.println(mony);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void queryList(){

        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("192.168.200.18", 9210, "http")));
        SearchRequest searchRequest = new SearchRequest("chain.agency_label");
        searchRequest.types("_doc");
        SearchSourceBuilder searchSourceBuilder =  new SearchSourceBuilder();
        //查询条件，可以参考官网手册
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        //1581692414897
        //
       // searchSourceBuilder.sort(SortBuilders.fieldSort("total_num").order(SortOrder.DESC));
//        searchSourceBuilder.sort("total_num", SortOrder.DESC);
//        boolQuery.must(QueryBuilders.termQuery("agency_id", "9A99797A57EF48F1A3ACA610E1E91BD3"));

        RangeQueryBuilder startTime = QueryBuilders.rangeQuery("start_time").from(1547481600000L).to(1547481600000L).includeLower(true).includeUpper(true);
        boolQuery.filter(startTime);
        searchSourceBuilder.size(10);
        searchSourceBuilder.from(0);
        searchSourceBuilder.query(boolQuery);

        searchRequest.source(searchSourceBuilder);
        try {
            //查询结果
            SearchResponse searchResponse = client.search(searchRequest);
            SearchHits hits = searchResponse.getHits();
            SearchHit[] searchHits = hits.getHits();
            for(SearchHit hit : searchHits) {
                System.out.println(hit.getSourceAsString());
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Test
    public void queryList2() throws IOException {

        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("192.168.200.18", 9210, "http")));
        SearchRequest searchRequest = new SearchRequest("chain.agency_label");
        searchRequest.types("_doc");
        SearchSourceBuilder searchSourceBuilder =  new SearchSourceBuilder();

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        //{
        //  "range" : {
        //    "start_time" : {
        //      "from" : 1547481600000,
        //      "to" : 1547481600000,
        //      "include_lower" : true,
        //      "include_upper" : true,
        //      "boost" : 1.0
        //    }
        //  }
        //}
        RangeQueryBuilder startTime = QueryBuilders.rangeQuery("start_time").from(1547481600000L).to(1547481600000L).includeLower(true).includeUpper(true);

        boolQuery.must(QueryBuilders.termQuery("agency_id", "98da1f52422d42e1b39dc7737bb60da4"));
        searchSourceBuilder.size(10);
        searchSourceBuilder.from(0);
        searchSourceBuilder.query(boolQuery);
        final SumAggregationBuilder fa = AggregationBuilders.sum("fa").field("financing_amount");
        searchSourceBuilder.aggregation(fa);

        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest);
        InternalSum sum= searchResponse.getAggregations().get("fa");

        System.out.println( sum.getValue());
    }

    @Test
    public void groupByList() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("192.168.200.118", 9210, "http")));
        SearchRequest searchRequest = new SearchRequest("chain.agency_event");
        searchRequest.types("_doc");
        SearchSourceBuilder searchSourceBuilder =  new SearchSourceBuilder();

        AggregationBuilder aggregationBuilder = ((TermsAggregationBuilder)AggregationBuilders.terms("agency_id").field("agency_id")).order(BucketOrder.count(false)).size(100);
        searchSourceBuilder.aggregation(aggregationBuilder);
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(100);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, new Header[0]);

        Map<String, Aggregation> aggMap = searchResponse.getAggregations().asMap();
        if (aggMap != null && aggMap.size() != 0) {
            Map<String, Long> groupMap = Maps.newHashMapWithExpectedSize(aggMap.size());
            ParsedStringTerms fieldAgg = (ParsedStringTerms)aggMap.get("agency_id");

            fieldAgg.getBuckets().stream().forEach((d) -> {
                Long var10000 = (Long)groupMap.put(d.getKeyAsString(), d.getDocCount());
            });
            Assert.assertNotNull(groupMap);
        }

    }

    public void createDocument()  throws Exception {
       //192.168.100.102
//
//        RestClient restClient = RestClient.builder(
//                new HttpHost("192.168.100.102", 9210, "http")).build();
//        String method = "POST";
//        String endpoint = "/kd-test/my-type/_bulk";
//        String sql="{\"id\":3,\"username\":\"测试测试\",\"description\":\"测试测试\"}";
//        // JSON格式字符串
//        HttpEntity entity = new NStringEntity(sql, ContentType.APPLICATION_JSON);
//        Response response = restClient.performRequest(method, endpoint, Collections.emptyMap(), entity);
//        System.out.println(EntityUtils.toString(response.getEntity()));
    }


    public static class PrintFn<String> extends DoFn<String, String> {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {

            System.out.println(c.element().toString());
            c.output(c.element());
        }
    }
    public static class FieldValueExtractFnID implements ElasticsearchIO.Write.FieldValueExtractFn{
        @Override
        public String apply(JsonNode input) {
            return input.path("ids").asText();
        }
    }

}
