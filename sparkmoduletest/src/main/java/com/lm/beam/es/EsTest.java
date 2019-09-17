package com.lm.beam.es;

import com.fasterxml.jackson.databind.JsonNode;
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
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.joda.time.Duration;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.*;


/**
 * @Author: limeng
 * @Date: 2019/9/4 14:46
 */
public class EsTest implements Serializable {
    @Test
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

        String ip="192.168.100.102";
        int port=9210;
        RestClient restClient = RestClient.builder(new HttpHost(ip, port)).setMaxRetryTimeoutMillis(6000).build();
        String query = "{\"query\": { \"match_all\": {} }}";
        HttpEntity queryEntity = new NStringEntity(query, ContentType.APPLICATION_JSON);
        /**
         * kd-test
         * my-type
         */
        String endPoint =
                String.format(
                        "/%s/%s/_search",
                        "kg.group_20190907",
                        "_doc");
        Map<String, String> params = new HashMap<>();
        Response response = restClient.performRequest("GET", endPoint, params, queryEntity);
        InputStream content = response.getEntity().getContent();
        stringStream(content);

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

        pipeline.apply(Create.of(sql)).apply(ElasticsearchIO.write().withConnectionConfiguration( ElasticsearchIO
                .ConnectionConfiguration.create(new String[]{"http://192.168.100.102:9210"}, "kd-test", "my-type")));
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
    public void createDocument() throws Exception {
        RestClient restClient = RestClient.builder(
                new HttpHost("192.168.100.102", 9210, "http")).build();

        String method = "POST";
        String endpoint = "/kd-test/my-type/_bulk";
        String sql="{\"id\":3,\"username\":\"测试测试\",\"description\":\"测试测试\"}";
        // JSON格式字符串
        HttpEntity entity = new NStringEntity(sql, ContentType.APPLICATION_JSON);
        Response response = restClient.performRequest(method, endpoint, Collections.emptyMap(), entity);
        System.out.println(EntityUtils.toString(response.getEntity()));
        // 返回结果：
        // {"_index":"book","_type":"novel","_id":"1","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":0,"_primary_term":1}
    }


    public static class PrintFn<String> extends DoFn<String, String> {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {

            System.out.println(c.element().toString());
            c.output(c.element());
        }
    }


}
