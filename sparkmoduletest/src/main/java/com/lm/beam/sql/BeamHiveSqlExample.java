package com.lm.beam.sql;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.hcatalog.HCatalogIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hive.hcatalog.data.HCatRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author: limeng
 * @Date: 2019/7/10 16:50
 */
public class BeamHiveSqlExample {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        // 显式指定PipelineRunner：DirectRunner（Local模式）
        options.setRunner(DirectRunner.class);

        Pipeline pipeline = Pipeline.create(options);

        Map<String, String> configProperties = new HashMap<String, String>();
        configProperties.put("hive.metastore.uris","thrift://192.168.20.117:9083");
        pipeline.apply(HCatalogIO.read()
                        .withConfigProperties(configProperties)
                        .withDatabase("a_mart")
                        .withTable("test")
                );


        pipeline.run().waitUntilFinish();
    }
}
