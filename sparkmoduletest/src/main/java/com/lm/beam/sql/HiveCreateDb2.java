package com.lm.beam.sql;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.hcatalog.HCatalogIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;

/**
 * @Author: limeng
 * @Date: 2019/7/15 16:05
 */
public class HiveCreateDb2 {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(DirectRunner.class); // 显式指定PipelineRunner：DirectRunner（Local模式）

        Pipeline pipeline = Pipeline.create(options);
//        pipeline.apply(HCatalogIO.read()
//                .withConfigProperties())

    }
}
