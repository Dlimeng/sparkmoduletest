package com.lm.beam.sql;

import com.lm.beam.model.IndexerPipelineOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/**
 * 应用测试
 * @Author: limeng
 * @Date: 2019/8/1 14:01
 */
public class ApplicationTest {
    public static void main(String[] args) {
        IndexerPipelineOptions indexerPipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(IndexerPipelineOptions.class);
        indexerPipelineOptions.setRunner(DirectRunner.class);

//        Operate operate = new Operate();
//        operate.setSelect("test1");
//        operate.setDelete("test2");
//        indexerPipelineOptions.setOperate(operate);
        Pipeline pipeline = Pipeline.create(indexerPipelineOptions);

        System.out.println(indexerPipelineOptions);
        pipeline.run().waitUntilFinish();
    }
}
