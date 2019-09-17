package com.lm.beam;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 测试流水线
 * @Author: limeng
 * @Date: 2019/9/4 15:31
 */
public class PipelineTest {
    @Test
    public void init() throws IOException, InterruptedException {
        PipelineOptions options = PipelineOptionsFactory.create();
        // 显式指定PipelineRunner：DirectRunner（Local模式）
        options.setRunner(DirectRunner.class);
        Pipeline pipeline = Pipeline.create(options);

        int size=10;
        PipelineOptions options1 = pipeline.getOptions();
        for (int i=0;i<size;i++){
            String name="index:"+i;
            this.read(options1,name);
        }

        pipeline.run().waitUntilFinish();
    }

    public void read(PipelineOptions options,String index) throws InterruptedException {
        Pipeline pipeline = Pipeline.create(options);
        List<String> list = new ArrayList<>();
        int size = 100;
        for (int i=0;i<size;i++){
            list.add(String.valueOf(i));
        }
        PCollection<String> stringPCollection = pipeline.apply(Create.of(list)).setName(index);
        stringPCollection.apply(ParDo.of(new Print()));
        System.out.println(stringPCollection.getName());

        pipeline.run();
    }

    static class Print extends DoFn<String,String> {
        @Setup
        public void setUp(){

        }

        @StartBundle
        public void startBundle(){

        }

        @ProcessElement
        public void processElement( ProcessContext c) {
            String element = c.element();
            c.output(element);
        }
    }
}
