package com.lm.beam.kafka;


import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Instant;
import org.joda.time.Duration;
import org.junit.Test;

/**
 * @Classname WordCount
 * @Description TODO
 * @Date 2019/11/7 18:20
 * @Created by limeng
 */
public class WordCount {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        // 显式指定PipelineRunner：DirectRunner（Local模式）
        options.setRunner(DirectRunner.class);
        Pipeline pipeline = Pipeline.create(options);

        PCollection<KafkaRecord<String, String>> kafkas = pipeline.apply(KafkaIO.<String, String>read()
                .withBootstrapServers("")
                .withTopic("")
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withLogAppendTime());



        PCollection windowedProductIds = kafkas.apply(Window
                .into(SlidingWindows
                        .of(Duration.standardSeconds(10))));


        pipeline.run().waitUntilFinish();
    }

    @Test
    public void jdbcToEs(){
        PipelineOptions options = PipelineOptionsFactory.create();
        // 显式指定PipelineRunner：DirectRunner（Local模式）
        options.setRunner(DirectRunner.class);
        Pipeline pipeline = Pipeline.create(options);




    }



}
