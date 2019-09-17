package com.lm.beam.reflect;


import com.lm.beam.model.IndexerPipelineOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @Author: limeng
 * @Date: 2019/8/29 16:04
 */
public class IndexerTest {
    public static void main(String[] args) {
        IndexerPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(IndexerPipelineOptions.class);
        options.setPattern("p1");
        options.setSourceName("s1");
        options.setRunner(DirectRunner.class);
        Pipeline pipeline = Pipeline.create(options);
        //fieldListToMap(options);
        //fieldListToMap2(new Indexer());

        List<String> ls=new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            ls.add(String.valueOf(i));
        }

        PCollection<String> apply = pipeline.apply(Create.of(ls));
        Set<Map.Entry<TupleTag<?>, PValue>> entries = apply.expand().entrySet();
        for(Map.Entry<TupleTag<?>, PValue> e:entries){
            String id = e.getKey().getId();
            String s = e.getKey().toString();
            String name = e.getValue().getName();
            System.out.println(id);
            System.out.println(s);
            System.out.println(name);
            PValue value = e.getValue();
        }
        pipeline.run().waitUntilFinish();

    }


}
