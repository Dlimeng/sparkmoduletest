package com.lm.beam.transform;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.junit.Test;

import java.io.Serializable;
import java.util.*;

/**
 * @Author: limeng
 * @Date: 2019/9/17 14:06
 */
public class GroupTest implements Serializable {
    @Test
    public void testStr(){
        String time="ccss";
        boolean contains = time.contains(".");
        System.out.println(contains);
    }

    @Test
    public void testGroup(){
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(DirectRunner.class);
        Pipeline pipeline = Pipeline.create(options);


        Schema build = Schema.builder().addStringField("id").addStringField("age").addStringField("name").build();

        KV<String, Row> k1 = KV.of("12", Row.withSchema(build).addValue("id1").addValue("12").addValue("name1").build());
        KV<String, Row> k2 = KV.of("12", Row.withSchema(build).addValue("id2").addValue("12").addValue("name2").build());
        KV<String, Row> k3 = KV.of("13", Row.withSchema(build).addValue("id3").addValue("13").addValue("name3").build());
        List<KV<String, Row>> list=new ArrayList<>();
        list.add(k1);
        list.add(k2);
        list.add(k3);

        PCollection<KV<String, Row>> kvs = pipeline.apply(Create.of(list)).setCoder(KvCoder.of(StringUtf8Coder.of(), SchemaCoder.of(build)));
        PCollection<KV<String, Set<Row>>> apply = kvs.apply(Combine.perKey(new UniqueSets()));
        apply.apply(ParDo.of(new Print2()));
//        PCollection<KV<String, Row>> kvs = pipeline.apply(Create.of(list)).setCoder(KvCoder.of(StringUtf8Coder.of(), SchemaCoder.of(build)));
//        PCollection<KV<String, Iterable<Row>>> as = kvs.apply(GroupByKey.create());

//        as.apply(ParDo.of(new Print()));
        pipeline.run().waitUntilFinish();
    }

    public static class Print extends DoFn<KV<String, Iterable<Row>>,KV<String, Iterable<Row>>>{
        @ProcessElement
        public void processElement( ProcessContext c) {
            KV<String, Iterable<Row>> element = c.element();
            String key = element.getKey();
            Iterable<Row> value = element.getValue();

            Iterator<Row> iterator = value.iterator();
            while (iterator.hasNext()){
                Row next = iterator.next();
                List<Object> values = next.getValues();
                for(Object o:values){
                    System.out.print(o.toString()+" ");
                }
                System.out.println(" key:"+key);
            }
            c.output(element);
        }
    }

    public static class Print2 extends DoFn<KV<String, Set<Row>>,KV<String, Set<Row>>>{
        @ProcessElement
        public void processElement( ProcessContext c) {
            KV<String, Set<Row>> element = c.element();
            String key = element.getKey();
            Set<Row> value = element.getValue();
            value.forEach(f->{
                String string = f.getString(0);
                System.out.println(key +" "+ string);
            });
            System.out.println("end");
            c.output(element);
        }
    }

    public static class Print3 extends DoFn<KV<String, Iterable<Map<String, String>>>,KV<String, Iterable<Map<String, String>>> >{
        @ProcessElement
        public void processElement( ProcessContext c) {
            KV<String, Iterable<Map<String, String>>> element = c.element();
            element.getKey();
            Iterator<Map<String, String>> iterator = element.getValue().iterator();
            while (iterator.hasNext()){
                Map<String, String> next = iterator.next();
                next.forEach((k,v)->{
                    System.out.print(k+" : "+v);
                });
            }
            System.out.println();
            c.output(element);
        }
    }

    public static class UniqueSets extends Combine.CombineFn<Row, Set<Row>, Set<Row>>{

        @Override
        public Set<Row> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public Set<Row> addInput(Set<Row> accumulator, Row input) {
            accumulator.add(input);
            return accumulator;
        }

        @Override
        public Set<Row> mergeAccumulators(Iterable<Set<Row>> accumulators) {
            Set<Row> all = new HashSet<>();
            for (Set<Row> part : accumulators) {
                all.addAll(part);
            }
            return all;
        }

        @Override
        public Set<Row> extractOutput(Set<Row> accumulator) {
            return accumulator;
        }
    }


}
