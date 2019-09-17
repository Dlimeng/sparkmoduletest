package com.lm.beam.sql;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.values.*;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * 合并
 * @Author: limeng
 * @Date: 2019/9/1 21:42
 */
public class MergeTest {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(SparkPipelineOptions.class);
        options.setRunner(DirectRunner.class);
        Pipeline pipeline = Pipeline.create(options);

        Schema type = Schema.builder().addStringField("id").addStringField("version").build();
        List<Row> list1 =new ArrayList<>();
        List<Row> list2 =new ArrayList<>();

        list1.add(Row.withSchema(type).addValue("id1").addValue("v1").build());
        list1.add(Row.withSchema(type).addValue("id2").addValue("v2").build());

        list2.add(Row.withSchema(type).addValue("id22").addValue("v1").build());
        list2.add(Row.withSchema(type).addValue("id25").addValue("v5").build());

        PCollection<Row> l1 = pipeline.apply(Create.of(list1)).setCoder(SchemaCoder.of(type));
        PCollection<Row> l2 = pipeline.apply(Create.of(list2)).setCoder(SchemaCoder.of(type));
        l1.setName("l1");
        l2.setName("l2");

        PCollectionList<Row> p1 = PCollectionList.of(l1).and(l2);

        p1.apply(new generate("version"));

        PCollection<Row> apply = p1.apply(Flatten.<Row>pCollections());

        pipeline.run().waitUntilFinish();
    }
    //生成新的
   static class generate extends PTransform<PCollectionList<Row>,PCollection<Row>>{
        private final String version;

        public generate(String version) {
            this.version = version;
        }

        @Override
        public PCollection<Row> expand(PCollectionList<Row> input) {
            //旧版本
            PCollection<Row> p1 = null;
            //新版本
            PCollection<Row> p2 = null;
            //分组
            List<PCollection<Row>> all = input.getAll();
            for(PCollection<Row> p:all){
                if(p.getName().equals("l1")){
                    p1 = p;
                }else if(p.getName().equals("l2")){
                    p2 = p;
                }
            }

            //过滤
            //视图
            PCollectionView<Iterable<Row>> v2 = p2.apply(View.asIterable());
            //过滤出老版本数据加上修改完的数据
            PCollection<Row> f1=p1.apply(ParDo.of(new DoFn<Row, Row>() {
                @ProcessElement
                public void processElement(@Element Row row1, OutputReceiver<Row> out, ProcessContext c) {
                    Iterable<Row> rows = c.sideInput(v2);
                    Row element = c.element();
                    Iterator<Row> iterator = rows.iterator();
                    boolean status =true;
                    while (iterator.hasNext()){
                        Row next = iterator.next();
                        String version2 = next.getString(version);
                        String version1 = element.getString(version);
                        if(version1.equals(version2)){
                            //新版修改数据输出
                            out.output(next);
                            status = false;
                        }
                    }
                    if(status){
                        out.output(element);
                    }
                }
            }).withSideInputs(v2));

            //合并
            PCollection<Row> mergeds = PCollectionList.of(f1).and(p2).apply(Flatten.<Row>pCollections());

            //去重
            PCollection<Row> result = mergeds.apply(Distinct.withRepresentativeValueFn(new SerializableFunction<Row, String>() {
                @Override
                public String apply(Row input) {
                    return input.getString(version);
                }
            }).withRepresentativeType(TypeDescriptor.of(String.class)));

            result.apply(ParDo.of(new Print()));
            return result;
        }
    }


    static class Print extends DoFn<Row,Row>{
        @Setup
        public void setUp(){

        }

        @StartBundle
        public void startBundle(){

        }

        @ProcessElement
        public void processElement( ProcessContext c) {
            Row element = c.element();
            List<Object> values = element.getValues();
            StringBuilder stringBuilder = new StringBuilder();
            for (Object o:values){
                stringBuilder.append(o.toString()+" ");
            }
            System.out.println(stringBuilder.toString());
            c.output(element);
        }
    }

    static class DoFnTest<T> extends DoFn<T,T>{
        @Setup
        public void setUp(){
        }

        @StartBundle
        public void startBundle(){
        }

        @ProcessElement
        public void processElement( ProcessContext c) {

        }

        @FinishBundle
        public void finishBundle(){

        }
        @Teardown
        public void teardown(){

        }
    }

}
