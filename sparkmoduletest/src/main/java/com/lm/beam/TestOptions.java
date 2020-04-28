package com.lm.beam;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.lm.beam.model.IndexerPipelineOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

/**
 * @Author: limeng
 * @Date: 2019/8/23 16:13
 */
public class TestOptions {
    public static void main(String[] args) {
        IndexerPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(IndexerPipelineOptions.class);
//        List<String> filesToStage = options.getFilesToStage();
        options.setRunner(DirectRunner.class);
       // System.out.println(filesToStage);
        Pipeline pipeline = Pipeline.create(options);

        HashMap<String,String> map = JSON.parseObject(options.getMapPar(), new TypeReference<HashMap<String,String>>() {});
        System.out.println(map);
        //        List<String> list=new ArrayList<>();
//        for (int i=0;i<100;i++){
//            list.add(String.valueOf(i));
//        }
//        pipeline.apply(Create.of(list)).setCoder(StringUtf8Coder.of()).apply(ParDo.of(new testDoFn("cc")));



        pipeline.run().waitUntilFinish();
    }


    static class testDoFn extends DoFn<Object, Object>{

        private static final long serialVersionUID = 4110441612932553294L;
        private String status;
        private  List<String> list=null;
        private  Date start=null;
        private  Date end=null;

        public testDoFn(String status) {
            this.status = status;
        }
        @Setup
        public void setUp(){
            System.out.println("ccccccccccccccccccccc");
        }
        @StartBundle
        public void startBundle() throws Exception {
            System.out.println("startBundle");
            list = new ArrayList<>();
            start = new Date();
        }

        @ProcessElement
        public void processElement(ProcessContext c){
            Object element = c.element();
            list.add(element.toString());
            System.out.println(element.toString());
            c.output(element);
        }

        @FinishBundle
        public void finishBundle(){
            System.out.println("finishBundle");
            list.clear();
            end = new Date();
            System.out.println("end.getTime() "+end.getTime());
            System.out.println("start.getTime() "+start.getTime());
            long l = (end.getTime() - start.getTime()) / 1000;
            System.out.println("时间间隔："+l);
        }
    }
    @Test
    public void testTime(){
//        String time="2019-09-12 16:25:39.0";
//        String[] split = time.split("\\.|\\,");
//
//        int i = time.indexOf("\\.|\\,");
//        System.out.println(i);
//        String format="yyyy-MM-dd HH:mm:ss";
//        Instant parse = Instant.parse(split[0], DateTimeFormat.forPattern(format));

        String token="select * from dwt_mart.company_tmp limit 100";

        String[] split = token.trim().split("\\s+");
        Assert.assertNotNull(split);

    }



}
