package com.lm.beam.file;

import com.lm.beam.sql.model.IndexerPipelineOptions;
import com.lm.beam.sql.model.WordCountOptions;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import java.beans.PropertyVetoException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.values.*;
import org.apache.hadoop.conf.Configuration;

/**
 * @Author: limeng
 * @Date: 2019/7/23 19:39
 */
public class FileBeam {
    public static void main(String[] args) {

        IndexerPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(IndexerPipelineOptions.class);
        options.setRunner(DirectRunner.class);
        options.setInputFile("hdfs://192.168.20.117:8020/testsql.txt");

        PipelineOptionsFactory.register(IndexerPipelineOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        //startDocumentImportPipeline(options);

       startDocumentImportPipeline2(pipeline);
        pipeline.run();

    }

    private static class PrintFn<T> extends DoFn<T, T> {
        @DoFn.ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            System.out.println(c.element().toString());
        }
    }

    public static void startDocumentImportPipeline(IndexerPipelineOptions pipelineOptions) {
        WordCountOptions options = PipelineOptionsFactory.create().as(WordCountOptions.class);
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://192.168.20.117:8020");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        List<Configuration> list = new ArrayList<Configuration>();
        list.add(conf);
        options.setHdfsConfiguration(list);
        Pipeline p = Pipeline.create(options);
        PCollection<String> readLines = p.apply("ReadLines", TextIO.read().from(pipelineOptions.getInputFile())).apply(ParDo.of(new PrintFn<String>()));
        Pipeline pipeline = readLines.getPipeline();
        pipeline.apply("ReadLines", TextIO.read().from(pipelineOptions.getInputFile())).apply(ParDo.of(new PrintFn<String>()));

        System.out.println("cccccccccccccc");

        p.run();

    }


    public static void startDocumentImportPipeline2(Pipeline pipeline) {

        pipeline.apply("ReadLines", TextIO.read().from("D:\\beam.txt"))
                .apply(ParDo.of(new DoFn<String, String>() {

                    @ProcessElement
                    public void processElement(ProcessContext c) throws Exception {
                        String element = c.element();
                        System.out.println(element);
                        PipelineOptions pipelineOptions = c.getPipelineOptions();
                        Pipeline p = Pipeline.create(pipelineOptions);
                        save(p,element);
                        p.run();

                        //save(pipeline,element);
                       // c.output(startsWithATag,c.element());
                    }
                }));
    }

    public static void save(Pipeline pipeline,String sql){
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        try {
            cpds.setDriverClass("com.mysql.jdbc.Driver");
        } catch (PropertyVetoException e) {
            e.printStackTrace();
        }
        cpds.setJdbcUrl("jdbc:mysql://192.168.20.115:3306/test");
        cpds.setUser("root");
        cpds.setPassword("root");

        System.out.println("22222"+sql);
        Schema type =
                Schema.builder().addStringField("sass").build();
        Row build = Row.withSchema(type).addValue("123").build();

        pipeline
                .apply(Create.of(build))
                .apply(
                        JdbcIO.<Row>write()
                                .withDataSourceConfiguration(
                                        JdbcIO.DataSourceConfiguration.create(
                                                cpds))
                                .withStatement(sql)
                                .withPreparedStatementSetter(
                                        (element, statement) -> {

                                        })
                );

    }



}
