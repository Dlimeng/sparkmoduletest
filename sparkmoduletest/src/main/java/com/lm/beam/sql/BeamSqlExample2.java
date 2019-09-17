package com.lm.beam.sql;

import com.lm.beam.sql.model.TestRow;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;

/**
 * @Author: limeng
 * @Date: 2019/7/10 13:51
 */
public class BeamSqlExample2 {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        // 显式指定PipelineRunner：DirectRunner（Local模式）
        options.setRunner(DirectRunner.class);

        Pipeline pipeline = Pipeline.create(options);

        Schema build = Schema.builder().addStringField("name").addStringField("id").build();
//        PCollection<TestRow> apply = pipeline.apply(JdbcIO.<TestRow>read().
//                withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
//                        .create("com.mysql.jdbc.Driver", "jdbc:mysql://192.168.20.115:3306/test")
//                        .withUsername("root").withPassword("root")
//
//                ).withQuery("select * from t").withCoder(SerializableCoder.of(TestRow.class))
//                .withRowMapper(new JdbcIO.RowMapper<TestRow>() {
//                    @Override
//                    public TestRow mapRow(ResultSet resultSet) throws Exception {
//                        String name = resultSet.getString(1);
//                        System.out.println(name);
//                        TestRow testRow = new TestRow();
//                        testRow.setName(name);
//                        return testRow;
//                    }
//                })
//        );


        PCollection<Map<String, String>> apply = pipeline.apply(JdbcIO.<Map<String, String>>read().
                withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                        .create("com.mysql.jdbc.Driver", "jdbc:mysql://192.168.20.115:3306/test")
                        .withUsername("root").withPassword("root")

                ).withQuery("select * from t").withCoder(MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
                .withRowMapper(new JdbcIO.RowMapper<Map<String, String>>() {
                    @Override
                    public Map<String, String> mapRow(ResultSet resultSet) throws Exception {
                        ResultSetMetaData metaData = resultSet.getMetaData();
                        int columnCount = metaData.getColumnCount();
                        for (int i = 0; i < columnCount; i++) {
                            String columnLabel = metaData.getColumnLabel(i+1);
                            System.out.println(columnLabel);
                        }
                        String name = resultSet.getString(1);
                        String id = resultSet.getString(1);
                        Map<String, String> map = new HashMap<>();
                        map.put("id", id);
                        map.put("name", name);
                        return map;
                    }
                })
        );

        apply.apply(ParDo.of(new PrintByMap()));
        pipeline.run().waitUntilFinish();

    }


    public static class  PrintByString extends DoFn<TestRow,TestRow> {
        private Logger logger = LoggerFactory.getLogger(this.getClass());
        @ProcessElement
        public void processElement( ProcessContext c) {
            TestRow element = c.element();
            logger.info(element.toString());
            c.output(element);
        }
    }

    public static class  PrintByMap extends DoFn<Map<String,String>,Void> {
        private Logger logger = LoggerFactory.getLogger(this.getClass());
        @ProcessElement
        public void processElement( ProcessContext c) {
            Map<String, String> element = c.element();
            Collection<String> values = element.values();
            Iterator<String> iterator = values.iterator();
            while (iterator.hasNext()){
                String next = iterator.next();
                System.out.println(next);
            }
        }
    }

    public static class  Print extends DoFn<Row,Row> {
        private Logger logger = LoggerFactory.getLogger(this.getClass());
        @ProcessElement
        public void processElement( ProcessContext c) {
            Row element = c.element();
            List<Object> values = element.getValues();
            StringBuilder stringBuilder = new StringBuilder();
            for (Object o:values){
                stringBuilder.append(o.toString()+" ");
            }
            logger.info(stringBuilder.toString());
            c.output(element);
        }
    }
}
