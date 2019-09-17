package com.lm.beam.sql;
import com.lm.beam.sql.model.TestRow;
import com.lm.beam.sql.util.MyDataSource;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.hcatalog.HCatalogIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.beans.PropertyVetoException;
import java.io.FileNotFoundException;
import java.sql.*;
import java.util.Collections;
import java.util.List;
import java.util.Map;


/**
 * @Author: limeng
 * @Date: 2019/7/11 13:45
 */
public class HiveCreateDb {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    public static void main(String[] args) throws ClassNotFoundException, FileNotFoundException, SQLException, PropertyVetoException {
        PipelineOptions options = PipelineOptionsFactory.create();
        // 显式指定PipelineRunner：DirectRunner（Local模式）
        options.setRunner(DirectRunner.class);
        //PipelineOptionsFactory.fromArgs(args).withValidation().as(IndexerPipelineOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass(driverName);
        cpds.setJdbcUrl("jdbc:hive2://192.168.20.117:10000/a_mart");
        cpds.setUser("hive");
        cpds.setPassword("hive");

        String sql = "insert into a_mart.test values(\"444\",\"text\")";
        List<String> strings = Collections.singletonList(sql);
        PCollection<String> s2 = pipeline.apply(Create.of(strings)).setCoder(StringUtf8Coder.of());

        s2.apply(ParDo.of(new DoFn<String, Object>() {
            @ProcessElement
            public void processElement(ProcessContext c) throws Exception {
                String sql = c.element();


            }
        }));


        s2.apply(JdbcIO.<String>write()
                .withDataSourceConfiguration(
                        JdbcIO.DataSourceConfiguration.create(
                                cpds))
                .withStatement(sql)
                .withPreparedStatementSetter(
                        (element, statement) -> {
                            statement.executeUpdate();
                            System.out.println("ccc");
                        }));

        Schema type = Schema.builder().addStringField("id").addStringField("version").build();
        pipeline.apply(Create.empty(SchemaCoder.of(type))).apply(JdbcIO.<Row>write()
                .withDataSourceConfiguration(
                        JdbcIO.DataSourceConfiguration.create(
                                cpds))
                .withStatement(sql)
                .withPreparedStatementSetter(
                        (element, statement) -> {
                            statement.executeUpdate();
                        }));

        pipeline.run().waitUntilFinish();
    }


}
