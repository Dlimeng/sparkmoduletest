package com.lm.beam.sql;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.hive.jdbc.HivePreparedStatement;

import java.beans.PropertyVetoException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: limeng
 * @Date: 2019/7/20 20:09
 */
public class HiveSave {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    public static void main(String[] args) throws PropertyVetoException, SQLException {
        PipelineOptions options = PipelineOptionsFactory.create();
        // 显式指定PipelineRunner：DirectRunner（Local模式）
        options.setRunner(DirectRunner.class);
        //PipelineOptionsFactory.fromArgs(args).withValidation().as(IndexerPipelineOptions.class);

        Pipeline pipeline = Pipeline.create(options);

//        ComboPooledDataSource cpds = new ComboPooledDataSource();
//        cpds.setDriverClass("com.mysql.jdbc.Driver");
//        cpds.setJdbcUrl("jdbc:mysql://192.168.20.115:3306/test");
//        cpds.setUser("root");
//        cpds.setPassword("root");

        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass(driverName);
        cpds.setJdbcUrl("jdbc:hive2://192.168.20.117:10000/a_mart");
        cpds.setUser("hive");
        cpds.setPassword("hive");

        String name="23213";
        Schema type =
                Schema.builder().addStringField("sass").build();
        Row build = Row.withSchema(type).addValue(name).build();

        pipeline
                .apply(Create.of(build))
                .apply(
                        JdbcIO.<Row>write()
                                .withDataSourceConfiguration(
                                        JdbcIO.DataSourceConfiguration.create(
                                              cpds))
                                .withStatement("insert into a_mart.test values(\"333\",\"text\")")
                                .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<Row>() {
                                    @Override
                                    public void setParameters(Row element, PreparedStatement preparedStatement) throws Exception {
                                        preparedStatement.executeUpdate();
                                    }
                                })
                );

        System.out.println("success");
        pipeline.run().waitUntilFinish();

    }

    static class PrepareStatementFromTestRow implements JdbcIO.PreparedStatementSetter<Row> {

        @Override
        public void setParameters(Row element, PreparedStatement preparedStatement) throws Exception {

        }
    }



    public void load() throws PropertyVetoException {
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

//        pipeline.apply(JdbcIO.<Row>read().
//                withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
//                        .create(cpds)
//
//                ).withCoder(SchemaCoder.of(type))
//                .withQuery("select * from test")
//                .withRowMapper(new JdbcIO.RowMapper<Row>() {
//                    @Override
//                    public Row mapRow(ResultSet resultSet) throws Exception {
//
//                        return build;
//                    }
//                })
//        );
    }
}
