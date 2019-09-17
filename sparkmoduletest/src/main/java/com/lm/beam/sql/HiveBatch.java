package com.lm.beam.sql;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;

import java.beans.PropertyVetoException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * hive批量操作
 * @Author: limeng
 * @Date: 2019/9/3 17:10
 */
public class HiveBatch {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    public static void main(String[] args) throws PropertyVetoException, SQLException {
        PipelineOptions options = PipelineOptionsFactory.create();
        // 显式指定PipelineRunner：DirectRunner（Local模式）
        options.setRunner(DirectRunner.class);
        Pipeline pipeline = Pipeline.create(options);
        ComboPooledDataSource cpds1 = new ComboPooledDataSource();
        cpds1.setDriverClass(driverName);
        cpds1.setJdbcUrl("jdbc:hive2://192.168.20.117:10000/default");
        cpds1.setUser("hive");
        cpds1.setPassword("hive");



        Schema type = Schema.builder().addStringField("name").addStringField("id").build();
        String sql = "insert into a_mart.test values(\"111\",\"1\")";
        String sql2 = "insert into a_mart.test values(\"222\",\"1\")";
        String sql3 = "insert into a_mart.test values(\"333\",\"1\")";
        List<String> strings = Collections.singletonList(sql2);
        PBegin begin = pipeline.begin();



        PDone apply = pipeline.apply(Create.empty(StringUtf8Coder.of())).apply(JdbcIO.<String>write().withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(cpds1)).withStatement("")
                .withPreparedStatementSetter((element, statement) -> {
                    statement.executeUpdate();
                }));

        JdbcIO.Write<String> stringWrite = JdbcIO.<String>write().withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(cpds1)).withStatement("")
                .withPreparedStatementSetter((element, statement) -> {
                    statement.executeUpdate();
                });



        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testWrite() throws PropertyVetoException {
        PipelineOptions options = PipelineOptionsFactory.create();
        // 显式指定PipelineRunner：DirectRunner（Local模式）
        options.setRunner(DirectRunner.class);
        Pipeline pipeline = Pipeline.create(options);

        ComboPooledDataSource cpds1 = new ComboPooledDataSource();
        cpds1.setDriverClass(driverName);
        cpds1.setJdbcUrl("jdbc:hive2://192.168.20.117:10000/default");
        cpds1.setUser("hive");
        cpds1.setPassword("hive");

        String sql="insert into test2019(name,id) values(?,?)";

        Schema type = Schema.builder().addStringField("name").addStringField("id").build();
        List<Row> rows=new ArrayList<>();
        rows.add(Row.withSchema(type).addValue("name3").addValue("id3").build());
        rows.add(Row.withSchema(type).addValue("name4").addValue("id4").build());
        pipeline.apply(Create.of(rows)).apply(JdbcIO.<Row>write()
                .withDataSourceConfiguration(
                        JdbcIO.DataSourceConfiguration.create(
                                cpds1).withConnectionProperties("hive"))
                .withStatement(sql)
                .withPreparedStatementSetter(
                        (element, statement) -> {
                            List<Object> values = element.getValues();
                            int fieldCount = element.getFieldCount();
                            if(fieldCount > 0){
                                for (int i = 0; i < values.size(); i++) {
                                    statement.setObject(i+1,values.get(i));
                                }
                            }else{
                                System.out.println("sql values is null");
                            }
                            int i = statement.executeUpdate();

                        }).withBatchSize(1024L));


        pipeline.run().waitUntilFinish();
    }

}
