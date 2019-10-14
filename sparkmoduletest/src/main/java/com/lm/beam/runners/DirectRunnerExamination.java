package com.lm.beam.runners;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;

import javax.sql.DataSource;
import java.beans.PropertyVetoException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 本地引擎
 * @Author: limeng
 * @Date: 2019/10/8 17:09
 */
public class DirectRunnerExamination {
    int inNum=100;
    @Test
    public void testDirectRunner(){
        long startTime = System.currentTimeMillis();
        DirectOptions options = PipelineOptionsFactory.fromArgs("").as(DirectOptions.class);
        options.setRunner(DirectRunner.class);

        //int processors =Runtime.getRuntime().availableProcessors() << 1;
        options.setTargetParallelism(30);

        Pipeline pipeline = Pipeline.create(options);

        List<KV<String, String>> list = new ArrayList<>();

        String sql="insert into batch_test(id,name) values(?,?)";

        for (int i = 1; i <= inNum; i++) {
            String id=String.valueOf(i);
            String name="name"+i;
            list.add(KV.of(id, name));
        }

        pipeline.apply(Create.of(list))
                .setCoder(KvCoder.of(StringUtf8Coder.of(),StringUtf8Coder.of()))
                .apply(JdbcIO.<KV<String,String>>write()
                .withDataSourceConfiguration(JdbcIO.
                        DataSourceConfiguration.
                        create(getDataSource()))
                .withStatement(sql).withBatchSize(2048L)
                .withPreparedStatementSetter(new PrepareStatementFromRow()));


        pipeline.run().waitUntilFinish();
        long endTime = System.currentTimeMillis();
        System.out.println("插入数据用时：" + (endTime - startTime) / 1000 + " 秒");
    }


    @Test
    public void testSparkRunner(){
        long startTime = System.currentTimeMillis();
        SparkPipelineOptions options = PipelineOptionsFactory.fromArgs("").as(SparkPipelineOptions.class);
        options.setRunner(SparkRunner.class);
        options.setSparkMaster("local[*]");
        /**
         * 默认local[*] 87
         * local[30]   72
         * 100L 248
         * 500L 114
         * 1000L 105
         * 1500L 95
         * Options BundleSize 和 io BatchSize 对应  88
         * 2500L 84
         * 3000L 82
         * 4096L 84
         */
        options.setBundleSize(4096L);
        Pipeline pipeline = Pipeline.create(options);

        List<KV<String, String>> list = new ArrayList<>();

        String sql="insert into batch_test(id,name) values(?,?)";

        for (int i = 1; i <= inNum; i++) {
            String id=String.valueOf(i);
            String name="name"+i;
            list.add(KV.of(id, name));
        }

        pipeline.apply(Create.of(list))
                .setCoder(KvCoder.of(StringUtf8Coder.of(),StringUtf8Coder.of()))
                .apply(JdbcIO.<KV<String,String>>write()
                        .withDataSourceConfiguration(JdbcIO.
                                DataSourceConfiguration.
                                create(getDataSource()))
                        .withStatement(sql).withBatchSize(2048L)
                        .withPreparedStatementSetter(new PrepareStatementFromRow()));


        pipeline.run().waitUntilFinish();
        long endTime = System.currentTimeMillis();
        System.out.println("插入数据用时：" + (endTime - startTime) / 1000 + " 秒");
    }


    private DataSource getDataSource()  {
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        try {
            cpds.setDriverClass("com.mysql.jdbc.Driver");
        } catch (PropertyVetoException e) {
            e.printStackTrace();
        }
        cpds.setJdbcUrl("jdbc:mysql://192.168.20.115:3306/test?useSSL=false");
        cpds.setUser("root");
        cpds.setPassword("root");
        return cpds;
    }

    public static class PrepareStatementFromRow implements JdbcIO.PreparedStatementSetter<KV<String,String>>{
        @Override
        public void setParameters(KV<String, String> element, PreparedStatement preparedStatement) throws Exception {
            preparedStatement.setString(1,element.getKey());
            preparedStatement.setString(2,element.getValue());
        }
    }

}
