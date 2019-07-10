package com.lm.beam.sql;

import com.lm.beam.sql.model.TestRow;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;

import java.sql.ResultSet;

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

        pipeline.apply(JdbcIO.<TestRow>read().
                withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                        .create("com.mysql.jdbc.Driver", "jdbc:mysql://192.168.20.115:3306/test")
                        .withUsername("root").withPassword("root")

                ).withQuery("select * from t").withCoder(SerializableCoder.of(TestRow.class))
                .withRowMapper(new JdbcIO.RowMapper<TestRow>() {
                    @Override
                    public TestRow mapRow(ResultSet resultSet) throws Exception {
                        String name = resultSet.getString(1);
                        System.out.println(name);
                        TestRow testRow = new TestRow();
                        testRow.setName(name);
                        return testRow;
                    }
                })
        );

        pipeline.run().waitUntilFinish();

    }
}
