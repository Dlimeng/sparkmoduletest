package com.lm.beam.sql;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;

import java.beans.PropertyVetoException;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @Author: limeng
 * @Date: 2019/8/26 18:41
 */
public class MysqlSelect {
    public static void main(String[] args) throws PropertyVetoException {
        String driverClass="com.mysql.jdbc.Driver";
        PipelineOptions options = PipelineOptionsFactory.create();

        options.setRunner(DirectRunner.class);

        Pipeline pipeline = Pipeline.create(options);
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass(driverClass);
        cpds.setJdbcUrl("jdbc:mysql://192.168.20.115:3306/test?useSSL=false");
        cpds.setUser("root");
        cpds.setPassword("root");
        Schema type = Schema.builder().addStringField("name").addInt32Field("id").build();


        JdbcIO.Read<Row> rowRead = JdbcIO.<Row>read().
                withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                        .create(cpds)

                ).withCoder(SchemaCoder.of(type))
                .withQuery("select * from t")
                .withRowMapper(new JdbcIO.RowMapper<Row>() {
                    @Override
                    public Row mapRow(ResultSet resultSet) throws SQLException {

                        //映射
                        Row resultSetRow = null;
                        Schema build = Schema.builder().addStringField("name").addInt32Field("id").build();
                        resultSetRow = Row.withSchema(build).addValue(resultSet.getString(1)).addValue(resultSet.getInt(2)).build();
                        return resultSetRow;
                    }
                });



        pipeline.apply(rowRead);


        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testRead(){

    }
}
