package com.lm.beam.sql;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

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

    @Test
    public void save(){
        PipelineOptions options = PipelineOptionsFactory.create();

        options.setRunner(DirectRunner.class);

        Pipeline pipeline = Pipeline.create(options);

        List<KV<String, String>> kvs = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String id="id:"+i;
            String name="name:"+i;
            kvs.add(KV.of(id,name));
        }

//        pipeline.apply(Create.of(kvs)).setCoder(KvCoder.of(StringUtf8Coder.of(),StringUtf8Coder.of()))
//                .apply(Filter.by( (KV<String, String> kv) ->  kv.getKey() == "id:1"))
//                .apply(JdbcIO.<KV<String, String>>write().withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create("数据连接池")).withStatement("sql")
//                        .withPreparedStatementSetter((element, statement) -> {
//                            statement.setString(1,element.getKey());
//                            statement.setString(2,element.getValue());
//                        }));

        pipeline.run().waitUntilFinish();

    }


    @Test
    public void testReadByDb() throws Exception {
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass("com.mysql.jdbc.Driver");
        cpds.setJdbcUrl("jdbc:mysql://192.168.20.115:3306/kd_swap?useSSL=false");
        cpds.setUser("root");
        cpds.setPassword("root");


        Connection connection = cpds.getConnection();
        PreparedStatement ps = connection.prepareStatement("show databases");
        ResultSet resultSet = ps.executeQuery();

        while (resultSet.next()){
            String database = resultSet.getString("Database");
            System.out.println("database:"+database);
        }
        resultSet.close();
        ps.close();
        connection.close();

    }


    @Test
    public void testsaveByDb() throws Exception {
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass("com.mysql.jdbc.Driver");
        cpds.setJdbcUrl("jdbc:mysql://192.168.200.115:3306/kd_test?useSSL=false");
        cpds.setUser("root");
        cpds.setPassword("root");

        String sql="insert into test2(id,`name`,version) values(?,?,?)";

        Connection connection = cpds.getConnection();
        connection.setAutoCommit(false);
        PreparedStatement ps = connection.prepareStatement(sql);
        for (int i = 0; i < 10000; i++) {

            ps.setString(1,"id"+i);
            ps.setString(2,"name"+i);
            ps.setString(3,"version"+i);
            ps.addBatch();
            if(i!=0 && i % 1000 == 0){
                ps.executeBatch();
                connection.commit();
                ps.clearBatch();
            }
        }
        ps.executeBatch();
        connection.commit();
        ps.close();
        connection.close();

    }

    @Test
    public void test1(){
        int a=2000;
        System.out.println(a&(1000-1));
        System.out.println(3000&(1000-1));
        System.out.println(4000&(1000-1));
        System.out.println(100&(1000-1));
        System.out.println(99&(1000-1));

    }


}
