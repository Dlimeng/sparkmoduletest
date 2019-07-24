package com.lm.beam.sql;
import com.lm.beam.sql.model.TestRow;
import com.lm.beam.sql.util.MyDataSource;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.beans.PropertyVetoException;
import java.io.FileNotFoundException;
import java.sql.*;
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


       // SerializableCoder.of(ds.getClass());
        ComboPooledDataSource cpds = new ComboPooledDataSource();
//        cpds.setDriverClass(driverName);
//        cpds.setJdbcUrl("jdbc:hive2://192.168.20.117:10000/a_mart");
//        cpds.setUser("hive");
//        cpds.setPassword("hive");

        cpds.setDriverClass("com.mysql.jdbc.Driver");
        cpds.setJdbcUrl("jdbc:mysql://192.168.20.115:3306/test");
        cpds.setUser("root");
        cpds.setPassword("root");

        Connection connection = cpds.getConnection();
        Integer vendorTypeNumber = JDBCType.VARCHAR.getVendorTypeNumber();
        System.out.println(vendorTypeNumber);
       // ResultSet resultSet = connection.prepareStatement("select count(1) as bb from t ").executeQuery();
       //
//        while(resultSet.next()){
//            ResultSetMetaData metaData = resultSet.getMetaData();
//            int columnCount = metaData.getColumnCount();
//            System.out.println("columnCount "+columnCount);
//            String columnName = metaData.getColumnName(1);
//            String columnLabel = metaData.getColumnLabel(1);
//            int columnType = metaData.getColumnType(1);
//            String columnTypeName = metaData.getColumnTypeName(1);
//            System.out.println(columnType);
//            String r1 = resultSet.getString(1);
////            String r2 = resultSet.getString(2);
////            String r3 = resultSet.getString(3);
//            System.out.println(r1);
//        }
        //JDBCType.VARCHAR
//        String r2 = resultSet.getString(2);
//        System.out.println(r1+"  "+r2);
        //Schema.Field.of();
        //JDBCType
        //JDBCType.ARRAY.getVendorTypeNumber();
        //KvCoder.of(BigEndianIntegerCoder.of(), StringUtf8Coder.of())
        Schema type =
                Schema.builder().addStringField("id").addStringField("name").build();

        for (String name:type.getFieldNames()){
            System.out.println(name);
        }
        //CoderRegistry cr = pipeline.getCoderRegistry();

//        pipeline.apply(JdbcIO.<Map<String,Object>>read()
//                        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(cpds))
//                .withCoder(MapCoder.of(StringUtf8Coder))
//                        .withQuery("select * from test")
//                .withRowMapper(new JdbcIO.RowMapper<KV<String,String>>(){
//
//                    @Override
//                    public KV<String, String> mapRow(ResultSet resultSet) throws Exception {
//                        String g = resultSet.getString(1);
//                        System.out.println(g);
//                        return KV.of(resultSet.getString(1), resultSet.getString(2));
//                    }
//                }));

        /*PCollection<TestRow> apply = pipeline.apply(JdbcIO.<TestRow>read().
                withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                        .create(cpds)

                ).withCoder(SerializableCoder.of(TestRow.class))
                .withQuery("select * from test")
                .withRowMapper(new JdbcIO.RowMapper<TestRow>() {
                    @Override
                    public TestRow mapRow(ResultSet resultSet) throws Exception {
                        TestRow testRow = new TestRow();
                        testRow.setName(resultSet.getString(1));
                        return testRow;
                    }
                })
        );*/

//        PCollection<Row> apply = pipeline.apply(JdbcIO.<Row>read().
//                withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
//                        .create(cpds)
//
//                ).withCoder(SchemaCoder.of(type))
//                .withQuery("select * from test")
//                .withRowMapper(new JdbcIO.RowMapper<Row>() {
//                    @Override
//                    public Row mapRow(ResultSet resultSet) throws Exception {
//                        String id = resultSet.getString(1);
//                        String name = resultSet.getString(2);
//                        System.out.println(id+" "+name);
//                        Row build = Row.withSchema(type).addValue(id).addValue(name).build();
//
//                        ResultSetMetaData metaData = resultSet.getMetaData();
//                        int columnCount = metaData.getColumnCount();
//                        int columnType = metaData.getColumnType(1);
//                        String columnTypeName = metaData.getColumnTypeName(1);
//                        String columnClassName = metaData.getColumnClassName(1);
//
//                        return build;
//                    }
//                })
//        );
//
//        apply.apply(ParDo.of(new DoFn<Row, String>(){
//            @ProcessElement
//            public void processElement(ProcessContext c) {
//                Row element = c.element();
//
//            }
//        }));

        pipeline.run().waitUntilFinish();
    }


}
