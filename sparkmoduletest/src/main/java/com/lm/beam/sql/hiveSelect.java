package com.lm.beam.sql;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.hadoop.WritableCoder;
import org.apache.beam.sdk.io.hcatalog.HCatalogIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.PropertyVetoException;
import java.io.Serializable;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: limeng
 * @Date: 2019/9/10 19:53
 */
public class hiveSelect implements Serializable {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    @Test
    public void testRead(){
        SparkPipelineOptions options = PipelineOptionsFactory.fromArgs("").withValidation().as(SparkPipelineOptions.class);
        // 显式指定PipelineRunner：DirectRunner（Local模式）
        options.setRunner(DirectRunner.class);
        Pipeline pipeline = Pipeline.create(options);

        Map<String, String> configProperties = new HashMap<String, String>();
        configProperties.put("hive.metastore.uris","thrift://192.168.20.117:9083");

        pipeline.apply(HCatalogIO.read()
                        .withConfigProperties(configProperties)
                        .withDatabase("default")
                        .withTable("testpar")
                .withFilter("year='2018' and month='8'")).apply(ParDo.of(new PrintByHCatIO()));


        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testRead2() throws PropertyVetoException {
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass(driverName);
        cpds.setJdbcUrl("jdbc:hive2://192.168.200.117:10000/default");
        cpds.setUser("hive");
        cpds.setPassword("hive");

        try {
            Connection connection = cpds.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement("select count(1) from kd_swap.oldtable1");
             ResultSet resultSet = preparedStatement.executeQuery();
             while (resultSet.next()){
                 String num = resultSet.getString(1);
                 System.out.println(num);
             }


        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testRead3() throws PropertyVetoException {
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass(driverName);
        cpds.setJdbcUrl("jdbc:hive2://192.168.200.117:10000/default");
        cpds.setUser("hive");
        cpds.setPassword("hive");

        try {
            Connection connection = cpds.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement("show databases");
            ResultSet resultSet = preparedStatement.executeQuery();
            ResultSetMetaData metaData = resultSet.getMetaData();
            System.out.println(metaData.getColumnCount());

            while (resultSet.next()){
                String num = resultSet.getString(1);
                System.out.println(num);
            }


        } catch (SQLException e) {
            e.printStackTrace();
        }
    }



    @Test
    public void testWrite() throws HCatException {
        SparkPipelineOptions options = PipelineOptionsFactory.fromArgs("").withValidation().as(SparkPipelineOptions.class);
        // 显式指定PipelineRunner：DirectRunner（Local模式）
        options.setRunner(DirectRunner.class);
        Pipeline pipeline = Pipeline.create(options);

        Map<String, String> configProperties = new HashMap<String, String>();
        configProperties.put("hive.metastore.uris","thrift://192.168.20.117:9083");

        Map<String,String> map =new HashMap<>();
        map.put("year","2018");
        map.put("month","8");
        List<HCatFieldSchema> columns = new ArrayList<>(2);
        columns.add(new HCatFieldSchema("id", TypeInfoFactory.stringTypeInfo, ""));
        columns.add(new HCatFieldSchema("name", TypeInfoFactory.stringTypeInfo, ""));

        HCatSchema hCatSchema = new HCatSchema(columns);

        List<HCatRecord> expected = new ArrayList<>();
        HCatRecord record = new DefaultHCatRecord(2);
        record.set("id",hCatSchema,"newid");
        record.set("name",hCatSchema,"newname");

        expected.add(record);


        pipeline.apply("record", Create.of(expected).withCoder(getOutputCoder()))
                .apply(HCatalogIO.write()
                        .withConfigProperties(configProperties)
                        .withDatabase("default")
                        .withTable("testpar")
                .withPartition(map));

        pipeline.run().waitUntilFinish();
    }
    @SuppressWarnings({"unchecked", "rawtypes"})
    public  Coder<HCatRecord> getOutputCoder() {
        return (Coder) WritableCoder.of(DefaultHCatRecord.class);
    }

    public static class  PrintByHCatIO extends DoFn<HCatRecord,HCatRecord> {
        private Logger logger = LoggerFactory.getLogger(this.getClass());
        @ProcessElement
        public void processElement( ProcessContext c) {
            HCatRecord element = c.element();
            List<Object> all = element.getAll();
            StringBuilder stringBuilder = new StringBuilder();
            for (Object o:all){
                stringBuilder.append(o.toString()+" ");
            }
            System.out.println(stringBuilder.toString());
            c.output(element);
        }
    }
}
