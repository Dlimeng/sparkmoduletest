package com.lm.beam.sql;

import com.lm.beam.model.IndexerPipelineOptions;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.hadoop.WritableCoder;
import org.apache.beam.sdk.io.hcatalog.HCatalogIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.jdbc.HivePreparedStatement;
import org.junit.Test;

import java.beans.PropertyVetoException;
import java.io.Serializable;
import java.sql.*;
import java.util.*;

/**
 * @Author: limeng
 * @Date: 2019/7/20 20:09
 */
public class HiveSave implements Serializable {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    public static void main(String[] args) throws PropertyVetoException, SQLException {
        //SparkPipelineOptions
        SparkPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(SparkPipelineOptions.class);
        //PipelineOptions options = PipelineOptionsFactory.create();
        // 显式指定PipelineRunner：DirectRunner（Local模式）
        options.setRunner(DirectRunner.class);
        //PipelineOptionsFactory.fromArgs(args).withValidation().as(IndexerPipelineOptions.class);

        Pipeline pipeline = Pipeline.create(options);



        ComboPooledDataSource cpds = new ComboPooledDataSource();
//        cpds.setDriverClass("com.mysql.jdbc.Driver");
//        cpds.setJdbcUrl("jdbc:mysql://192.168.20.115:3306/test?useSSL=false");
//        cpds.setUser("root");
//        cpds.setPassword("root");
        cpds.setDriverClass(driverName);
        cpds.setJdbcUrl("jdbc:hive2://192.168.20.117:10000/default");
        cpds.setUser("hive");
        cpds.setPassword("hive");
        String sql="insert into test2019(name,id) values(\"21\",\"22\")";
//        Connection connection = cpds.getConnection();
//        PreparedStatement preparedStatement = connection.prepareStatement("insert into test2019(name,id) values(\"122\",\"123\")");
//        preparedStatement.executeUpdate();

        //getSchemaLabel(resultSet);

//        preparedStatement.close();
//        connection.close();

        Schema id = Schema.builder().addStringField("id").build();
        Row row = Row.withSchema(id).addValue("11").build();
        pipeline.apply(Create.of(row)).apply(JdbcIO.<Row>write()
                .withDataSourceConfiguration(
                        JdbcIO.DataSourceConfiguration.create(
                                cpds))

                );
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testSave() throws PropertyVetoException, SQLException {
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass(driverName);
        cpds.setJdbcUrl("jdbc:hive2://192.168.20.117:10000/default");
        cpds.setUser("hive");
        cpds.setPassword("hive");
        String sql="select * from test2019";

        Connection connection = cpds.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();

        while (resultSet.next()){
            String string = resultSet.getString(1);
            System.out.println(string);
        }
        preparedStatement.close();
        connection.close();

    }

    @Test
    public void testSave2() throws PropertyVetoException, SQLException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        String url="jdbc:hive2://192.168.20.200:2181,192.168.20.117:2181,192.168.20.116:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2";
        String username="hive";
        String passwod="hive";
        Class.forName(driverName).newInstance();
        Connection conn = DriverManager.getConnection(url,username,passwod);
        String sql="insert into test2019(name,id) values(\"21\",\"31\")";
        PreparedStatement preparedStatement = conn.prepareStatement(sql);
        preparedStatement.executeUpdate();
        preparedStatement.close();
        conn.close();
    }


    @Test
    public void testSave3() throws HCatException, PropertyVetoException {

        PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(DirectRunner.class);
        Pipeline pipeline = Pipeline.create(options);
        List<HCatFieldSchema> columns = new ArrayList<>(4);
        columns.add(new HCatFieldSchema("entid", TypeInfoFactory.intTypeInfo, ""));
        columns.add(new HCatFieldSchema("entname", TypeInfoFactory.stringTypeInfo, ""));
        columns.add(new HCatFieldSchema("personid",TypeInfoFactory.intTypeInfo,""));
        columns.add(new HCatFieldSchema("partition",TypeInfoFactory.intTypeInfo,""));

        HCatSchema hCatSchema = new HCatSchema(columns);

        List<HCatRecord> expected = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            HCatRecord record = new DefaultHCatRecord(3);
            record.set("entid",hCatSchema,i);
            record.set("entname",hCatSchema,"newname"+i);
            record.set("personid",hCatSchema,i);
            expected.add(record);
        }

        String driverClass="com.mysql.jdbc.Driver";

        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass(driverClass);
        cpds.setJdbcUrl("jdbc:mysql://40.73.59.12:3306/yuansu_increment?useSSL=false");
        cpds.setUser("yuansu_increment");
        cpds.setPassword("Kboxbr201920192019");


        PCollection<HCatRecord> record = pipeline.apply(JdbcIO.<HCatRecord>read().
                withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                        .create(cpds)

                ).withCoder(getOutputCoder()).withFetchSize(10000)
                .withQuery("select entid,entname,personid from company limit 100")
                .withRowMapper(new JdbcIO.RowMapper<HCatRecord>() {
                    Random r = new Random(1);
                    @Override
                    public HCatRecord mapRow(ResultSet resultSet) throws SQLException, HCatException {
                        HCatRecord record = new DefaultHCatRecord(4);
                        record.set("entid", hCatSchema, resultSet.getInt("entid"));
                        record.set("entname", hCatSchema, resultSet.getString("entname"));
                        record.set("personid", hCatSchema, resultSet.getInt("entid"));
                        int i = r.nextInt(10);
                        record.set("partition", hCatSchema, i);
                        return record;
                    }
                }));

//        PCollectionList<HCatRecord> partition = record.apply(Partition.of(10, new Partition.PartitionFn<HCatRecord>() {
//            @Override
//            public int partitionFor(HCatRecord elem, int numPartitions) {
//                try {
//                    return elem.getInteger("partition", hCatSchema) / numPartitions;
//
//                } catch (HCatException e) {
//                    e.printStackTrace();
//                }
//                return 0;
//            }
//        }));

        Map<String, String> configProperties = new HashMap<>();
        configProperties.put("hive.metastore.uris","thrift://m5.server:9083");

       // PCollection<HCatRecord> record = pipeline.apply("record", Create.of(expected));

//       for (int i = 0; i < partition.size(); i++) {
//           partition.get(i).apply(HCatalogIO.write().withConfigProperties(configProperties).withDatabase("default").withTable("company_test2").withBatchSize(1024L));
//
//        }
        record.apply(Window.remerge()).apply(HCatalogIO.write().withConfigProperties(configProperties).withDatabase("default").withTable("company_test2").withBatchSize(1024L));
        Window.remerge();
        pipeline.run().waitUntilFinish();
    }


    /**
     * 获取Schema
     * @param resultSet 结果集
     */
    public static Schema getSchemaLabel(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        if(columnCount > 0){
            String columnLabel1 = metaData.getColumnLabel(1);
            String columnLabel2 = metaData.getColumnLabel(2);
            List<Schema.Field>  fields = new ArrayList<>();

            while (resultSet.next()){
                String name = resultSet.getString(columnLabel1);
                String type = resultSet.getString(columnLabel2);
                Schema.Field field = getSchemaField(name,type);
                fields.add(field);
            }
            if(fields != null && fields.size() > 0) {
                return Schema.builder().addFields(fields).build();
            }
        }
        return null;
    }


    public static Schema.Field getSchemaField(String columnName,String columnType){
        Schema.Field result=null;
        if(StringUtils.isNotBlank(columnType) && StringUtils.isNotBlank(columnName)){

            //string
            String stringNumber = "string";
            String varcharNumber = JDBCType.VARCHAR.getName().toLowerCase();
            String longVarcharNumber = JDBCType.LONGNVARCHAR.getName().toLowerCase();
            String charNumber = JDBCType.CHAR.getName().toLowerCase();
            //BigDecimal
            String numericNumber = JDBCType.NUMERIC.getName().toLowerCase();
            String decimalNumber = JDBCType.DECIMAL.getName().toLowerCase();
            //boolean
            String bitNumber = JDBCType.BIT.getName().toLowerCase();
            String booleanNumber = JDBCType.BOOLEAN.getName().toLowerCase();
            //byte
            String tinyintNumber = JDBCType.TINYINT.getName().toLowerCase();
            //short
            String smallintNumber = JDBCType.SMALLINT.getName().toLowerCase();
            String intNumber="int";
            //int
            String integerNumber = JDBCType.INTEGER.getName().toLowerCase();
            //long
            String bigintNumber = JDBCType.BIGINT.getName().toLowerCase();
            //float
            String realNumber = JDBCType.REAL.getName().toLowerCase();
            //double
            String floatNumber = JDBCType.FLOAT.getName().toLowerCase();
            String doubleNumber = JDBCType.DOUBLE.getName().toLowerCase();
            //byte[]
            String binaryNumber = JDBCType.BINARY.getName().toLowerCase();
            String varbinaryNumber = JDBCType.VARBINARY.getName().toLowerCase();
            String longvarbinaryNumber = JDBCType.LONGVARBINARY.getName().toLowerCase();
            //date
            String dateNumber = JDBCType.DATE.getName().toLowerCase();
            //time
            String timeNumber = JDBCType.TIME.getName().toLowerCase();
            //timestamp
            String timestampNumber = JDBCType.TIMESTAMP.getName().toLowerCase();
            //clob
            String clobNumber = JDBCType.CLOB.getName().toLowerCase();
            //blob
            String blobNumber = JDBCType.BLOB.getName().toLowerCase();
            //array
            String arrayNumber = JDBCType.ARRAY.getName().toLowerCase();
            //mapping of underlying type
            String distinctNumber = JDBCType.DISTINCT.getName().toLowerCase();
            //Struct
            String structNumber = JDBCType.STRUCT.getName().toLowerCase();
            //ref
            String refNumber = JDBCType.REF.getName().toLowerCase();
            //URL
            String datalinkNumber = JDBCType.DATALINK.getName().toLowerCase();

            //string
            if(columnType.contains(stringNumber) || columnType.contains(varcharNumber)||  columnType.contains(longVarcharNumber)|| columnType.contains(charNumber)){
                result = Schema.Field.of(columnName,Schema.FieldType.STRING);

            }else if(columnType.contains(bitNumber) || columnType.contains(booleanNumber)){
                //boolean
                result = Schema.Field.of(columnName,Schema.FieldType.BOOLEAN);

            }else if(columnType.contains(numericNumber) || columnType.contains(decimalNumber)){
                //BigDecimal
                result = Schema.Field.of(columnName,Schema.FieldType.DECIMAL);

            }else if(columnType.contains(integerNumber) || columnType.contains(intNumber)){
                //int
                result = Schema.Field.of(columnName,Schema.FieldType.INT32);

            }else if(columnType.contains(bigintNumber)){
                //long
                result = Schema.Field.of(columnName,Schema.FieldType.INT64);

            }else if(columnType.contains(realNumber)) {
                //float
                result = Schema.Field.of(columnName,Schema.FieldType.FLOAT);

            }else if(columnType.contains(floatNumber) || columnType.contains(doubleNumber)){
                //double
                result = Schema.Field.of(columnName,Schema.FieldType.DOUBLE);

            }else if(columnType.contains(smallintNumber)){
                //short
                result = Schema.Field.of(columnName,Schema.FieldType.INT16);

            }else if(columnType.contains(dateNumber)|| columnType.contains(timeNumber) || columnType.contains(timestampNumber)){
                //date time timestamp
                result = Schema.Field.of(columnName,Schema.FieldType.DATETIME);

            }else if(columnType.contains(tinyintNumber)){
                //byte
                result = Schema.Field.of(columnName,Schema.FieldType.BYTE);

            }
            return result;
        }
        return null;
    }
    /**
     * 类型序列化
     * @return
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static Coder<HCatRecord> getOutputCoder() {
        return (Coder) WritableCoder.of(DefaultHCatRecord.class);
    }
    public static class  changes extends DoFn<HCatRecord, HCatRecord>{
        @ProcessElement
        public void processElement(ProcessContext ctx) throws Exception {
            ctx.output(ctx.element());
        }
    }

}
