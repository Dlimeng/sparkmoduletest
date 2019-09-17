package com.lm.beam.sql.oracle;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang.StringUtils;
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
 * @Date: 2019/9/11 16:30
 */
public class OracleTest implements Serializable {
    //驱动
    private static String driver = "oracle.jdbc.driver.OracleDriver";
    //连接字符串
    private static String url = "jdbc:oracle:thin:@//192.168.200.25:1521/huaxia";

    private static String username = "kg"; //用户名

    private static String password = "kg"; //密码

    @Test
    public void testRead() throws PropertyVetoException, SQLException {
        ComboPooledDataSource cpds1 = new ComboPooledDataSource();
        cpds1.setDriverClass(driver);
        cpds1.setJdbcUrl(url);
        cpds1.setUser(username);
        cpds1.setPassword(password);

       // String sql="select * from kg_user where rownum < 10";
        String sql="select * from test1";
        Connection connection = cpds1.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()){
            String columnName = resultSet.getString(1);
            String dataType = resultSet.getString(2);
            System.out.println("columnName:"+columnName+" dataType:"+dataType);
        }
        resultSet.close();
        preparedStatement.close();
        connection.close();

    }

    @Test
    public void testRead3() throws PropertyVetoException, SQLException {
        //SparkPipelineOptions
        SparkPipelineOptions options = PipelineOptionsFactory.fromArgs("").withValidation().as(SparkPipelineOptions.class);
        // 显式指定PipelineRunner：DirectRunner（Local模式）
        options.setRunner(DirectRunner.class);
        Pipeline pipeline = Pipeline.create(options);

        String sql="select * from test1";

        ComboPooledDataSource cpds1 = new ComboPooledDataSource();
        cpds1.setDriverClass(driver);
        cpds1.setJdbcUrl(url);
        cpds1.setUser(username);
        cpds1.setPassword(password);

        Schema build = Schema.builder().addStringField("id").addStringField("name").build();

        PCollection<Row> apply = pipeline.apply(JdbcIO.<Row>read().
                withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                        .create(cpds1)

                ).withCoder(SchemaCoder.of(build))
                .withQuery(sql)
                .withRowMapper(new JdbcIO.RowMapper<Row>() {
                    @Override
                    public Row mapRow(ResultSet resultSet) throws Exception {

                        while (resultSet.next()){
                            String string = resultSet.getString(1);
                            System.out.println(string);
                        }

                        return null;
                    }
                }));


        apply.setCoder(SchemaCoder.of(build)).apply(ParDo.of(new Print()));
        pipeline.run().waitUntilFinish();
    }

    public void testWrite(){

    }

    /**
     * 详情
     * @throws Exception
     */
    @Test
    public void testRead2() throws Exception {
        ComboPooledDataSource cpds1 = new ComboPooledDataSource();
        cpds1.setDriverClass(driver);
        cpds1.setJdbcUrl(url);
        cpds1.setUser(username);
        cpds1.setPassword(password);

        String sql="select u.column_name,u.data_type from user_tab_columns u where u.table_name='KG_USER' order by u.column_name ";
        Connection connection = cpds1.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()){
            String columnName = resultSet.getString(1);
            String dataType = resultSet.getString(2);
            System.out.println("columnName:"+columnName+" dataType:"+dataType);
        }

        resultSet.close();
        preparedStatement.close();
        connection.close();
    }

    /**
     * 转换集合
     * @param resultSet 结果集
     * @return
     * @throws SQLException
     */
    public static List<Object> convertList(Schema type,ResultSet resultSet) throws SQLException {
        List<Object> objects = new ArrayList<>();
        for (String name:type.getFieldNames()){
            Object object = resultSet.getObject(name);
            if(object != null){
                objects.add(object);
            }
        }
        return objects;
    }


    public static class  Print extends DoFn<Row,Row> {
        private Logger logger = LoggerFactory.getLogger(this.getClass());
        @ProcessElement
        public void processElement( ProcessContext c) {
            Row element = c.element();
            List<Object> values = element.getValues();
            StringBuilder stringBuilder = new StringBuilder();
            for (Object o:values){
                stringBuilder.append(o.toString()+" ");
            }
            logger.info(stringBuilder.toString());
            c.output(element);
        }
    }

    /**
     * 查询map
     */
    public static class MapOracleRowMapper implements JdbcIO.RowMapper<Map<String,String>>{

        @Override
        public Map<String, String> mapRow(ResultSet resultSet) throws Exception {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            Map<String, String> map=null;
            if(columnCount > 0){
                map=new HashMap<>();
                for (int i = 0; i < columnCount; i++) {
                    String columnLabel = metaData.getColumnLabel(i+1);
                    System.out.println(columnLabel);
                    if(StringUtils.isNotBlank(columnLabel)){
                        map.put(columnLabel.split("\\.")[1],resultSet.getString(columnLabel));
                    }
                }
            }
            return map;
        }
    }
}
