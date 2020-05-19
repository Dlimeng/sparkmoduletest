package com.lm.beam.sql.gbase;

//import com.lm.beam.runners.DirectRunnerExamination;
//import com.mchange.v2.c3p0.ComboPooledDataSource;
//import org.apache.beam.runners.spark.SparkPipelineOptions;
//import org.apache.beam.runners.spark.SparkRunner;
//import org.apache.beam.sdk.Pipeline;
//import org.apache.beam.sdk.coders.KvCoder;
//import org.apache.beam.sdk.coders.SerializableCoder;
//import org.apache.beam.sdk.coders.StringUtf8Coder;
//import org.apache.beam.sdk.io.jdbc.JdbcIO;
//import org.apache.beam.sdk.options.PipelineOptionsFactory;
//import org.apache.beam.sdk.transforms.Create;
//import org.apache.beam.sdk.values.KV;
//import org.junit.Test;
//
//import javax.sql.DataSource;
//import java.beans.PropertyVetoException;
//import java.math.BigDecimal;
//import java.sql.*;
//import java.util.ArrayList;
//import java.util.Calendar;
//import java.util.Date;
//import java.util.List;

/**
 * gbase
 * @Author: limeng
 * @Date: 2019/10/10 18:59
 */
public class gbaseTest {
//    private String url="jdbc:gbase://192.168.100.1:5258/test";
//    private String username="root";
//    private String password="gbase";
//    private String driver="com.gbase.jdbc.Driver";
//    private Connection conn;
//    private int inNum=1000000;
//    @Test
//    public void save(){
//        //conn = getConnection();
//        String sql="insert into batch_test(id, name, createtime, updatetime, `desc`, money1, money2, word, age) values(?,?,?,?,?,?,?,?,?)";
//        try {
//            conn = getDataSource().getConnection();
//            Calendar calendar = Calendar.getInstance();
//            long time = calendar.getTime().getTime();
//            PreparedStatement ps = conn.prepareStatement(sql);
//            ps.setInt(1,1);
//            ps.setString(2,"name1");
//            ps.setTimestamp(3,new Timestamp(time));
//            ps.setTimestamp(4,new Timestamp(time));
//            ps.setString(5,"desc111111111111");
//            ps.setDouble(6,0.11);
//            BigDecimal bigDecimal = new BigDecimal(10);
//            ps.setBigDecimal(7,bigDecimal);
//            ps.setString(8,"word111");
//            ps.setInt(9,9);
//            int i = ps.executeUpdate();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//    }
//
//
//    @Test
//    public void testSparkRunner(){
//        long startTime = System.currentTimeMillis();
//        SparkPipelineOptions options = PipelineOptionsFactory.fromArgs("").as(SparkPipelineOptions.class);
//        options.setRunner(SparkRunner.class);
//        options.setSparkMaster("local[*]");
//        /**
//         * 默认local[*] 87
//         * local[30]   72
//         * 100L 248
//         * 500L 114
//         * 1000L 105
//         * 1500L 95
//         * Options BundleSize 和 io BatchSize 对应  88
//         * 2500L 84
//         * 3000L 82
//         * 4096L 84
//         */
//        //options.setBundleSize(4096L);
//        Pipeline pipeline = Pipeline.create(options);
//
//        List<KV<String, String>> list = new ArrayList<>();
//
//        String sql="insert into batch_test2(id,name) values(?,?)";
//
//        for (int i = 1; i <= inNum; i++) {
//            String id=String.valueOf(i);
//            String name="name"+i;
//            list.add(KV.of(id, name));
//        }
//
//
//
//        pipeline.apply(Create.of(list))
//                .setCoder(KvCoder.of(StringUtf8Coder.of(),StringUtf8Coder.of()))
//                .apply(JdbcIO.<KV<String,String>>write()
//                        .withDataSourceConfiguration(JdbcIO.
//                                DataSourceConfiguration.
//                                create(getDataSource()))
//                        .withStatement(sql).withBatchSize(2048L)
//                        .withPreparedStatementSetter(new DirectRunnerExamination.PrepareStatementFromRow()));
//
//
//        pipeline.run().waitUntilFinish();
//        long endTime = System.currentTimeMillis();
//        System.out.println("插入数据用时：" + (endTime - startTime) / 1000 + " 秒");
//    }
//
//    @Test
//    public void testSparkRunner2(){
//        long startTime = System.currentTimeMillis();
//        SparkPipelineOptions options = PipelineOptionsFactory.fromArgs("").as(SparkPipelineOptions.class);
//        options.setRunner(SparkRunner.class);
//        options.setSparkMaster("local[*]");
//
//        Pipeline pipeline = Pipeline.create(options);
//
//        List<BatchTestObject> list = new ArrayList<>();
//
//        String sql="insert into batch_test(id, name, createtime, updatetime, `desc`, money1, money2, word, age) values(?,?,?,?,?,?,?,?,?)";
//        BatchTestObject batchTestObject=null;
//        for (int i = 1; i <= inNum; i++) {
//            batchTestObject =new BatchTestObject();
//            batchTestObject.setId(i);
//
//            String name="name"+i;
//            batchTestObject.setName(name);
//            batchTestObject.setCreatetime(new Date());
//            batchTestObject.setUpdatetime(new Date());
//            batchTestObject.setDesc("desc:"+i);
//            batchTestObject.setMoney1(0.11);
//            BigDecimal bigDecimal = new BigDecimal(10);
//            batchTestObject.setMoney2(bigDecimal);
//            batchTestObject.setWord("word:"+i);
//            batchTestObject.setAge(i);
//            list.add(batchTestObject);
//        }
//
//
//        pipeline.apply(Create.of(list))
//                .setCoder(SerializableCoder.of(BatchTestObject.class))
//                .apply(JdbcIO.<BatchTestObject>write()
//                        .withDataSourceConfiguration(JdbcIO.
//                                DataSourceConfiguration.
//                                create(getDataSource()))
//                        .withStatement(sql).withBatchSize(2048L)
//                        .withPreparedStatementSetter(new gbaseTest.PrepareStatementFromRow()));
//
//
//
//
//        pipeline.run().waitUntilFinish();
//        long endTime = System.currentTimeMillis();
//        System.out.println("插入数据用时：" + (endTime - startTime) / 1000 + " 秒");
//    }
//
//
//    /**
//     * 获取连接方法
//     *
//     */
//    public Connection getConnection() {
//        Connection conn = null;
//        try {
//            Class.forName(driver);
//            conn = (Connection) DriverManager.getConnection(url, username, password);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return conn;
//    }
//
//
//    public DataSource getDataSource(){
//        ComboPooledDataSource cpds = new ComboPooledDataSource();
//        try {
//            cpds.setDriverClass(driver);
//        } catch (PropertyVetoException e) {
//            e.printStackTrace();
//        }
//        cpds.setJdbcUrl(url);
//        cpds.setUser(username);
//        cpds.setPassword(password);
//        return cpds;
//    }
//
//    public static class PrepareStatementFromRow implements JdbcIO.PreparedStatementSetter<BatchTestObject>{
//        @Override
//        public void setParameters(BatchTestObject element, PreparedStatement ps) throws Exception {
//
//            ps.setInt(1,element.getId());
//            ps.setString(2,element.getName());
//            ps.setTimestamp(3,new Timestamp(element.getCreatetime().getTime()));
//            ps.setTimestamp(4,new Timestamp(element.getUpdatetime().getTime()));
//            ps.setString(5,element.getDesc());
//            ps.setDouble(6,element.getMoney1());
//            ps.setBigDecimal(7,element.getMoney2());
//            ps.setString(8,element.getWord());
//            ps.setInt(9,element.getAge());
//        }
//    }

}
