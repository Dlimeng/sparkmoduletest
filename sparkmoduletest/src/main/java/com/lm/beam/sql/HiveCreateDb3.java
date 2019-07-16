package com.lm.beam.sql;

import com.alibaba.druid.pool.DruidDataSource;
import jodd.util.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * @Author: limeng
 * @Date: 2019/7/15 16:49
 */
public class HiveCreateDb3 {
    private static DruidDataSource hiveDataSource = new DruidDataSource();
    public static Connection conn = null;
    private static final Logger log = LoggerFactory.getLogger(HiveCreateDb3.class);

    public static DruidDataSource getHiveDataSource() {
        if(hiveDataSource.isInited()){
            return hiveDataSource;
        }

        try {
            hiveDataSource.setUrl("jdbc:hive2://192.168.20.117:10000/a_mart");
            hiveDataSource.setUsername("hive");
            hiveDataSource.setPassword("hive");

            //配置初始化大小、最小、最大
            hiveDataSource.setInitialSize(20);
            hiveDataSource.setMinIdle(20);
            hiveDataSource.setMaxActive(500);

            //配置获取连接等待超时的时间
             hiveDataSource.setMaxWait(60000);



            //配置间隔多久才进行一次检测，检测需要关闭的空闲连接，单位是毫秒
            hiveDataSource.setTimeBetweenEvictionRunsMillis(60000);

            //配置一个连接在池中最小生存的时间，单位是毫秒
            hiveDataSource.setMinEvictableIdleTimeMillis(300000);

//            hiveDataSource.setValidationQuery("select * from xxxx");
            hiveDataSource.setTestWhileIdle(false);
//            hiveDataSource.setTestOnBorrow(false);
//            hiveDataSource.setTestOnReturn(false);

            //打开PSCache，并且指定每个连接上PSCache的大小
            hiveDataSource.setPoolPreparedStatements(true);
            hiveDataSource.setMaxPoolPreparedStatementPerConnectionSize(20);

            //配置监控统计拦截的filters
//            hiveDataSource.setFilters("stat");

            hiveDataSource.init();
        } catch (SQLException e) {
            e.printStackTrace();
            closeHiveDataSource();
        }
        return hiveDataSource;
    }

    /**
     *@Description:关闭Hive连接池
     */
    public static void closeHiveDataSource(){
        if(hiveDataSource != null){
            hiveDataSource.close();
        }
    }

    /**
     *
     *@Description:获取Hive连接
     *@return
     */
    public static Connection getHiveConn(){
        try {
            hiveDataSource = getHiveDataSource();
            conn = hiveDataSource.getConnection();
        } catch (SQLException e) {
            log.error("--"+e+":获取Hive连接失败！");
        }
        return conn;
    }

    /**
     *@Description:关闭Hive数据连接
     */
    public static void closeConn(){
        try {
            if(conn != null){
                conn.close();
            }
        } catch (SQLException e) {
            log.error("--"+e+":关闭Hive-conn连接失败！");
        }
    }


    public static void main(String[] args) throws Exception {
        DataSource ds = HiveCreateDb3.getHiveDataSource();
        Connection conn = ds.getConnection();
        Statement stmt = null;
        if(conn == null){
            System.out.println("null");
        }else{
            System.out.println("conn");
            stmt = conn.createStatement();
            ResultSet res = stmt.executeQuery("select * from test t");
            int i = 0;
            while(res.next()){
                if(i<10){
                    System.out.println(res.getString(1));
                    i++;
                }
            }
        }

        stmt.close();
        conn.close();
    }
}
