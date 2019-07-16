package com.lm.beam.sql.util;

import java.io.Serializable;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
/**
 * @Author: limeng
 * @Date: 2019/7/16 10:16
 */
public class jdbcConnection implements Serializable {

    private static final long serialVersionUID = -2852671130148042506L;

    private static String driver;
    private static String url;
    private static String username;
    private static String password;
    /**
     * 静态代码块加载配置文件信息
     */
    static {
        try {
            // 1.通过当前类获取类加载器
            //ClassLoader classLoader = jdbcConnection.class.getClassLoader();
            // 2.通过类加载器的方法获得一个输入流
            //InputStream is = classLoader.getResourceAsStream("db.properties");
            // 3.创建一个properties对象
            //Properties props = new Properties();
            // 4.加载输入流
           // props.load(is);
            // 5.获取相关参数的值
            //jdbc:hive2://192.168.20.117:10000/a_mart
            //org.apache.hive.jdbc.HiveDriver
//            driver ="com.mysql.jdbc.Driver";
//            url = "jdbc:mysql://192.168.20.115:3306/test";
//            username = "root";
//            password = "root";
                driver ="org.apache.hive.jdbc.HiveDriver";
                url = "jdbc:hive2://192.168.20.117:10000/a_mart";
                username = "hive";
                password = "hive";
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 获取连接方法
     *
     */
    public static Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName(driver);
            conn = (Connection)DriverManager.getConnection(url, username, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 释放资源方法
     *
     * @param conn
     * @param pstmt
     * @param rs
     */
    public static void release(Connection conn, PreparedStatement pstmt, ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (pstmt != null) {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

}
