package com.lm.beam.sql.util;

import java.sql.*;

/**
 * @Author: limeng
 * @Date: 2019/7/25 15:56
 */
public class JDBCUtils {
    // Hive的驱动
    private static String driver = "org.apache.hive.jdbc.HiveDriver";

    // Hive的url地址
    private static String url = "jdbc:hive2://192.168.20.117:10000/a_mart";

    // 注册数据库的驱动
    static {
        try {
            Class.forName(driver);
        } catch (Exception ex) {
            throw new ExceptionInInitializerError(ex);
        }
    }

    // 获取数据库Hive的连接
    public static Connection getConnection() {
        try {
            return DriverManager.getConnection(url, "root", "root");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    // 释放资源
    public static void release(Connection conn, Statement st, ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (st != null) {
            try {
                st.close();
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
