package com.lm.beam.sql.util;

import java.sql.*;

/**
 * @Author: limeng
 * @Date: 2019/7/16 10:31
 */
public class jdbctest {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Connection conn=null;

            Class.forName("com.mysql.jdbc.Driver");
            //加载数据库驱动
            System.out.println("数据库驱动加载成功");
            String url="jdbc:mysql://192.168.20.115:3306/test?useSSL=false";
            //如果不加useSSL=false就会有警告，由于jdbc和mysql版本不同，有一个连接安全问题

            String user="root";
            String passWord="root";
            //System.out.println("1");
            //Connection对象引的是java.sql.Connection包
            conn=(Connection) DriverManager.getConnection(url,user,passWord);
            //创建连接
            System.out.println("已成功的与数据库MySQL建立连接！！");

    }
}
