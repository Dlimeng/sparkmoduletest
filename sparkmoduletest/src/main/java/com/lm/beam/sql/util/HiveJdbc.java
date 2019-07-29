package com.lm.beam.sql.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Author: limeng
 * @Date: 2019/7/25 15:58
 */
public class HiveJdbc {
    public static void main(String[] args) {
        Connection conn = null;
        PreparedStatement pst = null;
        String sql="insert into a_mart.test values(\"2222\",\"text\")";
        conn = JDBCUtils.getConnection();
        try {
            pst = conn.prepareStatement(sql);
            int i = pst.executeUpdate();
            System.out.println("success "+i);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}
