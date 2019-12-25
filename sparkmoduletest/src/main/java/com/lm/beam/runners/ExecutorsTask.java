package com.lm.beam.runners;

import org.junit.Test;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Calendar;
import java.util.concurrent.*;

/**
 * 多线程
 * @Author: limeng
 * @Date: 2019/10/8 18:15
 */
public class ExecutorsTask {

//    private String driver = "com.mysql.jdbc.Driver";
//    private String url = "jdbc:mysql://192.168.20.115:3306/test?useSSL=false";
//    private String username = "root";
//    private String password ="root";

    private String url="jdbc:gbase://192.168.100.1:5258/test";
    private String username="root";
    private String password="gbase";
    private String driver="com.gbase.jdbc.Driver";
    int inNum=100000;

    public void multiThreadImport() throws SQLException {
        Connection conn =getConnection();
        String sql="insert into batch_test(id,name) values(?,?)";
        conn.setAutoCommit(false);
        PreparedStatement ps = conn.prepareStatement(sql);

        int i2 = 2 << 10;

        for (int i = 1; i <= inNum; i++) {
              String id=String.valueOf(i);
              String name="name"+i;
              ps.setString(1,id);
              ps.setString(2,name);
              ps.addBatch();
              if((i & (i2-1)) == 0){
                  ps.executeBatch();
                  conn.commit();
                  ps.clearBatch();
              }
        }
        ps.executeBatch();
        conn.commit();
        ps.clearBatch();
        release(conn,null,null);
    }


    public void multiThreadImport2() throws SQLException {
        Connection conn =getConnection();
        String sql="insert into batch_test(id, name, createtime, updatetime, `desc`, money1, money2, word, age) values(?,?,?,?,?,?,?,?,?)";
        conn.setAutoCommit(false);
        PreparedStatement ps = conn.prepareStatement(sql);

        int i2 = 2 << 10;
        BigDecimal bigDecimal = new BigDecimal(10);
        Calendar calendar = Calendar.getInstance();
        long time = calendar.getTime().getTime();
        for (int i = 1; i <= inNum; i++) {
            String name="name"+i;
            ps.setInt(1,i);
            ps.setString(2,name);
            ps.setTimestamp(3,new Timestamp(time));
            ps.setTimestamp(4,new Timestamp(time));
            ps.setString(5,"desc:"+i);
            ps.setDouble(6,0.11);
            ps.setBigDecimal(7,bigDecimal);
            ps.setString(8,"word:"+i);
            ps.setInt(9,i);
            ps.addBatch();
            if((i & (i2-1)) == 0){
                ps.executeBatch();
                conn.commit();
                ps.clearBatch();
            }
        }
        ps.executeBatch();
        conn.commit();
        ps.clearBatch();
        release(conn,null,null);
    }


    /**
     * 获取连接方法
     *
     */
    public Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName(driver);
            conn = (Connection) DriverManager.getConnection(url, username, password);
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
    public  void release(Connection conn, PreparedStatement pstmt, ResultSet rs) {
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


    private ExecutorService getExecutorService(int threadNum){
        return new ThreadPoolExecutor(threadNum, threadNum,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>());
    }

    /**
     * 54秒 10万数据
     */
    @Test
    public void init(){
        long startTime = System.currentTimeMillis();
        //int processors = Runtime.getRuntime().availableProcessors();
        int processors = 30;
        ExecutorService executorService = getExecutorService(processors);
        int countDownLatchNum=10;
        final CountDownLatch countDownLatch = new CountDownLatch(countDownLatchNum);
        for (int i = 0; i < countDownLatchNum; i++) {
            executorService.execute(()->{
                try {
                    multiThreadImport2();
                }catch (Exception e){
                    e.printStackTrace();

                }finally {
                    countDownLatch.countDown();
                }
            });
        }

        try {
            countDownLatch.await();
            long endTime = System.currentTimeMillis();
            System.out.println("插入数据用时：" + (endTime - startTime) / 1000 + " 秒");
            executorService.shutdown();

        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }

}
