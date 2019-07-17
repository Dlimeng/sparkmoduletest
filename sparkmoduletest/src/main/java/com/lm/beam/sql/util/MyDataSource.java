package com.lm.beam.sql.util;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.LinkedList;
import java.util.logging.Logger;

/**
 * @Author: limeng
 * @Date: 2019/7/16 10:21
 */
public class MyDataSource implements DataSource, Serializable {

    private static final long serialVersionUID = 3754417760795108548L;

    private static LinkedList<Connection> pool=new LinkedList<Connection>();
    static{
        for (int i = 0; i < 5; i++) {
            Connection conn=jdbcConnection.getConnection();
            pool.add(conn);
        }
    }
    @Override
    public Connection getConnection() throws SQLException {
        Connection conn=null;
        if (pool.size()==0) {
            for (int i = 0; i < 5; i++) {
                conn=jdbcConnection.getConnection();
                pool.add(conn);
            }
        }
        conn=pool.remove(0);
        return conn;
    }

    public void addBack(Connection conn){
        pool.add(conn);
    }
    @Override
    public PrintWriter getLogWriter() throws SQLException {

        return null;
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setLogWriter(PrintWriter arg0) throws SQLException {
        // TODO Auto-generated method stub
        arg0.println(System.out);
    }

    @Override
    public void setLoginTimeout(int arg0) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean isWrapperFor(Class<?> arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }



    @Override
    public Connection getConnection(String arg0, String arg1)
            throws SQLException {

        return null;
    }

}
