package com.alibaba.cobar.manager.test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import com.alibaba.druid.pool.DruidDataSource;

public class ConnectionTest {

    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            DruidDataSource ds = new DruidDataSource();
            ds.setUsername("test");
            ds.setPassword("");
            ds.setUrl("jdbc:mysql://10.20.153.178:9066/");
            ds.setDriverClassName("com.mysql.jdbc.Driver");
            ds.setMaxActive(-1);
            ds.setMinIdle(0);
            ds.setTimeBetweenEvictionRunsMillis(600000);
            // ds.setNumTestsPerEvictionRun(Integer.MAX_VALUE);
            ds.setMinEvictableIdleTimeMillis(DruidDataSource.DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS);
            Connection conn = ds.getConnection();

            Statement stm = conn.createStatement();
            stm.execute("show @@version");

            ResultSet rst = stm.getResultSet();
            rst.next();
            String version = rst.getString("VERSION");

            System.out.println(version);
            ds.close();
        } catch (Exception exception) {
            System.out.println("10.20.153.178:9066   " + exception.getMessage() + exception);
        }
    }
}
