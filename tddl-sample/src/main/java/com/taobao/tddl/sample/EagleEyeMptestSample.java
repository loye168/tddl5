package com.taobao.tddl.sample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.properties.ConnectionProperties;
import com.taobao.tddl.matrix.jdbc.TDataSource;

public class EagleEyeMptestSample {

    public static void main(String[] args) throws TddlException, SQLException {
        TDataSource ds = new TDataSource();

        // init a datasource with local config file
        ds.setAppName("eagleeye_mptest");
        // ds.setRuleFile("eagleeye_rule.xml");
        ds.getConnectionProperties().put(ConnectionProperties.CONCURRENT_THREAD_SIZE, 10);
        ds.getConnectionProperties().put(ConnectionProperties.MERGE_CONCURRENT, true);
        ds.init();
        System.out.println("init done");
        testClient(ds);
    }

    public static void testClient(TDataSource ds) throws TddlException, SQLException {
        // insert a record
        Connection conn = ds.getConnection();
        String sql = "select * from client_test " + "where trace_time>=? and trace_time<=? limit 10";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setObject(1, "2014-06-23 00:00:00");
        ps.setObject(2, "2014-06-23 23:59:59");

        // select all records
        long start = System.currentTimeMillis();
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            StringBuilder sb = new StringBuilder();
            int count = rs.getMetaData().getColumnCount();
            for (int i = 1; i <= count; i++) {

                String key = rs.getMetaData().getColumnLabel(i);
                Object val = rs.getObject(i);
                sb.append("[" + rs.getMetaData().getTableName(i) + "." + key + "->" + val + "]");
            }
            System.out.println(sb.toString());
        }

        rs.close();
        ps.close();
        conn.close();

        System.out.println("query done");
        System.out.println(System.currentTimeMillis() - start);
    }
}
