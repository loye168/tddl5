package com.taobao.tddl.sample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.properties.ConnectionProperties;
import com.taobao.tddl.matrix.jdbc.TDataSource;

public class EagleEyeSample {

    public static void main(String[] args) throws TddlException, SQLException {
        TDataSource ds = new TDataSource();

        // init a datasource with local config file
        ds.setAppName("eagleeye");
        // ds.setRuleFile("eagleeye_rule.xml");
        ds.getConnectionProperties().put(ConnectionProperties.CONCURRENT_THREAD_SIZE, 10);

        ds.getConnectionProperties().put(ConnectionProperties.MERGE_CONCURRENT, true);

        ds.getConnectionProperties().put(ConnectionProperties.COUNT_OF_KEY_TO_RIGHT_INDEX_NESTED_LOOP, "1");

        ds.getConnectionProperties().put(ConnectionProperties.MAX_INDEX_NESTED_LOOP_ITERATION_TIMES, "10");

        ds.getConnectionProperties().put(ConnectionProperties.INDEX_NESTED_LOOP_TIME_OUT, "10000");
        ds.init();

        System.out.println("init done");

        testTraceId(ds);
    }

    public static void testTraceId(TDataSource ds) throws TddlException, SQLException {
        // insert a record
        Connection conn = ds.getConnection();
        String sql = " /* ANDOR ALLOW_TEMPORARY_TABLE=True */ select traceid,tracetime,span1 from eaglelog "
                     + "where tracetime>=? and tracetime<=? and rootAppName=? group by traceid,tracetime,span1 "
                     + "order by tracetime desc limit 15";
        PreparedStatement ps = conn.prepareStatement(sql);

        ps.setObject(1, 1399514386000L);
        ps.setObject(2, 1399514686000L);
        ps.setObject(3, "cybertron:default-ide-projectid");

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

    public static void testJaeTraceId(TDataSource ds) throws SQLException {
        Connection conn = ds.getConnection();

        // insert a record
        String sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */ select jaelog1.traceid,jaelog1.tracetime,max(jaelog2.span1),jaelog1.groupToken,jaelog2.result from";
        sql += " jaelog jaelog1 left join jaelog jaelog2 on jaelog1.traceid=jaelog2.traceid and jaelog2.tracetime>= ? and jaelog2.tracetime<= ? and jaelog2.rpcType=253 where jaelog1.tracetime>= ? and jaelog1.tracetime<= ? ";
        sql += "and jaelog1.rpcType!=253 and jaelog1.groupToken=? group by jaelog1.traceid,jaelog1.groupToken,jaelog2.result limit 20";
        PreparedStatement ps = conn.prepareStatement(sql);
        // ps.setString(1, "2a784acf13946236294728641e");
        ps.setLong(1, 1394553600000L);
        ps.setLong(2, 1394630000000L);
        ps.setLong(3, 1394553600000L);
        ps.setLong(4, 1394630000000L);
        ps.setString(5, "3229$$$");

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
