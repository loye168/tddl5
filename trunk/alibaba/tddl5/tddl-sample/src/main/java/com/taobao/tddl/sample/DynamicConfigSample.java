package com.taobao.tddl.sample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.taobao.tddl.client.jdbc.TDataSource;

public class DynamicConfigSample {

    public static void main(String[] args) throws Exception {
        TDataSource ds = new TDataSource();

        ds.setAppName("DEGADDB_APP");

        ds.setDynamicRule(true);
        ds.init();

        long start = System.currentTimeMillis();
        System.out.println("init done");
        Connection conn = ds.getConnection();
        // insert a record
        // conn.prepareStatement("replace into sample_table (id,name,address) values (1,'sun','hz')").executeUpdate();
        System.out.println("insert done");
        // select all records
        PreparedStatement ps = conn.prepareStatement("replace into tddl_category (id,name,gmt_modified,gmt_create) values (1,'aa',now(),now())");
        // ps.executeUpdate();

        ps = conn.prepareStatement("select * from   g_adcustomer");
        // ps.setDate(1, new Date(System.currentTimeMillis()));
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

        System.out.println(System.currentTimeMillis() - start);

        start = System.currentTimeMillis();
        // insert a record
        // conn.prepareStatement("replace into sample_table (id,name,address) values (1,'sun','hz')").executeUpdate();
        System.out.println("insert done");
        // select all records
        ps = conn.prepareStatement("replace into tddl_category (id,name,gmt_modified,gmt_create) values (1,'aa',now(),now())");
        // ps.executeUpdate();

        ps = conn.prepareStatement("select * from   g_adcustomer where 1=1");
        // ps.setDate(1, new Date(System.currentTimeMillis()));
        rs = ps.executeQuery();
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

        System.out.println(System.currentTimeMillis() - start);
        conn.close();
        System.out.println("query done");
    }
}
