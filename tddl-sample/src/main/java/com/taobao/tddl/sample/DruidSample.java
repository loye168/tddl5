package com.taobao.tddl.sample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.alibaba.druid.pool.DruidDataSource;

public class DruidSample {

    public static void main(String[] args) throws Exception {
        DruidDataSource ds = new DruidDataSource();
        ds.setUrl("jdbc:mysql://127.0.0.1:9507/");
        ds.setUsername("TDDL5_APP");
        ds.setPassword("TDDL5_APP");
        ds.init();

        System.out.println("init done");
        Connection conn = ds.getConnection();
        // insert a record
        // conn.prepareStatement("replace into sample_table (id,name,address) values (1,'sun','hz')").executeUpdate();
        System.out.println("insert done");
        // select all records
        PreparedStatement ps = conn.prepareStatement("replace into tddl_category (id,name,gmt_modified,gmt_create) values (1,'aa',now(),now())");
        // ps.executeUpdate();

        ps = conn.prepareStatement("show @@version");
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
        conn.close();
        System.out.println("query done");
    }
}
