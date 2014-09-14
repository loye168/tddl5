package com.taobao.tddl.sample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.taobao.tddl.client.jdbc.TDataSource;

public class YongchuanSample {

    public static void main(String[] args) throws Exception {

        com.taobao.tddl.client.jdbc.TDataSource ds = new TDataSource();
        ds.setDynamicRule(true);
        // init a datasource with dynamic config on diamond
        ds.setAppName("TMALL_SERVICE_DPC");

        ds.init();

        System.out.println("init done");

        Connection conn = ds.getConnection();

        PreparedStatement ps = conn.prepareStatement("select last_insert_id()");

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

        ps.close();
        conn.close();

        System.out.println("query done");

    }

}
