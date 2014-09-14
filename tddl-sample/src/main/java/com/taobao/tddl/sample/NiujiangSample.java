package com.taobao.tddl.sample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.taobao.tddl.client.jdbc.TDataSource;

public class NiujiangSample {

    public static void main(String[] args) throws Exception {

        com.taobao.tddl.client.jdbc.TDataSource ds = new TDataSource();

        // init a datasource with dynamic config on diamond

        ds.setAppName("CBUDW_DEV_APP");
        ds.setDynamicRule(true);
        ds.init();

        System.out.println("init done");

        Connection conn = ds.getConnection();

        System.out.println("insert done");

        // select all records

        PreparedStatement ps = conn.prepareStatement("select * from ads_dm_jhs_item_info_sdt0");

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
