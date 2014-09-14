package com.taobao.tddl.sample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.taobao.tddl.client.jdbc.TDataSource;

public class ZhuochengSample {

    public static void main(String[] args) throws Exception {

        com.taobao.tddl.client.jdbc.TDataSource ds = new TDataSource();
        ds.setDynamicRule(true);
        // init a datasource with dynamic config on diamond
        ds.setAppName("TRIP_ORDER_APP_DAILY");
        ds.setRuleFile("zhuocheng.xml");

        ds.init();

        System.out.println("init done");

        Connection conn = ds.getConnection();

        // insert a record
        int n = conn.prepareStatement("insert into trip_order (order_id,buyer_id) values (527316008704326,162712643)")
            .executeUpdate();
        // n =
        // conn.prepareStatement("insert into trip_order (id,buyer_id) values (9999999,999999999999999)")
        // .executeUpdate();
        // System.out.println("insert done" + n);

        // select all records
        PreparedStatement ps = conn.prepareStatement(" select * from trip_order where buyer_id=999999999999999 limit 10");

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
