package com.taobao.tddl.sample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.taobao.tddl.client.jdbc.TDataSource;

public class XuyunSample {

    public static void main(String[] args) throws Exception {

        TDataSource ds = new TDataSource();
        ds.setDynamicRule(true);
        // init a datasource with dynamic config on diamond
        ds.setAppName("HUANGPU_APP");
        ds.setAppRuleFile("xuyun.xml");
        ds.setDynamicRule(true);
        ds.init();

        System.out.println("init done");

        Connection conn = ds.getConnection();

        PreparedStatement ps = conn.prepareStatement("SELECT  count(1) as prefer_record_count ,  avg(desc_prefer) as desc_average_prefer, sum(effect_prefer)/count(1) as effect_average_prefer, sum(service_prefer)/count(1) as service_average_prefer from huangpu_prefer222 where content_id=1");

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
