package com.taobao.tddl.sample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.taobao.tddl.client.jdbc.TDataSource;

public class LushaoqianSample {

    public static void main(String[] args) throws Exception {

        TDataSource ds = new TDataSource();
        ds.setDynamicRule(true);
        // init a datasource with dynamic config on diamond
        ds.setAppName("WIRELESS_WAP_AUKS_V3_APP");
        ds.setAppRuleFile("lushaoqian.xml");
        ds.init();

        System.out.println("init done");

        Connection conn = ds.getConnection();

        PreparedStatement ps = conn.prepareStatement("update auks_task_detail_normal set gmt_modified=now(),sub_task_status=10 where task_id=999 and sub_task_status in (1,2,4,5) and timestampadd(SECOND,gmt_modified,now())>1");

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
