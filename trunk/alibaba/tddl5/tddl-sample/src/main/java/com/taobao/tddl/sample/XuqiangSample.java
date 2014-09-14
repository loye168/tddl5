package com.taobao.tddl.sample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.taobao.tddl.group.jdbc.TGroupDataSource;

public class XuqiangSample {

    public static void main(String[] args) throws Exception {

        TGroupDataSource ds = new TGroupDataSource();
        // init a datasource with dynamic config on diamond
        ds.setAppName("ICBU_PB_METADATA_TEST_APP");
        ds.setDbGroupKey("PBSERVER_GROUP");
        ds.init();

        System.out.println("init done");

        Connection conn = ds.getConnection();

        PreparedStatement ps = conn.prepareStatement("select * from auks_task_detail_normal");

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
