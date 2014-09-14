package com.taobao.tddl.sample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.taobao.tddl.client.jdbc.TDataSource;

public class mPXSQSample {

    public static void main(String[] args) throws Exception {

        TDataSource ds = new TDataSource();

        // init a datasource with dynamic config on diamond
        ds.setAppName("TBRAC2_APP");
        ds.setDynamicRule(true);
        ds.init();

        System.out.println("init done");

        Connection conn = ds.getConnection();

        // select all records
        PreparedStatement ps = conn.prepareStatement("select _rcp_daily_case_main.id from rcp_daily_case_main as _rcp_daily_case_main where gmt_create =?");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date date = sdf.parse("2013-10-20");
        ps.setObject(1, date);

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
