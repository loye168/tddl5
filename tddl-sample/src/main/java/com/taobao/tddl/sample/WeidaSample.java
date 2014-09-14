package com.taobao.tddl.sample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.taobao.tddl.client.jdbc.TDataSource;

public class WeidaSample {

    public static void main(String[] args) throws Exception {

        com.taobao.tddl.client.jdbc.TDataSource ds = new TDataSource();
        ds.setDynamicRule(true);
        // init a datasource with dynamic config on diamond
        ds.setAppName("SPU_CENTER_APP");
        // Map cp = new HashMap();
        // cp.put(ConnectionProperties.MERGE_CONCURRENT, "true");
        // ds.setConnectionProperties(cp);
        ds.setAppRuleFile("weida.xml");
        ds.init();

        System.out.println("init done");

        Connection conn = ds.getConnection();

        // insert a record
        // conn.prepareStatement("replace into sample_table (id,name,address) values (1,'sun','hz')").executeUpdate();

        System.out.println("insert done");

        // select all records
        PreparedStatement ps = conn.prepareStatement("SELECT * from sample_table");

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
