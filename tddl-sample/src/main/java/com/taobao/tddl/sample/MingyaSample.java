package com.taobao.tddl.sample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.matrix.jdbc.TDataSource;

public class MingyaSample {

    public static void main(String[] args) throws TddlException, SQLException {

        TDataSource ds = new TDataSource();

        // init a datasource with dynamic config on diamond
        ds.setAppName("CREDITCTRL_APP");

        Map cp = new HashMap();
        // cp.put(ConnectionProperties.MERGE_CONCURRENT, "false");

        ds.setConnectionProperties(cp);
        ds.init();

        System.out.println("init done");

        Connection conn = ds.getConnection();

        System.out.println("insert done");

        // select all records
        PreparedStatement ps = conn.prepareStatement("SELECT * from SYS_LOG");
        // PreparedStatement ps =
        // conn.prepareStatement("SELECT device_id from bullet_dt_device_content  limit 10");

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
