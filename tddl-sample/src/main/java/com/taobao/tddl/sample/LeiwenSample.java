package com.taobao.tddl.sample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.properties.ConnectionProperties;
import com.taobao.tddl.matrix.jdbc.TDataSource;

public class LeiwenSample {

    public static void main(String[] args) throws TddlException, SQLException {

        TDataSource ds = new TDataSource();

        // init a datasource with dynamic config on diamond
        ds.setAppName("XIAMI_WEB_ATHENA");

        Map cp = new HashMap();
        cp.put(ConnectionProperties.ALLOW_TEMPORARY_TABLE, "true");
        // cp.put(ConnectionProperties.MERGE_CONCURRENT, "true");
        ds.setConnectionProperties(cp);
        ds.init();

        System.out.println("init done");

        Connection conn = ds.getConnection();

        // select all records
        PreparedStatement ps = conn.prepareStatement("select user_id,num from (SELECT user_id, count(*) AS num FROM receive_flowers GROUP BY user_id  having num >= 100) t ORDER BY num desc");

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
