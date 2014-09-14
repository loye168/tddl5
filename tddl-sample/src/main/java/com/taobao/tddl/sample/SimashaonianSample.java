package com.taobao.tddl.sample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.matrix.jdbc.TDataSource;

public class SimashaonianSample {

    public static void main(String[] args) throws TddlException, SQLException {

        TDataSource ds = new TDataSource();

        // init a datasource with dynamic config on diamond
        ds.setAppName("ACSS_BILL_APP");

        ds.init();

        System.out.println("init done");

        Connection conn = ds.getConnection();

        // insert a record
        // conn.prepareStatement("replace into sample_table (id,name,address) values (1,'sun','hz')").executeUpdate();

        // System.out.println("insert done");

        // select all records
        PreparedStatement ps = conn.prepareStatement(" select * from bill_detail BILL_DETAIL limit 100,200");

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
