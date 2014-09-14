package com.taobao.tddl.sample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.matrix.jdbc.TDataSource;

public class InsightLifeSample {

    public static void main(String[] args) throws TddlException, SQLException {

        TDataSource ds = new TDataSource();

        // init a datasource with dynamic config on diamond
        ds.setAppName("INTL_COMM_DEV_APP");

        ds.init();

        System.out.println("init done");

        Connection conn = ds.getConnection();

        // select all records
        PreparedStatement ps = conn.prepareStatement("SELECT * from ae_macro_daily_target a left join ae_macro_daily_target_value b on a.target_id=b.target_id where a.type=? and b.stat_date=? order by a.target_id desc");

        ps.setObject(1, "'ae_byr'");
        ps.setObject(2, "2013-12-11");
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
