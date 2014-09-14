package com.taobao.tddl.sample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.taobao.tddl.client.jdbc.TDataSource;

public class CBU_IPAGE_HBASE {

    public static void main(String[] args) throws Exception {
        TDataSource ds = new TDataSource();
        ds.setAppName("CBU_IPAGE_HBASE");
        ds.init();

        Connection conn = ds.getConnection();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = sdf.parse("2013-09-16 00:00:00");
        PreparedStatement stmt1 = conn.prepareStatement("insert into CN_IPAGE_WP_TREND (PV_CNT_1D_001,REAL_URL,STAT_DATE) values (?,?,?)");

        stmt1.setString(1, "100065650");
        stmt1.setObject(2, "abc");
        stmt1.setObject(3, date);
        stmt1.executeUpdate();

        PreparedStatement stmt = conn.prepareStatement("select  CN_IPAGE_WP_TREND.PV_CNT_1D_001 as pv_cnt_1d_001, CN_IPAGE_WP_TREND.REAL_URL as real_url, CN_IPAGE_WP_TREND.STAT_DATE as stat_date from    CN_IPAGE_WP_TREND  limit 0,450");
        ResultSet rs = stmt.executeQuery();
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
        stmt.close();
        conn.close();
    }
}
