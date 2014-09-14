package com.taobao.tddl.sample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.taobao.tddl.client.jdbc.TDataSource;

public class CBU_IPAGE_HBASE {

    public static void main(String[] args) throws Exception {

        TDataSource ds = new TDataSource();

        ds.setAppName("CBU_IPAGE_HBASE");

        Map<String, Object> cp = new HashMap();

        ds.setConnectionProperties(cp);
        ds.init();

        Connection conn = ds.getConnection();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = sdf.parse("2013-09-16 00:00:00");
        PreparedStatement stmt = conn.prepareStatement("select  CN_IPAGE_WP_TREND.PV_CNT_1D_001 as pv_cnt_1d_001, CN_IPAGE_WP_TREND.REAL_URL as real_url, CN_IPAGE_WP_TREND.STAT_DATE as stat_date from    CN_IPAGE_WP_TREND where (CN_IPAGE_WP_TREND.REAL_URL='80af291dbd133f14506fdbcf9f29d698') and ((CN_IPAGE_WP_TREND.STAT_DATE>='2014-01-07' and CN_IPAGE_WP_TREND.STAT_DATE<='2014-01-07')) limit 0,450");

        stmt.setObject(1, date);
        stmt.setLong(2, 100065650L);

        ResultSet rs = stmt.executeQuery();

        while (rs.next()) {
            StringBuilder sb = new StringBuilder();
            int count = rs.getMetaData().getColumnCount();
            // // rs.getObject("uv_cnt_tt_187_ol_pc");
            // rs.getObject("uv_pc");
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
