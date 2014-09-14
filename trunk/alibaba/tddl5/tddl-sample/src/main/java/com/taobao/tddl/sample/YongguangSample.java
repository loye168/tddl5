package com.taobao.tddl.sample;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Date;

import com.taobao.tddl.client.jdbc.TDataSource;

public class YongguangSample {

    public static void main(String[] args) throws Exception {

        com.taobao.tddl.client.jdbc.TDataSource ds = new TDataSource();
        ds.setDynamicRule(true);
        // init a datasource with dynamic config on diamond
        ds.setAppName("TMALL_SERVICE_DPC");
        // Map cp = new HashMap();
        // cp.put(ConnectionProperties.MERGE_CONCURRENT, "true");
        // ds.setConnectionProperties(cp);
        ds.init();

        System.out.println("init done");

        Connection conn = ds.getConnection();
        Date gmt = new Date(1350304585380l);
        PreparedStatement ps = conn.prepareStatement("replace into ob_normaltbl_onegroup_oneatom ( PK, ID, GMT_CREATE, GMT_TIMESTAMP, GMT_DATETIME, NAME, FLOATCOL) values ( ?, ?, ?, ?, ?, ?, ?) ");

        // PreparedStatement ps =
        // conn.prepareStatement("REPLACE INTO ob_normaltbl_oneGroup_oneAtom (pk,id,gmt_create,gmt_timestamp,gmt_datetime,name,floatCol) VALUES(0,0,'2012-10-15 00:00:00','2012-10-15 20:36:25','2012-10-15 20:36:25','zhuoxue',1.1)");
        // // ps.setObject(1, gmt);
        Date gmtDay = new Date(1350230400000l);
        ps.setObject(1, 0);
        ps.setObject(2, 0);
        ps.setObject(3, "2012-10-15 00:00:00");
        ps.setObject(4, "2012-10-15 20:36:25");
        ps.setObject(5, "2012-10-15 20:36:25");
        ps.setObject(6, "zhuoxue");
        ps.setObject(7, new BigDecimal("1.1"));
        // ps.execute();
        ps.executeUpdate();

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

        ps.close();
        conn.close();

        System.out.println("query done");

    }

}
