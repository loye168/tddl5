package com.taobao.tddl.sample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Date;

import com.alipay.oceanbase.OceanbaseDataSourceProxy;

public class ObSample {

    public static void main(String[] args) throws Exception {

        Date gmt = new Date(1350304585380l);
        long start = System.currentTimeMillis();
        OceanbaseDataSourceProxy ds = new OceanbaseDataSourceProxy();
        ds.setConfigURL("http://obconsole.test.alibaba-inc.com/ob-config/config.co?dataId=wireless_message");
        ds.init();
        System.out.println("init done");

        Connection conn = ds.getConnection();

        // select all records
        PreparedStatement ps = conn.prepareStatement("select * from ob_normaltbl_onegroup_oneatom ");

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
        System.out.println("done " + (System.currentTimeMillis() - start));
        rs.close();
        ps.close();
        conn.close();

        System.out.println("query done");
    }

}
