package com.taobao.tddl.sample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.taobao.tddl.client.jdbc.TDataSource;

public class TianxinSample {

    public static void main(String[] args) throws Exception {

        com.taobao.tddl.client.jdbc.TDataSource ds = new TDataSource();
        ds.setDynamicRule(true);
        // init a datasource with dynamic config on diamond
        ds.setAppName("DDBASE_APP");
        ds.setAppRuleFile("tianxin.xml");
        ds.init();

        System.out.println("init done");

        Connection conn = ds.getConnection();

        // insert a record
        // conn.prepareStatement("replace into sample_table (id,name,address) values (1,'sun','hz')").executeUpdate();

        System.out.println("insert done");

        // select all records
        PreparedStatement ps = conn.prepareStatement(" SELECT  *FROM  (SELECT ID FROM dd_auction  WHERE  USER_ID = 1   AND  STORE_ID = 2   AND             name like CONCAT('%','123','%')  ORDER BY GMT_MODIFIED DESC LIMIT 1 ,10 ) d , dd_auction d1 WHERE  d.ID =  d1.ID        ");

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
