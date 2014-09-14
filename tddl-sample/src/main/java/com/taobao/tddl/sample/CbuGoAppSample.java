package com.taobao.tddl.sample;

import java.sql.ResultSet;

import com.taobao.tddl.client.jdbc.TDataSource;
import com.taobao.tddl.matrix.jdbc.TConnection;
import com.taobao.tddl.matrix.jdbc.TPreparedStatement;

public class CbuGoAppSample {

    public static void main(String[] args) throws Exception {

        com.taobao.tddl.client.jdbc.TDataSource ds = new TDataSource();
        // init a datasource with dynamic config on diamond
        ds.setAppName("CBU_GO_APP");
        ds.init();
        System.out.println("init done");
        TConnection conn = ds.getConnection();
        // insert a record
        // conn.prepareStatement("replace into sample_table (id,name,address) values (1,'sun','hz')").executeUpdate();
        System.out.println("insert done");
        // select all records
        // TPreparedStatement ps =
        // conn.prepareStatement("SELECT * FROM buyer_quotation WHERE buyer_member_id = 'zhoulhtest001' AND status IN ('sent') AND (buyer_mark IS NULL OR buyer_mark NOT IN ('buyer_deleted' )) AND purchase_id IN ('850020079' ) ORDER BY gmt_create DESC");
        TPreparedStatement ps = conn.prepareStatement("SELECT * FROM buyer_quotation WHERE buyer_member_id = 'zhoulhtest001' AND status IN ('sent') AND (buyer_mark IS NULL OR buyer_mark NOT IN ('buyer_deleted' )) AND purchase_id IN ('850020079' ) ORDER BY gmt_create DESC");
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
