package com.taobao.tddl.sample;

import java.sql.SQLException;

import com.taobao.tddl.common.exception.TddlException;

public class MaxTest {

    public static void main(String[] args) throws TddlException, SQLException {

        System.out.println("0" + 1);
        // TDataSource ds = new TDataSource();
        // ds.setAppName("notify_msg_track_mix");
        // ds.init();
        //
        // System.out.println("init done");
        // long start = System.currentTimeMillis();
        // Connection conn = ds.getConnection();
        // int k = 0;
        //
        // while (k++ < 10000000) {
        // PreparedStatement ps =
        // conn.prepareStatement("select max(gmt_create_ms) from notify_msg where gmt_create_days = ?");
        // ps.setInt(1, 16119);
        //
        // ResultSet rs = ps.executeQuery();
        // while (rs.next()) {
        // // StringBuilder sb = new StringBuilder();
        // // int count = rs.getMetaData().getColumnCount();
        // // for (int i = 1; i <= count; i++) {
        // //
        // // String key = rs.getMetaData().getColumnLabel(i);
        // // Object val = rs.getObject(i);
        // // sb.append("[" + rs.getMetaData().getTableName(i) + "." + key
        // // + "->" + val + "]");
        // // }
        // // System.out.println(sb.toString());
        // }
        //
        // rs.close();
        // ps.close();
        //
        // }
        // System.out.println("done " + (System.currentTimeMillis() - start));
    }

}
