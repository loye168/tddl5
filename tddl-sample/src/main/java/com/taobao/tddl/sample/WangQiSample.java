package com.taobao.tddl.sample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.taobao.tddl.client.jdbc.TDataSource;

public class WangQiSample {

    public static void main(String[] args) throws Exception {

        com.taobao.tddl.client.jdbc.TDataSource ds = new TDataSource();
        // init a datasource with dynamic config on diamond
        ds.setAppName("WANGQI_APP");
        ds.setRuleFile("wangqi-rule.xml");
        ds.init();

        System.out.println("init done");
        Connection conn = ds.getConnection();

        // select all records
        int affect = 0;
        PreparedStatement ps = null;
        ps = conn.prepareStatement("insert into wq_team_task(id,gmt_create,gmt_modified,project_id,board_id,title,priority,end_time,status,risk,finish_time) values(?,?,?,?,?,?,?,?,?,?,?)");
        for (int i = 0; i < 2; i++) {
            ps.setObject(1, 1L + i);
            ps.setObject(2, "2014-06-18 15:24:46");
            ps.setObject(3, "2014-06-18 15:24:46");
            ps.setObject(4, 1L + i);
            ps.setObject(5, 1L);
            ps.setObject(6, "ljhtest");
            ps.setObject(7, 1L);
            ps.setObject(8, "2014-06-18 15:24:46");
            ps.setObject(9, 1L);
            ps.setObject(10, 1L);
            ps.setObject(11, "2014-06-18 15:24:46");
            ps.addBatch();
        }
        int batchAffect[] = ps.executeBatch();
        for (int i : batchAffect) {
            affect += i;
        }
        System.out.println("insert affect = " + affect);
        ps.close();

        for (int index = 1; index <= 2; index++) {
            ps = conn.prepareStatement("select * from wq_team_task where id = " + index + " and PROJECT_ID = " + index);
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
            System.out.println("query done");

            ps = conn.prepareStatement("delete from wq_team_task where id = " + index + " and PROJECT_ID = " + index);
            affect = ps.executeUpdate();
            System.out.println("delete affect = " + affect);
        }
        conn.close();
    }
}
