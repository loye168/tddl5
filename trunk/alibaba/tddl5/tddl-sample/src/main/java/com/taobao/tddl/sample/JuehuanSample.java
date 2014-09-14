package com.taobao.tddl.sample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.taobao.tddl.client.jdbc.TDataSource;

public class JuehuanSample {

    public static void main(String[] args) throws Exception {

        TDataSource ds = new TDataSource();
        ds.setDynamicRule(true);
        // init a datasource with dynamic config on diamond
        ds.setAppName("TMALL_PUSH_APP");
        ds.setAppRuleFile("juehuan.xml");
        ds.init();

        System.out.println("init done");

        Connection conn = ds.getConnection();
        conn.prepareStatement(" insert into push_device(gmt_create,gmt_modified,device_token,sys_platform,device_imei,device_imsi,app_version,app_channel,device_uuid,device_token_hash,user_id,allow_push,start_hour,end_hour) values(now(),now(),'test9',0,'imei','imsi',0,0,'uuid',0,12,1,0,23)")
            .executeUpdate();
        PreparedStatement ps = conn.prepareStatement("/*+TDDL_GROUP({groupIndex:1})*/select * from push_device where device_token='test9'");

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
