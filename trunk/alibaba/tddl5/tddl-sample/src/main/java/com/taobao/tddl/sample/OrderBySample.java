package com.taobao.tddl.sample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import com.taobao.tddl.client.jdbc.TDataSource;
import com.taobao.tddl.common.properties.ConnectionProperties;

public class OrderBySample {

    public static void main(String[] args) throws Exception {
        TDataSource ds = new TDataSource();

        ds.setAppName("ICBU_DA_PBSVR_DEV_APP");

        ds.setDynamicRule(true);

        Map cp = new HashMap();
        cp.put(ConnectionProperties.ALLOW_TEMPORARY_TABLE, "true");
        cp.put(ConnectionProperties.CHOOSE_TEMPORARY_TABLE, "true");

        ds.setConnectionProperties(cp);
        ds.init();

        System.out.println("init done");
        Connection conn = ds.getConnection();
        // insert a record
        // conn.prepareStatement("replace into sample_table (id,name,address) values (1,'sun','hz')").executeUpdate();
        System.out.println("insert done");
        // select all records
        PreparedStatement ps = conn.prepareStatement("replace into tddl_category (id,name,gmt_modified,gmt_create) values (1,'aa',now(),now())");
        // ps.executeUpdate();

        ps = conn.prepareStatement("select ADL_DM_MDM_MEM_PROD_EFFECT_SDT0.STAT_DATE,ADL_DM_MDM_MEM_PROD_EFFECT_SDT0.SUM_PROD_SHOW_NUM,ADL_DM_MDM_MEM_PROD_EFFECT_SDT0.SUM_PROD_CLICK_NUM,ADL_DM_MDM_MEM_PROD_EFFECT_SDT0.SUM_PROD_FB_NUM,ADL_DM_MDM_MEM_PROD_EFFECT_SDT0.SUM_PROD_VISITOR_CNT from adl_dm_mdm_mem_prod_effect_sdt0 ADL_DM_MDM_MEM_PROD_EFFECT_SDT0 order by ADL_DM_MDM_MEM_PROD_EFFECT_SDT0.STAT_DATE asc  limit 0,100");
        // ps.setDate(1, new Date(System.currentTimeMillis()));
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
