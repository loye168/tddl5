package com.taobao.tddl.sample;

import java.sql.Connection;
import java.sql.PreparedStatement;

import com.taobao.tddl.client.jdbc.TDataSource;

public class BolinSample {

    public static void main(String[] args) throws Exception {

        TDataSource ds = new TDataSource();
        ds.setDynamicRule(true);
        // init a datasource with dynamic config on diamond
        ds.setAppName("SC_P4P_APP");
        ds.setDynamicRule(true);
        ds.init();

        System.out.println("init done");

        Connection conn = ds.getConnection();

        PreparedStatement ps = conn.prepareStatement("/*+TDDL({type:executeByCondition,parameters:[\"cust_id=?;l\"],virtualTableName:p4p_ad_match})*/       insert ignore into p4p_ad_match (id, gmt_create, gmt_modified, offer_id, keyword_id, mlr_score,       cust_id, word_level, is_preferential,is_delete,qs_level)       values (?, now(), now(), ?,       ?, ?, ?, ?,       ?,?,?)    ");
        ps.setObject(1, 1);
        ps.setObject(2, 1);
        ps.setObject(3, 1);
        ps.setObject(4, 1);
        ps.setObject(5, 1);
        ps.setObject(6, 1);
        ps.setObject(7, 1);
        ps.setObject(8, 1);
        ps.setObject(9, 1);
        ps.setObject(10, 1);
        ps.executeUpdate();

        ps.close();
        conn.close();

        System.out.println("query done");

    }

}
