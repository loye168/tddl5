package com.taobao.tddl.sample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.taobao.tddl.client.jdbc.TDataSource;
import com.taobao.tddl.common.model.App;

public class AndorShowSample {

    public static void main(String[] args) throws Exception {

        TDataSource ds = new TDataSource();

        // init a datasource with dynamic config on diamond\
        ds.setAppName("DAILY_SUBWAY_MYSQL");

        App subApp = new App();
        subApp.setAppName("DAILY_SOLAR_MERCURY_APP");
        subApp.setRuleFile("DAILY_SOLAR_MERCURY_APP_rule.xml");
        // subApp.setSchemaFile("xx.xml");

        // subApp.setTopologyFile("xxx.xml");
        ds.addSubApp(subApp);

        // Map cp = new HashMap();
        // cp.put(ConnectionProperties.OPTIMIZER_CACHE_SIZE, 2000);
        // ds.setConnectionProperties(cp);
        ds.init();

        System.out.println("init done");

        {
            Connection conn = ds.getConnection();

            PreparedStatement ps = conn.prepareStatement("select a.*, c.* from lunatmpcustomer b, lunaadgroup a, adgroup c where a.custid=b.custid  and a.CUSTID=1102000076 and b.custid=1102000076 and a.id=1671516  and c.member_id=b.memberid and c.id=1671516");

            // PreparedStatement ps =
            // conn.prepareStatement("insert into adam_stream_sample_collect(id,gmt_create,gmt_modified,index_id,report_time,data_value,date_time,dimension_value,combine_key ) values(99999,'2014-03-05','2014-03-05',11,'2014-03-05',11,'2014-03-05','aaa','bb')");

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

        //
        // {
        // Connection conn = ds.getConnection();
        //
        // PreparedStatement ps =
        // conn.prepareStatement("select sum(case when index_id=100013 then data_value else 1 end) pv,dimension_value countryId,sum(data_value) payGmv from adam_stream_sample_collect where index_id = 110008 and  date_time >='2014-02-25' and date_time <'2014-02-26' group by dimension_value  ");
        //
        // // PreparedStatement ps =
        // //
        // conn.prepareStatement("insert into adam_stream_sample_collect(id,gmt_create,gmt_modified,index_id,report_time,data_value,date_time,dimension_value,combine_key ) values(99999,'2014-03-05','2014-03-05',11,'2014-03-05',11,'2014-03-05','aaa','bb')");
        //
        // ResultSet rs = ps.executeQuery();
        // while (rs.next()) {
        // StringBuilder sb = new StringBuilder();
        // int count = rs.getMetaData().getColumnCount();
        // for (int i = 1; i <= count; i++) {
        //
        // String key = rs.getMetaData().getColumnLabel(i);
        // Object val = rs.getObject(i);
        // sb.append("[" + rs.getMetaData().getTableName(i) + "." + key + "->" +
        // val + "]");
        // }
        // System.out.println(sb.toString());
        // }
        //
        // rs.close();
        // ps.close();
        // conn.close();
        // System.out.println("query done");
        // }
        //
        // {
        // Connection conn = ds.getConnection();
        //
        // PreparedStatement ps =
        // conn.prepareStatement("select * from bmw_users limit 10");
        //
        // // PreparedStatement ps =
        // //
        // conn.prepareStatement("insert into adam_stream_sample_collect(id,gmt_create,gmt_modified,index_id,report_time,data_value,date_time,dimension_value,combine_key ) values(99999,'2014-03-05','2014-03-05',11,'2014-03-05',11,'2014-03-05','aaa','bb')");
        //
        // ResultSet rs = ps.executeQuery();
        // while (rs.next()) {
        // StringBuilder sb = new StringBuilder();
        // int count = rs.getMetaData().getColumnCount();
        // for (int i = 1; i <= count; i++) {
        //
        // String key = rs.getMetaData().getColumnLabel(i);
        // Object val = rs.getObject(i);
        // sb.append("[" + rs.getMetaData().getTableName(i) + "." + key + "->" +
        // val + "]");
        // }
        // System.out.println(sb.toString());
        // }
        //
        // rs.close();
        // ps.close();
        // conn.close();
        // System.out.println("query done");
        // }
        //
        // {
        // Connection conn = ds.getConnection();
        //
        // // insert a record
        // conn.prepareStatement("replace into sample_table (id,name,address) values (1,'sun','hz')").executeUpdate();
        //
        // System.out.println("insert done");
        //
        // // select all records
        // PreparedStatement ps =
        // conn.prepareStatement("SELECT * from sample_table");
        //
        // ResultSet rs = ps.executeQuery();
        // while (rs.next()) {
        // StringBuilder sb = new StringBuilder();
        // int count = rs.getMetaData().getColumnCount();
        // for (int i = 1; i <= count; i++) {
        //
        // String key = rs.getMetaData().getColumnLabel(i);
        // Object val = rs.getObject(i);
        // sb.append("[" + rs.getMetaData().getTableName(i) + "." + key + "->" +
        // val + "]");
        // }
        // System.out.println(sb.toString());
        // }
        //
        // rs.close();
        // ps.close();
        // conn.close();
        //
        // System.out.println("query done");
        // }
        //
        // {
        // Connection conn = ds.getConnection();
        // SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        // Date date = sdf.parse("2013-09-16 00:00:00");
        // PreparedStatement stmt =
        // conn.prepareStatement("select  CN_IPAGE_WP_TREND.PV_CNT_1D_001 as pv_cnt_1d_001, CN_IPAGE_WP_TREND.REAL_URL as real_url, CN_IPAGE_WP_TREND.STAT_DATE as stat_date from    CN_IPAGE_WP_TREND where (CN_IPAGE_WP_TREND.REAL_URL='80af291dbd133f14506fdbcf9f29d698') and ((CN_IPAGE_WP_TREND.STAT_DATE>='2014-01-07' and CN_IPAGE_WP_TREND.STAT_DATE<='2014-01-07')) limit 0,450");
        //
        // stmt.setObject(1, date);
        // stmt.setLong(2, 100065650L);
        //
        // ResultSet rs = stmt.executeQuery();
        //
        // while (rs.next()) {
        // StringBuilder sb = new StringBuilder();
        // int count = rs.getMetaData().getColumnCount();
        // // // rs.getObject("uv_cnt_tt_187_ol_pc");
        // // rs.getObject("uv_pc");
        // for (int i = 1; i <= count; i++) {
        //
        // String key = rs.getMetaData().getColumnLabel(i);
        // Object val = rs.getObject(i);
        // sb.append("[" + rs.getMetaData().getTableName(i) + "." + key + "->" +
        // val + "]");
        // }
        // System.out.println(sb.toString());
        // }
        // rs.close();
        // stmt.close();
        // conn.close();
        // }
    }

}
