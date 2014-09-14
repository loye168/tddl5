package com.taobao.tddl.sample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.taobao.tddl.client.jdbc.TDataSource;
import com.taobao.tddl.common.model.App;

public class XihuiSample {

    public static void main(String[] args) throws Exception {

        TDataSource ds = new TDataSource();

        // init a datasource with dynamic config on diamond
        ds.setAppName("DAILY_SUBWAY_MYSQL");

        App subApp = new App();
        subApp.setAppName("DAILY_SOLAR_MERCURY_APP");
        subApp.setRuleFile("DAILY_SOLAR_MERCURY_APP_rule.xml");

        ds.addSubApp(subApp);

        ds.init();

        System.out.println("init done");

        {
            Connection conn = ds.getConnection();

            PreparedStatement ps = conn.prepareStatement("select a.*, c.* from lunatmpcustomer b, lunaadgroup a, adgroup c where a.custid=b.custid  and a.CUSTID=1102000076 and b.custid=1102000076 and a.id=1671516  and c.member_id=b.memberid and c.id=1671516");

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

}
