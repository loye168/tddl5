package com.taobao.tddl.sample;

import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.springframework.util.Log4jConfigurer;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.model.App;
import com.taobao.tddl.common.properties.ConnectionProperties;
import com.taobao.tddl.matrix.jdbc.TDataSource;
import com.taobao.tddl.util.SqlFileUtil;

public class Simba2Sample {

    public static void main(String[] args) throws TddlException, SQLException, FileNotFoundException {
        Log4jConfigurer.initLogging("src/main/resources/log4j.properties");

        TDataSource ds = new TDataSource();

        // 设置默认db(ob)
        ds.setAppName("DEV_DPS_APP");
        ds.setTopologyFile("tddl-topology-dps.xml");
        ds.setRuleFile("tddl-rule-dps-nonmysql.xml");

        // 设置simba2的mysql
        App subApp = new App();
        subApp.setAppName("DAILY_SOLAR_MERCURY_APP");
        subApp.setRuleFile("tddl-rule-dps-simba2-mysql.xml");
        ds.addSubApp(subApp);

        // 添加subway的mysql
        subApp = new App();
        subApp.setAppName("DEV_SUBWAY_MYSQL");
        subApp.setRuleFile("tddl-rule-dps-subway-mysql.xml");
        ds.addSubApp(subApp);

        Map cp = new HashMap();
        cp.put("ALLOW_TEMPORARY_TABLE", "True");
        cp.put(ConnectionProperties.TEMP_TABLE_DIR, ".\\temp\\");
        cp.put(ConnectionProperties.TEMP_TABLE_CUT_ROWS, false);
        cp.put(ConnectionProperties.TEMP_TABLE_MAX_ROWS, 1000);
        ds.setConnectionProperties(cp);

        ds.init();
        System.out.println("init done");

        // subway_adgroup_list.sql
        // solar_adgroup_list.sql
        String sql = SqlFileUtil.getSql("replace.txt");
        // sql = SqlFileUtil.getSql("solar_adgroup_list.sql");
        Connection conn = ds.getConnection();
        {
            PreparedStatement ps = conn.prepareStatement(sql);
            long start = System.currentTimeMillis();
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
        }

        conn.close();
    }

}
