package com.taobao.tddl.sample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import com.taobao.tddl.client.jdbc.TDataSource;
import com.taobao.tddl.rule.TableRule;

/**
 * Tddl5.1版本之后的全量dump例子
 * 
 * @author jianghang 2014-6-23 下午1:30:16
 * @since 5.1.0
 */
public class DumpSample {

    public static void main(String args[]) throws Exception {
        String appName = "WAP_PUSH_USER_APP";
        String logicTableName = "push_app_user";

        // 初始化tddl数据源
        TDataSource ds = new TDataSource();
        ds.setAppName(appName);
        ds.setDynamicRule(true);
        ds.init();

        TableRule rule = ds.getConfigHolder().getOptimizerContext().getRule().getTableRule(logicTableName);
        for (Map.Entry<String, Set<String>> entry : rule.getActualTopology().entrySet()) {
            String groupKey = entry.getKey();
            for (String realTableName : entry.getValue()) {
                executePerTable(ds, logicTableName, groupKey, realTableName);
            }
        }
    }

    /**
     * <pre>
     * 使用tddl hint指定具体的库表执行sql。
     * 1.JDBC用户直接在sql前拼装hint
     * 2.IBATIS用户可以通过##的占位符替换方式，将hint写在xml配置文件中
     * </pre>
     * 
     * @param ds
     * @param groupKey
     * @param realTableName
     * @throws SQLException
     */
    public static void executePerTable(DataSource ds, String logicTableName, String groupKey, String realTableName)
                                                                                                                   throws SQLException {
        String sql = "/*+TDDL({'type':'direct','vtab':?,'dbid':?,'realtabs':[?]})*/ select * from " + logicTableName
                     + " limit ?";
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = ds.getConnection();
            ps = conn.prepareStatement(sql);

            // 第一个参数是逻辑表名，表示sql中的这个表名要进行替换
            ps.setString(1, logicTableName);
            // 第二个参数是groupKey，即在这个库上执行
            ps.setString(2, groupKey);
            // 第三个参数是具体表名，逻辑表名将会被替换成该名字执行sql
            ps.setString(3, realTableName);
            // 第四个参数是sql中的limit值。
            ps.setInt(4, 10);

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
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (ps != null) {
                ps.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
    }
}
