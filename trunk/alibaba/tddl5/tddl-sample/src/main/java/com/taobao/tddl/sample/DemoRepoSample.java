package com.taobao.tddl.sample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.matrix.jdbc.TDataSource;

public class DemoRepoSample {

    public static void main(String[] args) throws TddlException, SQLException {

        TDataSource ds = new TDataSource();

        // init a datasource with local config file
        ds.setAppName("tddl5_sample");
        ds.setRuleFile("classpath:sample_rule.xml");
        ds.setTopologyFile("sample_topology.xml");
        ds.setSchemaFile("demo_repo_schema.xml");
        ds.init();

        System.out.println("init done");

        Connection conn = ds.getConnection();

        // insert a record
        conn.prepareStatement("replace into _tddl_ (id,name) values (1,'sun1')").executeUpdate();
        conn.prepareStatement("replace into _tddl_ (id,name) values (2,'sun1')").executeUpdate();

        conn.prepareStatement("replace into _tddl_ (id,name) values (3,'sun1')").executeUpdate();

        conn.prepareStatement("replace into _tddl_ (id,name) values (4,'sun2')").executeUpdate();

        conn.prepareStatement("replace into _tddl_ (id,name) values (5,'sun2')").executeUpdate();

        System.out.println("insert done");

        // select all records
        PreparedStatement ps = conn.prepareStatement("SELECT id from _tddl_ order by id");
        // PreparedStatement ps =
        // conn.prepareStatement("SELECT * from _tddl_ t1 join _tddl_ t2 where t1.name=t2.name and t2.name='sun1' and t1.id=1");

        // PreparedStatement ps =
        // conn.prepareStatement("SELECT * from _tddl_ t1 where name='sun'");

        // PreparedStatement ps =
        // conn.prepareStatement("SELECT count(*)+1 from _tddl_ t1 where t1.id=1");
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
