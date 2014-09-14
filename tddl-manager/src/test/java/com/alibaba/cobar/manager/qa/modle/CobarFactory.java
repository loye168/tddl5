package com.alibaba.cobar.manager.qa.modle;

import java.io.IOException;
import java.util.Properties;

import com.alibaba.cobar.manager.dao.delegate.CobarAdapter;
import com.alibaba.druid.pool.DruidDataSource;

public class CobarFactory {

    public static CobarAdapter getCobarAdapter(String cobarNodeName) throws IOException {
        CobarAdapter cAdapter = new CobarAdapter();
        Properties prop = new Properties();
        prop.load(CobarFactory.class.getClassLoader().getResourceAsStream("cobarNode.properties"));
        DruidDataSource ds = new DruidDataSource();
        String user = prop.getProperty(cobarNodeName + ".user").trim();
        String password = prop.getProperty(cobarNodeName + ".password").trim();
        String ip = prop.getProperty(cobarNodeName + ".ip").trim();
        int managerPort = Integer.parseInt(prop.getProperty(cobarNodeName + ".manager.port").trim());
        int maxActive = -1;
        int minIdle = 0;
        long timeBetweenEvictionRunsMillis = 10 * 60 * 1000;
        // int numTestsPerEvictionRun = Integer.MAX_VALUE;
        long minEvictableIdleTimeMillis = DruidDataSource.DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS;
        ds.setUsername(user);
        ds.setPassword(password);
        ds.setUrl(new StringBuilder().append("jdbc:mysql://")
            .append(ip)
            .append(":")
            .append(managerPort)
            .append("/")
            .toString());
        ds.setDriverClassName("com.mysql.jdbc.Driver");
        ds.setMaxActive(maxActive);
        ds.setMinIdle(minIdle);
        ds.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
        // ds.setNumTestsPerEvictionRun(numTestsPerEvictionRun);
        ds.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);

        cAdapter.setDataSource(ds);
        return cAdapter;
    }

    public static SimpleCobarNode getSimpleCobarNode(String cobarNodeName) throws Exception {
        Properties prop = new Properties();
        prop.load(CobarFactory.class.getClassLoader().getResourceAsStream("cobarNode.properties"));
        String user = prop.getProperty(cobarNodeName + ".user").trim();
        String password = prop.getProperty(cobarNodeName + ".password").trim();
        String ip = prop.getProperty(cobarNodeName + ".ip").trim();
        int dmlPort = Integer.parseInt(prop.getProperty(cobarNodeName + ".dml.port").trim());
        int managerPort = Integer.parseInt(prop.getProperty(cobarNodeName + ".manager.port").trim());
        SimpleCobarNode sCobarNode = new SimpleCobarNode(ip, dmlPort, managerPort, user, password);
        return sCobarNode;
    }
}
