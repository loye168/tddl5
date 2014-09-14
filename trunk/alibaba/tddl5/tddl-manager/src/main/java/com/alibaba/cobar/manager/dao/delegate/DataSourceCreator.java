package com.alibaba.cobar.manager.dao.delegate;

import javax.sql.DataSource;

import org.springframework.beans.factory.InitializingBean;

import com.alibaba.druid.pool.DruidDataSource;

public class DataSourceCreator implements DataSourceFactory, InitializingBean {

    private final String driverClassName               = "com.mysql.jdbc.Driver";
    private int          maxActive                     = 50;
    private int          minIdle                       = 0;
    private long         timeBetweenEvictionRunsMillis = 60 * 1000;              // 1min
    // private int numTestsPerEvictionRun = -1;
    private long         minEvictableIdleTimeMillis    = 180 * 1000;             // 3min

    public void setMaxActive(int maxActive) {
        this.maxActive = maxActive;
    }

    public void setMinIdle(int minIdle) {
        this.minIdle = minIdle;
    }

    public void setTimeBetweenEvictionRunsMillis(long timeBetweenEvictionRunsMillis) {
        this.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis;
    }

    // public void setNumTestsPerEvictionRun(int numTestsPerEvictionRun) {
    // this.numTestsPerEvictionRun = numTestsPerEvictionRun;
    // }

    public void setMinEvictableIdleTimeMillis(long minEvictableIdleTimeMillis) {
        this.minEvictableIdleTimeMillis = minEvictableIdleTimeMillis;
    }

    @Override
    public DataSource createDataSource(String ip, int port, String user, String password) {

        DruidDataSource ds = new DruidDataSource();
        ds.setUsername(user);
        ds.setPassword(password);
        ds.setUrl(new StringBuilder().append("jdbc:mysql://")
            .append(ip)
            .append(":")
            .append(port)
            .append("/")
            .toString());
        ds.setDriverClassName(driverClassName);
        ds.setMaxActive(maxActive);
        ds.setMinIdle(minIdle);
        ds.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
        // ds.setNumTestsPerEvictionRun(numTestsPerEvictionRun);
        ds.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);

        return ds;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        // TODO Auto-generated method stub

    }
}
