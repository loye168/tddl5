package com.taobao.tddl.repo.oceanbase;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import com.alipay.oceanbase.OceanbaseDataSourceProxy;
import com.taobao.tddl.common.jdbc.IConnection;
import com.taobao.tddl.common.jdbc.IDataSource;

public class OBDataSourceWrapper extends OceanbaseDataSourceProxy implements IDataSource {

    OceanbaseDataSourceProxy ds;

    public OBDataSourceWrapper(OceanbaseDataSourceProxy ds){
        this.ds = ds;
    }

    @Override
    public int hashCode() {
        return ds.hashCode();
    }

    @Override
    public void init() throws Exception {
        ds.init();
    }

    @Override
    public boolean equals(Object obj) {
        return ds.equals(obj);
    }

    @Override
    public void destroy() throws Exception {
        ds.destroy();
    }

    @Override
    public void setUsername(String username) {
        ds.setUsername(username);
    }

    @Override
    public void setPassword(String password) {
        ds.setPassword(password);
    }

    @Override
    public void setConnectionProperties(String properties) {
        ds.setConnectionProperties(properties);
    }

    @Override
    public void setInitialSize(int initialSize) {
        ds.setInitialSize(initialSize);
    }

    @Override
    public void setMinIdle(int minIdle) {
        ds.setMinIdle(minIdle);
    }

    @Override
    public void setMaxActive(int maxActive) {
        ds.setMaxActive(maxActive);
    }

    @Override
    public void setMaxWait(long maxWait) {
        ds.setMaxWait(maxWait);
    }

    @Override
    public void setTestOnBorrow(boolean testOnBorrow) {
        ds.setTestOnBorrow(testOnBorrow);
    }

    @Override
    public void setTestOnReturn(boolean testOnReturn) {
        ds.setTestOnReturn(testOnReturn);
    }

    @Override
    public void setTestWhileIdle(boolean testWhileIdle) {
        ds.setTestWhileIdle(testWhileIdle);
    }

    @Override
    public void setTimeBetweenEvictionRunsMillis(long timeBetweenEvictionRunsMillis) {
        ds.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
    }

    @Override
    public void setMinEvictableIdleTimeMillis(long minEvictableIdleTimeMillis) {
        ds.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
    }

    @Override
    public void setQueryTimeout(int queryTimeout) {
        ds.setQueryTimeout(queryTimeout);
    }

    @Override
    public void setValidationQueryTimeout(int validationQueryTimeout) {
        ds.setValidationQueryTimeout(validationQueryTimeout);
    }

    @Override
    public void setValidationQuery(String validationQuery) {
        ds.setValidationQuery(validationQuery);
    }

    @Override
    public void setDefaultTransactionIsolation(boolean defaultTransactionIsolation) {
        ds.setDefaultTransactionIsolation(defaultTransactionIsolation);
    }

    @Override
    public void setDefaultReadOnly(boolean defaultReadOnly) {
        ds.setDefaultReadOnly(defaultReadOnly);
    }

    @Override
    public void setDefaultAutoCommit(boolean defaultAutoCommit) {
        ds.setDefaultAutoCommit(defaultAutoCommit);
    }

    @Override
    public void setConnectionErrorRetryAttempts(int connectionErrorRetryAttempts) {
        ds.setConnectionErrorRetryAttempts(connectionErrorRetryAttempts);
    }

    @Override
    public void setMaxWaitThreadCount(int maxWaitThreadCount) {
        ds.setMaxWaitThreadCount(maxWaitThreadCount);
    }

    @Override
    public void setMaxPoolPreparedStatementPerConnectionSize(int maxPoolPreparedStatementPerConnectionSize) {
        ds.setMaxPoolPreparedStatementPerConnectionSize(maxPoolPreparedStatementPerConnectionSize);
    }

    @Override
    public void setRemoveAbandonedTimeout(long removeAbandonedTimeout) {
        ds.setRemoveAbandonedTimeout(removeAbandonedTimeout);
    }

    @Override
    public String toString() {
        return ds.toString();
    }

    @Override
    public void setBreakAfterAcquireFailure(boolean breakAfterAcquireFailure) {
        ds.setBreakAfterAcquireFailure(breakAfterAcquireFailure);
    }

    @Override
    public void setConfigURL(String configURL) {
        ds.setConfigURL(configURL);
    }

    @Override
    public void setPeriod(int period) {
        ds.setPeriod(period);
    }

    @Override
    public Map<String, String> getConfigurations() {
        return ds.getConfigurations();
    }

    @Override
    public void setLevel(String level) {
        ds.setLevel(level);
    }

    @Override
    public void setDelayUnloadTime(int delayUnloadTime) {
        ds.setDelayUnloadTime(delayUnloadTime);
    }

    @Override
    public IConnection getConnection() throws SQLException {
        return new OBConnectionWrapper(ds.getConnection());
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return ds.getConnection(username, password);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return ds.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        ds.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        ds.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return ds.getLoginTimeout();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return ds.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return ds.isWrapperFor(iface);
    }

}
