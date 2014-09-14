package com.taobao.tddl.atom.jdbc;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.taobao.tddl.atom.utils.AtomDataSourceHelper;
import com.taobao.tddl.atom.utils.ConnRestrictSlot;
import com.taobao.tddl.common.model.SqlMetaData;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

public class TConnectionWrapper implements Connection {

    private static Logger            log        = LoggerFactory.getLogger(TConnectionWrapper.class);
    private final Connection         targetConnection;
    private final TDataSourceWrapper dataSourceWrapper;
    private ConnRestrictSlot         connRestrictSlot;
    private Set<TStatementWrapper>   statements = Collections.synchronizedSet(new HashSet<TStatementWrapper>(1));
    private final String             appName;
    private SqlMetaData              sqlMetaData;

    public TConnectionWrapper(Connection targetConnection, ConnRestrictSlot connRestrictSlot,
                              TDataSourceWrapper dataSourceWrapper, String appName){
        this.targetConnection = targetConnection;
        this.connRestrictSlot = connRestrictSlot;
        this.dataSourceWrapper = dataSourceWrapper;
        this.appName = appName;
    }

    public Connection getTargetConnection() {
        return targetConnection;
    }

    @Override
    public void clearWarnings() throws SQLException {
        this.targetConnection.clearWarnings();
    }

    @Override
    public void close() throws SQLException {
        try {
            this.targetConnection.close();
        } finally {
            if (log.isDebugEnabled()) {
                StringBuilder sb = new StringBuilder();
                sb.append("appname : ").append(dataSourceWrapper.connectionProperties.datasourceName).append(" ");
                sb.append("threadcount : ").append(dataSourceWrapper.threadCount);
            }

            if (connRestrictSlot != null) {
                connRestrictSlot.freeConnection();
                connRestrictSlot = null; // 防止重复关闭
            }
            dataSourceWrapper.threadCount.decrementAndGet();
            AtomDataSourceHelper.removeConnRestrictKey();
        }

        for (TStatementWrapper statementWrapper : this.statements) {
            try {
                statementWrapper.close(false);
            } catch (SQLException e) {
                log.error("", e);
            }
        }
    }

    void removeOpenedStatements(Statement statement) {
        if (!statements.remove(statement)) {
            log.warn("current statmenet ：" + statement + " doesn't exist!");
        }
    }

    @Override
    public void commit() throws SQLException {
        this.targetConnection.commit();
    }

    @Override
    public Statement createStatement() throws SQLException {
        Statement targetStatement = this.targetConnection.createStatement();
        TStatementWrapper statementWrapper = new TStatementWrapper(targetStatement,
            this,
            this.dataSourceWrapper,
            this.appName);
        this.statements.add(statementWrapper);
        return statementWrapper;
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        Statement targetStatement = this.targetConnection.createStatement(resultSetType, resultSetConcurrency);
        TStatementWrapper statementWrapper = new TStatementWrapper(targetStatement,
            this,
            this.dataSourceWrapper,
            this.appName);
        this.statements.add(statementWrapper);
        return statementWrapper;
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
                                                                                                           throws SQLException {
        Statement s = this.targetConnection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
        TStatementWrapper statementWrapper = new TStatementWrapper(s, this, this.dataSourceWrapper, this.appName);
        this.statements.add(statementWrapper);
        return statementWrapper;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return this.targetConnection.getAutoCommit();
    }

    @Override
    public String getCatalog() throws SQLException {
        return this.targetConnection.getCatalog();
    }

    @Override
    public int getHoldability() throws SQLException {
        return this.targetConnection.getHoldability();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        DatabaseMetaData targetMetaData = this.targetConnection.getMetaData();
        return new DatabaseMetaDataWrapper(targetMetaData, this);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return this.targetConnection.getTransactionIsolation();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return this.targetConnection.getTypeMap();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return this.targetConnection.getWarnings();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.targetConnection.isClosed();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return this.targetConnection.isReadOnly();
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return this.targetConnection.nativeSQL(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        CallableStatement cs = targetConnection.prepareCall(sql);
        CallableStatementWrapper csw = new CallableStatementWrapper(cs, this, this.dataSourceWrapper, sql, this.appName);
        statements.add(csw);
        return csw;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        CallableStatement cs = this.targetConnection.prepareCall(sql, resultSetType, resultSetConcurrency);
        CallableStatementWrapper csw = new CallableStatementWrapper(cs, this, this.dataSourceWrapper, sql, this.appName);
        statements.add(csw);
        return csw;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        CallableStatement cs = this.targetConnection.prepareCall(sql,
            resultSetType,
            resultSetConcurrency,
            resultSetHoldability);
        CallableStatementWrapper csw = new CallableStatementWrapper(cs, this, this.dataSourceWrapper, sql, this.appName);
        statements.add(csw);
        return csw;
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        PreparedStatement ps = this.targetConnection.prepareStatement(sql);
        TPreparedStatementWrapper psw = new TPreparedStatementWrapper(ps,
            this,
            this.dataSourceWrapper,
            sql,
            this.appName);
        statements.add(psw);
        return psw;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        PreparedStatement ps = this.targetConnection.prepareStatement(sql, autoGeneratedKeys);
        TPreparedStatementWrapper psw = new TPreparedStatementWrapper(ps,
            this,
            this.dataSourceWrapper,
            sql,
            this.appName);
        statements.add(psw);
        return psw;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        PreparedStatement ps = this.targetConnection.prepareStatement(sql, columnIndexes);
        TPreparedStatementWrapper psw = new TPreparedStatementWrapper(ps,
            this,
            this.dataSourceWrapper,
            sql,
            this.appName);
        statements.add(psw);
        return psw;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        PreparedStatement ps = this.targetConnection.prepareStatement(sql, columnNames);
        TPreparedStatementWrapper psw = new TPreparedStatementWrapper(ps,
            this,
            this.dataSourceWrapper,
            sql,
            this.appName);
        statements.add(psw);
        return psw;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
                                                                                                      throws SQLException {
        PreparedStatement ps = this.targetConnection.prepareStatement(sql, resultSetType, resultSetConcurrency);
        TPreparedStatementWrapper psw = new TPreparedStatementWrapper(ps,
            this,
            this.dataSourceWrapper,
            sql,
            this.appName);
        statements.add(psw);
        return psw;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        PreparedStatement ps = this.targetConnection.prepareStatement(sql, resultSetType, resultSetConcurrency);
        TPreparedStatementWrapper psw = new TPreparedStatementWrapper(ps,
            this,
            this.dataSourceWrapper,
            sql,
            this.appName);
        statements.add(psw);
        return psw;
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        this.targetConnection.releaseSavepoint(savepoint);
    }

    @Override
    public void rollback() throws SQLException {
        this.targetConnection.rollback();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        this.targetConnection.rollback(savepoint);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        this.targetConnection.setAutoCommit(autoCommit);
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        this.targetConnection.setCatalog(catalog);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        this.targetConnection.setHoldability(holdability);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        this.targetConnection.setReadOnly(readOnly);
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return this.targetConnection.setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return this.targetConnection.setSavepoint(name);
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        this.targetConnection.setTransactionIsolation(level);

    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        this.targetConnection.setTypeMap(map);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) {
            return (T) this;
        } else {
            throw new SQLException("not a wrapper for " + iface);
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return this.getClass().isAssignableFrom(iface);
    }

    @Override
    public Clob createClob() throws SQLException {
        return this.targetConnection.createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
        return this.targetConnection.createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return this.targetConnection.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return this.targetConnection.createSQLXML();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return this.targetConnection.isValid(timeout);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        this.targetConnection.setClientInfo(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        this.targetConnection.setClientInfo(properties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return this.targetConnection.getClientInfo(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return this.targetConnection.getClientInfo();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return this.targetConnection.createArrayOf(typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return this.targetConnection.createStruct(typeName, attributes);
    }

    public SqlMetaData getSqlMetaData() {
        return sqlMetaData;
    }

    public void setSqlMetaData(SqlMetaData sqlMetaData) {
        this.sqlMetaData = sqlMetaData;
    }

}
