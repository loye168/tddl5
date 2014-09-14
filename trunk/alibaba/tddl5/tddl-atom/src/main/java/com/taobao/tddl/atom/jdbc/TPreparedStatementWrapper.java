package com.taobao.tddl.atom.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import com.taobao.tddl.atom.utils.LoadFileUtils;
import com.taobao.tddl.common.jdbc.SqlTypeParser;
import com.taobao.tddl.monitor.unit.RouterUnitsHelper;

/**
 * preparedStatement 包装类
 * 
 * @author shenxun
 */
public class TPreparedStatementWrapper extends TStatementWrapper implements TPreparedStatement {

    protected final String sql;

    public TPreparedStatementWrapper(Statement targetStatement, TConnectionWrapper connectionWrapper,
                                     TDataSourceWrapper dataSourceWrapper, String sql, String appName){
        super(targetStatement, connectionWrapper, dataSourceWrapper, appName);
        this.sql = sql;
    }

    @Override
    public void addBatch() throws SQLException {
        ((PreparedStatement) targetStatement).addBatch();
    }

    @Override
    public void clearParameters() throws SQLException {
        ((PreparedStatement) targetStatement).clearParameters();
    }

    @Override
    public boolean execute() throws SQLException {
        if (SqlTypeParser.isQuerySql(sql)) {
            executeQuery();
            return true;
        } else {
            executeUpdate();
            return false;
        }
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        // if (sqlMetaData == null) throw new
        // NullPointerException("miss sql meta data.");
        ensureResultSetIsEmpty();
        recordReadTimes();
        increaseConcurrentRead();
        Exception e0 = null;

        startRpc(QUERY);
        try {
            currentResultSet = new TResultSetWrapper(this, ((PreparedStatement) targetStatement).executeQuery());
            return currentResultSet;
        } catch (SQLException e) {
            decreaseConcurrentRead();
            e0 = e;
            throw e;
        } finally {
            endRpc(sql, e0);
        }
    }

    @Override
    public int executeUpdate() throws SQLException {
        // if (sqlMetaData == null) throw new
        // NullPointerException("miss sql meta data.");
        ensureResultSetIsEmpty();
        recordWriteTimes();
        increaseConcurrentWrite();
        Exception e0 = null;

        startRpc(UPDATE);
        try {
            RouterUnitsHelper.unitDeployProtect(this.getAppName());
            return ((PreparedStatement) targetStatement).executeUpdate();
        } catch (SQLException e) {
            e0 = e;
            throw e;
        } finally {
            endRpc(sql, e0);
            decreaseConcurrentWrite();
        }
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        // 这里直接返回元数据
        return ((PreparedStatement) targetStatement).getMetaData();
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        // 这里直接返回原数据
        return ((PreparedStatement) targetStatement).getParameterMetaData();
    }

    @Override
    public void setArray(int i, Array x) throws SQLException {

        ((PreparedStatement) targetStatement).setArray(i, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        ((PreparedStatement) targetStatement).setAsciiStream(parameterIndex, x, length);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        ((PreparedStatement) targetStatement).setBigDecimal(parameterIndex, x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        ((PreparedStatement) targetStatement).setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public void setBlob(int i, Blob x) throws SQLException {
        ((PreparedStatement) targetStatement).setBlob(i, x);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        ((PreparedStatement) targetStatement).setBoolean(parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        ((PreparedStatement) targetStatement).setByte(parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        ((PreparedStatement) targetStatement).setBytes(parameterIndex, x);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        ((PreparedStatement) targetStatement).setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setClob(int i, Clob x) throws SQLException {
        ((PreparedStatement) targetStatement).setClob(i, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        ((PreparedStatement) targetStatement).setDate(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        ((PreparedStatement) targetStatement).setDate(parameterIndex, x, cal);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        ((PreparedStatement) targetStatement).setDouble(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        ((PreparedStatement) targetStatement).setFloat(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        ((PreparedStatement) targetStatement).setInt(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        ((PreparedStatement) targetStatement).setLong(parameterIndex, x);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        ((PreparedStatement) targetStatement).setNull(parameterIndex, sqlType);
    }

    @Override
    public void setNull(int paramIndex, int sqlType, String typeName) throws SQLException {
        ((PreparedStatement) targetStatement).setNull(paramIndex, sqlType, typeName);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        ((PreparedStatement) targetStatement).setObject(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        ((PreparedStatement) targetStatement).setObject(parameterIndex, x, targetSqlType);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scale) throws SQLException {
        ((PreparedStatement) targetStatement).setObject(parameterIndex, x, targetSqlType, scale);
    }

    @Override
    public void setRef(int i, Ref x) throws SQLException {
        ((PreparedStatement) targetStatement).setRef(i, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        ((PreparedStatement) targetStatement).setShort(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        ((PreparedStatement) targetStatement).setString(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        ((PreparedStatement) targetStatement).setTime(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        ((PreparedStatement) targetStatement).setTime(parameterIndex, x, cal);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        ((PreparedStatement) targetStatement).setTimestamp(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        ((PreparedStatement) targetStatement).setTimestamp(parameterIndex, x, cal);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        ((PreparedStatement) targetStatement).setURL(parameterIndex, x);
    }

    @Override
    @Deprecated
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        ((PreparedStatement) targetStatement).setUnicodeStream(parameterIndex, x, length);
    }

    //
    // public Connection getConnection() throws SQLException {
    // return connectionWrapper;
    // }

    @Override
    public boolean isClosed() throws SQLException {
        return ((PreparedStatement) targetStatement).isClosed();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        ((PreparedStatement) targetStatement).setPoolable(poolable);
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return ((PreparedStatement) targetStatement).isPoolable();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return (T) this;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return this.getClass().isAssignableFrom(iface);
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        ((PreparedStatement) targetStatement).setRowId(parameterIndex, x);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        ((PreparedStatement) targetStatement).setNString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        ((PreparedStatement) targetStatement).setNCharacterStream(parameterIndex, value, length);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        ((PreparedStatement) targetStatement).setNClob(parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        ((PreparedStatement) targetStatement).setClob(parameterIndex, reader, length);

    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        ((PreparedStatement) targetStatement).setBlob(parameterIndex, inputStream, length);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        ((PreparedStatement) targetStatement).setNClob(parameterIndex, reader, length);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        ((PreparedStatement) targetStatement).setSQLXML(parameterIndex, xmlObject);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        ((PreparedStatement) targetStatement).setAsciiStream(parameterIndex, x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        ((PreparedStatement) targetStatement).setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        ((PreparedStatement) targetStatement).setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        ((PreparedStatement) targetStatement).setAsciiStream(parameterIndex, x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        ((PreparedStatement) targetStatement).setBinaryStream(parameterIndex, x);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        ((PreparedStatement) targetStatement).setCharacterStream(parameterIndex, reader);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        ((PreparedStatement) targetStatement).setNCharacterStream(parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        ((PreparedStatement) targetStatement).setClob(parameterIndex, reader);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        ((PreparedStatement) targetStatement).setBlob(parameterIndex, inputStream);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        ((PreparedStatement) targetStatement).setNClob(parameterIndex, reader);
    }

    @Override
    public void setLocalInfileInputStream(InputStream stream) {
        LoadFileUtils.setLocalInfileInputStream(targetStatement, stream);
    }

    @Override
    public InputStream getLocalInfileInputStream() {

        return LoadFileUtils.getLocalInfileInputStream(targetStatement);
    }

}
