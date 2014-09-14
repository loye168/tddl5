package com.taobao.tddl.atom.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

public class CallableStatementWrapper extends TPreparedStatementWrapper implements CallableStatement {

    public CallableStatementWrapper(Statement targetStatement, TConnectionWrapper connectionWrapper,
                                    TDataSourceWrapper dataSourceWrapper, String sql, String appName){
        super(targetStatement, connectionWrapper, dataSourceWrapper, sql, appName);
    }

    @Override
    public boolean execute() throws SQLException {
        return ((CallableStatement) targetStatement).execute();
    }

    @Override
    public Array getArray(int i) throws SQLException {
        return ((CallableStatement) targetStatement).getArray(i);
    }

    @Override
    public Array getArray(String parameterName) throws SQLException {
        return ((CallableStatement) targetStatement).getArray(parameterName);
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        return ((CallableStatement) targetStatement).getBigDecimal(parameterIndex);
    }

    @Override
    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        return ((CallableStatement) targetStatement).getBigDecimal(parameterName);
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        return ((CallableStatement) targetStatement).getBigDecimal(parameterIndex, scale);
    }

    @Override
    public Blob getBlob(int i) throws SQLException {
        return ((CallableStatement) targetStatement).getBlob(i);
    }

    @Override
    public Blob getBlob(String parameterName) throws SQLException {
        return ((CallableStatement) targetStatement).getBlob(parameterName);
    }

    @Override
    public boolean getBoolean(int parameterIndex) throws SQLException {
        return ((CallableStatement) targetStatement).getBoolean(parameterIndex);
    }

    @Override
    public boolean getBoolean(String parameterName) throws SQLException {
        return ((CallableStatement) targetStatement).getBoolean(parameterName);
    }

    @Override
    public byte getByte(int parameterIndex) throws SQLException {
        return ((CallableStatement) targetStatement).getByte(parameterIndex);
    }

    @Override
    public byte getByte(String parameterName) throws SQLException {
        return ((CallableStatement) targetStatement).getByte(parameterName);
    }

    @Override
    public byte[] getBytes(int parameterIndex) throws SQLException {
        return ((CallableStatement) targetStatement).getBytes(parameterIndex);
    }

    @Override
    public byte[] getBytes(String parameterName) throws SQLException {
        return ((CallableStatement) targetStatement).getBytes(parameterName);
    }

    @Override
    public Clob getClob(int i) throws SQLException {
        return ((CallableStatement) targetStatement).getClob(i);
    }

    @Override
    public Clob getClob(String parameterName) throws SQLException {
        return ((CallableStatement) targetStatement).getClob(parameterName);
    }

    @Override
    public Date getDate(int parameterIndex) throws SQLException {
        return ((CallableStatement) targetStatement).getDate(parameterIndex);
    }

    @Override
    public Date getDate(String parameterName) throws SQLException {
        return ((CallableStatement) targetStatement).getDate(parameterName);
    }

    @Override
    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        return ((CallableStatement) targetStatement).getDate(parameterIndex, cal);
    }

    @Override
    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        return ((CallableStatement) targetStatement).getDate(parameterName, cal);
    }

    @Override
    public double getDouble(int parameterIndex) throws SQLException {
        return ((CallableStatement) targetStatement).getDouble(parameterIndex);
    }

    @Override
    public double getDouble(String parameterName) throws SQLException {
        return ((CallableStatement) targetStatement).getDouble(parameterName);
    }

    @Override
    public float getFloat(int parameterIndex) throws SQLException {
        return ((CallableStatement) targetStatement).getFloat(parameterIndex);
    }

    @Override
    public float getFloat(String parameterName) throws SQLException {
        return ((CallableStatement) targetStatement).getFloat(parameterName);
    }

    @Override
    public int getInt(int parameterIndex) throws SQLException {
        return ((CallableStatement) targetStatement).getInt(parameterIndex);
    }

    @Override
    public int getInt(String parameterName) throws SQLException {
        return ((CallableStatement) targetStatement).getInt(parameterName);
    }

    @Override
    public long getLong(int parameterIndex) throws SQLException {
        return ((CallableStatement) targetStatement).getLong(parameterIndex);
    }

    @Override
    public long getLong(String parameterName) throws SQLException {
        return ((CallableStatement) targetStatement).getLong(parameterName);
    }

    @Override
    public Object getObject(int parameterIndex) throws SQLException {
        return ((CallableStatement) targetStatement).getObject(parameterIndex);
    }

    @Override
    public Object getObject(String parameterName) throws SQLException {
        return ((CallableStatement) targetStatement).getObject(parameterName);
    }

    @Override
    public Object getObject(int i, Map<String, Class<?>> map) throws SQLException {
        return ((CallableStatement) targetStatement).getObject(i, map);
    }

    @Override
    public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
        return ((CallableStatement) targetStatement).getObject(parameterName, map);
    }

    @Override
    public Ref getRef(int i) throws SQLException {
        return ((CallableStatement) targetStatement).getRef(i);
    }

    @Override
    public Ref getRef(String parameterName) throws SQLException {
        return ((CallableStatement) targetStatement).getRef(parameterName);
    }

    @Override
    public short getShort(int parameterIndex) throws SQLException {
        return ((CallableStatement) targetStatement).getShort(parameterIndex);
    }

    @Override
    public short getShort(String parameterName) throws SQLException {
        return ((CallableStatement) targetStatement).getShort(parameterName);
    }

    @Override
    public String getString(int parameterIndex) throws SQLException {
        return ((CallableStatement) targetStatement).getString(parameterIndex);
    }

    @Override
    public String getString(String parameterName) throws SQLException {
        return ((CallableStatement) targetStatement).getString(parameterName);
    }

    @Override
    public Time getTime(int parameterIndex) throws SQLException {
        return ((CallableStatement) targetStatement).getTime(parameterIndex);
    }

    @Override
    public Time getTime(String parameterName) throws SQLException {
        return ((CallableStatement) targetStatement).getTime(parameterName);
    }

    @Override
    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        return ((CallableStatement) targetStatement).getTime(parameterIndex, cal);
    }

    @Override
    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        return ((CallableStatement) targetStatement).getTime(parameterName, cal);
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        return ((CallableStatement) targetStatement).getTimestamp(parameterIndex);
    }

    @Override
    public Timestamp getTimestamp(String parameterName) throws SQLException {
        return ((CallableStatement) targetStatement).getTimestamp(parameterName);
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        return ((CallableStatement) targetStatement).getTimestamp(parameterIndex, cal);
    }

    @Override
    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        return ((CallableStatement) targetStatement).getTimestamp(parameterName, cal);
    }

    @Override
    public URL getURL(int parameterIndex) throws SQLException {
        return ((CallableStatement) targetStatement).getURL(parameterIndex);
    }

    @Override
    public URL getURL(String parameterName) throws SQLException {
        return ((CallableStatement) targetStatement).getURL(parameterName);
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        ((CallableStatement) targetStatement).registerOutParameter(parameterIndex, sqlType);
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
        ((CallableStatement) targetStatement).registerOutParameter(parameterName, sqlType);
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
        ((CallableStatement) targetStatement).registerOutParameter(parameterIndex, sqlType, scale);
    }

    @Override
    public void registerOutParameter(int paramIndex, int sqlType, String typeName) throws SQLException {
        ((CallableStatement) targetStatement).registerOutParameter(paramIndex, sqlType, typeName);
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
        ((CallableStatement) targetStatement).registerOutParameter(parameterName, sqlType, scale);
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
        ((CallableStatement) targetStatement).registerOutParameter(parameterName, sqlType, typeName);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
        ((CallableStatement) targetStatement).setAsciiStream(parameterName, x, length);
    }

    @Override
    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
        ((CallableStatement) targetStatement).setBigDecimal(parameterName, x);
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
        ((CallableStatement) targetStatement).setBinaryStream(parameterName, x, length);
    }

    @Override
    public void setBoolean(String parameterName, boolean x) throws SQLException {
        ((CallableStatement) targetStatement).setBoolean(parameterName, x);
    }

    @Override
    public void setByte(String parameterName, byte x) throws SQLException {
        ((CallableStatement) targetStatement).setByte(parameterName, x);
    }

    @Override
    public void setBytes(String parameterName, byte[] x) throws SQLException {
        ((CallableStatement) targetStatement).setBytes(parameterName, x);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
        ((CallableStatement) targetStatement).setCharacterStream(parameterName, reader, length);
    }

    @Override
    public void setDate(String parameterName, Date x) throws SQLException {
        ((CallableStatement) targetStatement).setDate(parameterName, x);
    }

    @Override
    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
        ((CallableStatement) targetStatement).setDate(parameterName, x, cal);
    }

    @Override
    public void setDouble(String parameterName, double x) throws SQLException {
        ((CallableStatement) targetStatement).setDouble(parameterName, x);
    }

    @Override
    public void setFloat(String parameterName, float x) throws SQLException {
        ((CallableStatement) targetStatement).setFloat(parameterName, x);
    }

    @Override
    public void setInt(String parameterName, int x) throws SQLException {
        ((CallableStatement) targetStatement).setInt(parameterName, x);
    }

    @Override
    public void setLong(String parameterName, long x) throws SQLException {
        ((CallableStatement) targetStatement).setLong(parameterName, x);
    }

    @Override
    public void setNull(String parameterName, int sqlType) throws SQLException {
        ((CallableStatement) targetStatement).setNull(parameterName, sqlType);
    }

    @Override
    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        ((CallableStatement) targetStatement).setNull(parameterName, sqlType, typeName);
    }

    @Override
    public void setObject(String parameterName, Object x) throws SQLException {
        ((CallableStatement) targetStatement).setObject(parameterName, x);
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
        ((CallableStatement) targetStatement).setObject(parameterName, x, targetSqlType);
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
        ((CallableStatement) targetStatement).setObject(parameterName, x, targetSqlType);
    }

    @Override
    public void setShort(String parameterName, short x) throws SQLException {
        ((CallableStatement) targetStatement).setShort(parameterName, x);
    }

    @Override
    public void setString(String parameterName, String x) throws SQLException {
        ((CallableStatement) targetStatement).setString(parameterName, x);
    }

    @Override
    public void setTime(String parameterName, Time x) throws SQLException {
        ((CallableStatement) targetStatement).setTime(parameterName, x);
    }

    @Override
    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
        ((CallableStatement) targetStatement).setTime(parameterName, x, cal);
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
        ((CallableStatement) targetStatement).setTimestamp(parameterName, x);
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
        ((CallableStatement) targetStatement).setTimestamp(parameterName, x, cal);
    }

    @Override
    public void setURL(String parameterName, URL val) throws SQLException {
        ((CallableStatement) targetStatement).setURL(parameterName, val);
    }

    @Override
    public boolean wasNull() throws SQLException {
        return ((CallableStatement) targetStatement).wasNull();
    }

    @Override
    public RowId getRowId(int parameterIndex) throws SQLException {
        return ((CallableStatement) targetStatement).getRowId(parameterIndex);
    }

    @Override
    public RowId getRowId(String parameterName) throws SQLException {
        return ((CallableStatement) targetStatement).getRowId(parameterName);
    }

    @Override
    public void setRowId(String parameterName, RowId x) throws SQLException {
        ((CallableStatement) targetStatement).setRowId(parameterName, x);
    }

    @Override
    public void setNString(String parameterName, String value) throws SQLException {
        ((CallableStatement) targetStatement).setNString(parameterName, value);
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
        ((CallableStatement) targetStatement).setNCharacterStream(parameterName, value, length);
    }

    @Override
    public void setNClob(String parameterName, NClob value) throws SQLException {
        ((CallableStatement) targetStatement).setNClob(parameterName, value);
    }

    @Override
    public void setClob(String parameterName, Reader reader, long length) throws SQLException {
        ((CallableStatement) targetStatement).setClob(parameterName, reader, length);
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
        ((CallableStatement) targetStatement).setBlob(parameterName, inputStream);
    }

    @Override
    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
        ((CallableStatement) targetStatement).setNClob(parameterName, reader, length);
    }

    @Override
    public NClob getNClob(int parameterIndex) throws SQLException {
        return ((CallableStatement) targetStatement).getNClob(parameterIndex);
    }

    @Override
    public NClob getNClob(String parameterName) throws SQLException {
        return ((CallableStatement) targetStatement).getNClob(parameterName);
    }

    @Override
    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        ((CallableStatement) targetStatement).setSQLXML(parameterName, xmlObject);
    }

    @Override
    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        return ((CallableStatement) targetStatement).getSQLXML(parameterIndex);
    }

    @Override
    public SQLXML getSQLXML(String parameterName) throws SQLException {
        return ((CallableStatement) targetStatement).getSQLXML(parameterName);
    }

    @Override
    public String getNString(int parameterIndex) throws SQLException {
        return ((CallableStatement) targetStatement).getNString(parameterIndex);
    }

    @Override
    public String getNString(String parameterName) throws SQLException {
        return ((CallableStatement) targetStatement).getNString(parameterName);
    }

    @Override
    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        return ((CallableStatement) targetStatement).getNCharacterStream(parameterIndex);
    }

    @Override
    public Reader getNCharacterStream(String parameterName) throws SQLException {
        return ((CallableStatement) targetStatement).getNCharacterStream(parameterName);
    }

    @Override
    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        return ((CallableStatement) targetStatement).getCharacterStream(parameterIndex);
    }

    @Override
    public Reader getCharacterStream(String parameterName) throws SQLException {
        return ((CallableStatement) targetStatement).getCharacterStream(parameterName);
    }

    @Override
    public void setBlob(String parameterName, Blob x) throws SQLException {
        ((CallableStatement) targetStatement).setBlob(parameterName, x);
    }

    @Override
    public void setClob(String parameterName, Clob x) throws SQLException {
        ((CallableStatement) targetStatement).setClob(parameterName, x);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
        ((CallableStatement) targetStatement).setAsciiStream(parameterName, x, length);
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
        ((CallableStatement) targetStatement).setBinaryStream(parameterName, x, length);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
        ((CallableStatement) targetStatement).setCharacterStream(parameterName, reader, length);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
        ((CallableStatement) targetStatement).setAsciiStream(parameterName, x);
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
        ((CallableStatement) targetStatement).setBinaryStream(parameterName, x);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
        ((CallableStatement) targetStatement).setCharacterStream(parameterName, reader);
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
        ((CallableStatement) targetStatement).setNCharacterStream(parameterName, value);
    }

    @Override
    public void setClob(String parameterName, Reader reader) throws SQLException {
        ((CallableStatement) targetStatement).setClob(parameterName, reader);
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
        ((CallableStatement) targetStatement).setBlob(parameterName, inputStream);
    }

    @Override
    public void setNClob(String parameterName, Reader reader) throws SQLException {
        ((CallableStatement) targetStatement).setNClob(parameterName, reader);
    }
}
