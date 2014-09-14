package com.taobao.tddl.group.jdbc;

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
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

import com.taobao.tddl.common.jdbc.Parameters;

public class TGroupCallableStatement extends TGroupPreparedStatement implements CallableStatement {

    private CallableStatement targetStatement;

    public TGroupCallableStatement(TGroupDataSource groupDataSource, TGroupConnection groupConnection,
                                   CallableStatement targetStatement, String sql, String appName){
        super(groupDataSource, groupConnection, sql, appName);
        this.targetStatement = targetStatement;
    }

    @Override
    public boolean execute() throws SQLException {
        Parameters.setParameters(targetStatement, parameterSettings);
        return targetStatement.execute();
    }

    @Override
    public Array getArray(int i) throws SQLException {
        return targetStatement.getArray(i);
    }

    @Override
    public Array getArray(String parameterName) throws SQLException {
        return targetStatement.getArray(parameterName);
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        return targetStatement.getBigDecimal(parameterIndex);
    }

    @Override
    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        return targetStatement.getBigDecimal(parameterName);
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        return targetStatement.getBigDecimal(parameterIndex, scale);
    }

    @Override
    public Blob getBlob(int i) throws SQLException {
        return targetStatement.getBlob(i);
    }

    @Override
    public Blob getBlob(String parameterName) throws SQLException {
        return targetStatement.getBlob(parameterName);
    }

    @Override
    public boolean getBoolean(int parameterIndex) throws SQLException {
        return targetStatement.getBoolean(parameterIndex);
    }

    @Override
    public boolean getBoolean(String parameterName) throws SQLException {
        return targetStatement.getBoolean(parameterName);
    }

    @Override
    public byte getByte(int parameterIndex) throws SQLException {
        return targetStatement.getByte(parameterIndex);
    }

    @Override
    public byte getByte(String parameterName) throws SQLException {
        return targetStatement.getByte(parameterName);
    }

    @Override
    public byte[] getBytes(int parameterIndex) throws SQLException {
        return targetStatement.getBytes(parameterIndex);
    }

    @Override
    public byte[] getBytes(String parameterName) throws SQLException {
        return targetStatement.getBytes(parameterName);
    }

    @Override
    public Clob getClob(int i) throws SQLException {
        return targetStatement.getClob(i);
    }

    @Override
    public Clob getClob(String parameterName) throws SQLException {
        return targetStatement.getClob(parameterName);
    }

    @Override
    public Date getDate(int parameterIndex) throws SQLException {
        return targetStatement.getDate(parameterIndex);
    }

    @Override
    public Date getDate(String parameterName) throws SQLException {
        return targetStatement.getDate(parameterName);
    }

    @Override
    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        return targetStatement.getDate(parameterIndex, cal);
    }

    @Override
    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        return targetStatement.getDate(parameterName, cal);
    }

    @Override
    public double getDouble(int parameterIndex) throws SQLException {
        return targetStatement.getDouble(parameterIndex);
    }

    @Override
    public double getDouble(String parameterName) throws SQLException {
        return targetStatement.getDouble(parameterName);
    }

    @Override
    public float getFloat(int parameterIndex) throws SQLException {
        return targetStatement.getFloat(parameterIndex);
    }

    @Override
    public float getFloat(String parameterName) throws SQLException {
        return targetStatement.getFloat(parameterName);
    }

    @Override
    public int getInt(int parameterIndex) throws SQLException {
        return targetStatement.getInt(parameterIndex);
    }

    @Override
    public int getInt(String parameterName) throws SQLException {
        return targetStatement.getInt(parameterName);
    }

    @Override
    public long getLong(int parameterIndex) throws SQLException {
        return targetStatement.getLong(parameterIndex);
    }

    @Override
    public long getLong(String parameterName) throws SQLException {
        return targetStatement.getLong(parameterName);
    }

    @Override
    public Object getObject(int parameterIndex) throws SQLException {
        return targetStatement.getObject(parameterIndex);
    }

    @Override
    public Object getObject(String parameterName) throws SQLException {
        return targetStatement.getObject(parameterName);
    }

    @Override
    public Object getObject(int i, Map<String, Class<?>> map) throws SQLException {
        return targetStatement.getObject(i, map);
    }

    @Override
    public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
        return targetStatement.getObject(parameterName, map);
    }

    @Override
    public Ref getRef(int i) throws SQLException {
        return targetStatement.getRef(i);
    }

    @Override
    public Ref getRef(String parameterName) throws SQLException {
        return targetStatement.getRef(parameterName);
    }

    @Override
    public short getShort(int parameterIndex) throws SQLException {
        return targetStatement.getShort(parameterIndex);
    }

    @Override
    public short getShort(String parameterName) throws SQLException {
        return targetStatement.getShort(parameterName);
    }

    @Override
    public String getString(int parameterIndex) throws SQLException {
        return targetStatement.getString(parameterIndex);
    }

    @Override
    public String getString(String parameterName) throws SQLException {
        return targetStatement.getString(parameterName);
    }

    @Override
    public Time getTime(int parameterIndex) throws SQLException {
        return targetStatement.getTime(parameterIndex);
    }

    @Override
    public Time getTime(String parameterName) throws SQLException {
        return targetStatement.getTime(parameterName);
    }

    @Override
    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        return targetStatement.getTime(parameterIndex, cal);
    }

    @Override
    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        return targetStatement.getTime(parameterName, cal);
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        return targetStatement.getTimestamp(parameterIndex);
    }

    @Override
    public Timestamp getTimestamp(String parameterName) throws SQLException {
        return targetStatement.getTimestamp(parameterName);
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        return targetStatement.getTimestamp(parameterIndex, cal);
    }

    @Override
    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        return targetStatement.getTimestamp(parameterName, cal);
    }

    @Override
    public URL getURL(int parameterIndex) throws SQLException {
        return targetStatement.getURL(parameterIndex);
    }

    @Override
    public URL getURL(String parameterName) throws SQLException {
        return targetStatement.getURL(parameterName);
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        targetStatement.registerOutParameter(parameterIndex, sqlType);
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
        targetStatement.registerOutParameter(parameterName, sqlType);
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
        targetStatement.registerOutParameter(parameterIndex, sqlType, scale);
    }

    @Override
    public void registerOutParameter(int paramIndex, int sqlType, String typeName) throws SQLException {
        targetStatement.registerOutParameter(paramIndex, sqlType, typeName);
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
        targetStatement.registerOutParameter(parameterName, sqlType, scale);
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
        targetStatement.registerOutParameter(parameterName, sqlType, typeName);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
        targetStatement.setAsciiStream(parameterName, x, length);
    }

    @Override
    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
        targetStatement.setBigDecimal(parameterName, x);
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
        targetStatement.setBinaryStream(parameterName, x, length);
    }

    @Override
    public void setBoolean(String parameterName, boolean x) throws SQLException {
        targetStatement.setBoolean(parameterName, x);
    }

    @Override
    public void setByte(String parameterName, byte x) throws SQLException {
        targetStatement.setByte(parameterName, x);
    }

    @Override
    public void setBytes(String parameterName, byte[] x) throws SQLException {
        targetStatement.setBytes(parameterName, x);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
        targetStatement.setCharacterStream(parameterName, reader, length);
    }

    @Override
    public void setDate(String parameterName, Date x) throws SQLException {
        targetStatement.setDate(parameterName, x);
    }

    @Override
    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
        targetStatement.setDate(parameterName, x, cal);
    }

    @Override
    public void setDouble(String parameterName, double x) throws SQLException {
        targetStatement.setDouble(parameterName, x);
    }

    @Override
    public void setFloat(String parameterName, float x) throws SQLException {
        targetStatement.setFloat(parameterName, x);
    }

    @Override
    public void setInt(String parameterName, int x) throws SQLException {
        targetStatement.setInt(parameterName, x);
    }

    @Override
    public void setLong(String parameterName, long x) throws SQLException {
        targetStatement.setLong(parameterName, x);
    }

    @Override
    public void setNull(String parameterName, int sqlType) throws SQLException {
        targetStatement.setNull(parameterName, sqlType);
    }

    @Override
    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        targetStatement.setNull(parameterName, sqlType, typeName);
    }

    @Override
    public void setObject(String parameterName, Object x) throws SQLException {
        targetStatement.setObject(parameterName, x);
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
        targetStatement.setObject(parameterName, x, targetSqlType);
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
        targetStatement.setObject(parameterName, x, targetSqlType);
    }

    @Override
    public void setShort(String parameterName, short x) throws SQLException {
        targetStatement.setShort(parameterName, x);
    }

    @Override
    public void setString(String parameterName, String x) throws SQLException {
        targetStatement.setString(parameterName, x);
    }

    @Override
    public void setTime(String parameterName, Time x) throws SQLException {
        targetStatement.setTime(parameterName, x);
    }

    @Override
    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
        targetStatement.setTime(parameterName, x, cal);
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
        targetStatement.setTimestamp(parameterName, x);
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
        targetStatement.setTimestamp(parameterName, x, cal);
    }

    @Override
    public void setURL(String parameterName, URL val) throws SQLException {
        targetStatement.setURL(parameterName, val);
    }

    @Override
    public boolean wasNull() throws SQLException {
        return targetStatement.wasNull();
    }

    @Override
    public RowId getRowId(int parameterIndex) throws SQLException {
        return targetStatement.getRowId(parameterIndex);
    }

    @Override
    public RowId getRowId(String parameterName) throws SQLException {
        return targetStatement.getRowId(parameterName);
    }

    @Override
    public void setRowId(String parameterName, RowId x) throws SQLException {
        targetStatement.setRowId(parameterName, x);
    }

    @Override
    public void setNString(String parameterName, String value) throws SQLException {
        targetStatement.setNString(parameterName, value);
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
        targetStatement.setNCharacterStream(parameterName, value, length);
    }

    @Override
    public void setNClob(String parameterName, NClob value) throws SQLException {
        targetStatement.setNClob(parameterName, value);
    }

    @Override
    public void setClob(String parameterName, Reader reader, long length) throws SQLException {
        targetStatement.setClob(parameterName, reader, length);
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
        targetStatement.setBlob(parameterName, inputStream);
    }

    @Override
    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
        targetStatement.setNClob(parameterName, reader, length);
    }

    @Override
    public NClob getNClob(int parameterIndex) throws SQLException {
        return targetStatement.getNClob(parameterIndex);
    }

    @Override
    public NClob getNClob(String parameterName) throws SQLException {
        return targetStatement.getNClob(parameterName);
    }

    @Override
    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        targetStatement.setSQLXML(parameterName, xmlObject);
    }

    @Override
    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        return targetStatement.getSQLXML(parameterIndex);
    }

    @Override
    public SQLXML getSQLXML(String parameterName) throws SQLException {
        return targetStatement.getSQLXML(parameterName);
    }

    @Override
    public String getNString(int parameterIndex) throws SQLException {
        return targetStatement.getNString(parameterIndex);
    }

    @Override
    public String getNString(String parameterName) throws SQLException {
        return targetStatement.getNString(parameterName);
    }

    @Override
    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        return targetStatement.getNCharacterStream(parameterIndex);
    }

    @Override
    public Reader getNCharacterStream(String parameterName) throws SQLException {
        return targetStatement.getNCharacterStream(parameterName);
    }

    @Override
    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        return targetStatement.getCharacterStream(parameterIndex);
    }

    @Override
    public Reader getCharacterStream(String parameterName) throws SQLException {
        return targetStatement.getCharacterStream(parameterName);
    }

    @Override
    public void setBlob(String parameterName, Blob x) throws SQLException {
        targetStatement.setBlob(parameterName, x);
    }

    @Override
    public void setClob(String parameterName, Clob x) throws SQLException {
        targetStatement.setClob(parameterName, x);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
        targetStatement.setAsciiStream(parameterName, x, length);
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
        targetStatement.setBinaryStream(parameterName, x, length);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
        targetStatement.setCharacterStream(parameterName, reader, length);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
        targetStatement.setAsciiStream(parameterName, x);
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
        targetStatement.setBinaryStream(parameterName, x);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
        targetStatement.setCharacterStream(parameterName, reader);
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
        targetStatement.setNCharacterStream(parameterName, value);
    }

    @Override
    public void setClob(String parameterName, Reader reader) throws SQLException {
        targetStatement.setClob(parameterName, reader);
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
        targetStatement.setBlob(parameterName, inputStream);
    }

    @Override
    public void setNClob(String parameterName, Reader reader) throws SQLException {
        targetStatement.setNClob(parameterName, reader);
    }
}
