package com.taobao.tddl.common.mock;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class MockResultSetMetaData implements ResultSetMetaData {

    private String                                     tableName;
    public final Map<String/* 列名 */, Integer/* 列序号 */> columnName2Index;
    public final Map<Integer/* 列序号 */, String/* 列名 */> columnIndex2Name;

    public MockResultSetMetaData(Map<String, Integer> columns){
        this.columnName2Index = columns;
        this.columnIndex2Name = new HashMap<Integer, String>(columns.size());
        for (Map.Entry<String, Integer> e : columns.entrySet()) {
            columnIndex2Name.put(e.getValue(), e.getKey());
        }
    }

    public String getCatalogName(int column) throws SQLException {

        return null;
    }

    public String getColumnClassName(int column) throws SQLException {

        return null;
    }

    public int getColumnCount() throws SQLException {
        return columnName2Index.size();
    }

    public int getColumnDisplaySize(int column) throws SQLException {

        return 0;
    }

    public String getColumnLabel(int column) throws SQLException {

        return null;
    }

    public String getColumnName(int column) throws SQLException {

        return columnIndex2Name.get(column - 1);// column从1开始
    }

    public int getColumnType(int column) throws SQLException {

        return 0;
    }

    public String getColumnTypeName(int column) throws SQLException {

        return null;
    }

    public int getPrecision(int column) throws SQLException {

        return 0;
    }

    public int getScale(int column) throws SQLException {

        return 0;
    }

    public String getSchemaName(int column) throws SQLException {

        return null;
    }

    public String getTableName(int column) throws SQLException {
        return this.tableName;
    }

    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    public boolean isCaseSensitive(int column) throws SQLException {
        return false;
    }

    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    public int isNullable(int column) throws SQLException {
        return ResultSetMetaData.columnNoNulls;
    }

    public boolean isReadOnly(int column) throws SQLException {
        return false;
    }

    public boolean isSearchable(int column) throws SQLException {
        return false;
    }

    public boolean isSigned(int column) throws SQLException {
        return false;
    }

    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {

        return null;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {

        return false;
    }
}
