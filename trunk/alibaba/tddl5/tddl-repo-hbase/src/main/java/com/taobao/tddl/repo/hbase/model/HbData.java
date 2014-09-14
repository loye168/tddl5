package com.taobao.tddl.repo.hbase.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Hbase数据操作对象
 * 
 * @author jianghang 2013-7-30 下午4:28:18
 * @since 3.0.1
 */
public class HbData {

    private String         tableName;
    // 数据rowKey + column信息
    private byte[]         rowKey;
    private List<HbColumn> columns    = new ArrayList<HbData.HbColumn>();

    // 检索额外条件
    private int            maxVersion = 1;
    private long           startTime;
    private long           endTime;
    private byte[]         startRow;
    private byte[]         endRow;

    public static class HbColumn {

        private String columnFamily;
        private String columnName;
        private byte[] value;
        private long   timestamp;

        public HbColumn(String columnFamily, String columnName){
            this.columnFamily = columnFamily;
            this.columnName = columnName;
        }

        public HbColumn(String columnFamily, String columnName, byte[] value){
            this.columnFamily = columnFamily;
            this.columnName = columnName;
            this.value = value;
        }

        public HbColumn(String columnFamily, String columnName, byte[] value, long timestamp){
            this.columnFamily = columnFamily;
            this.columnName = columnName;
            this.value = value;
            this.timestamp = timestamp;
        }

        public String getColumnFamily() {
            return columnFamily;
        }

        public void setColumnFamily(String columnFamily) {
            this.columnFamily = columnFamily;
        }

        public String getColumnName() {
            return columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public byte[] getValue() {
            return value;
        }

        public void setValue(byte[] value) {
            this.value = value;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public byte[] getRowKey() {
        return rowKey;
    }

    public void setRowKey(byte[] rowKey) {
        this.rowKey = rowKey;
    }

    public List<HbColumn> getColumns() {
        return columns;
    }

    public void addColumn(HbColumn column) {
        columns.add(column);
    }

    public void setColumns(List<HbColumn> columns) {
        this.columns = columns;
    }

    public int getMaxVersion() {
        return maxVersion;
    }

    public void setMaxVersion(int maxVersion) {
        this.maxVersion = maxVersion;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public byte[] getStartRow() {
        return startRow;
    }

    public void setStartRow(byte[] startRow) {
        this.startRow = startRow;
    }

    public byte[] getEndRow() {
        return endRow;
    }

    public void setEndRow(byte[] endRow) {
        this.endRow = endRow;
    }

}
