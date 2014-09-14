package com.taobao.ustore.repo.hbase;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.optimizer.OptimizerContext;
import com.taobao.tddl.optimizer.config.table.HBaseColumnCoder;
import com.taobao.tddl.optimizer.config.table.TableMeta;

/**
 * @author junyu
 */
public class TablePhysicalSchema implements Serializable, Cloneable {

    private static final long                                     serialVersionUID = 5168519373619656091L;

    /**
     * 实际表，未处理过的
     */
    String                                                        tableName;

    Map<String/* NAME UPPER CASE */, String/* ORIGINAL */>        columns          = new HashMap<String, String>();

    Map<String/* NAME UPPER CASE */, HBaseColumnCoder/* CODER */> columnCoders     = new HashMap<String, HBaseColumnCoder>();

    Map<String/* ORIGINAL */, String/* NAME UPPER CASE */>        reverseColumns   = new HashMap<String, String>();

    RowkeyCoder                                                   rowKeyGenerator;
    List<String>                                                  rowKey;

    // TableMeta schema;

    public String getTableName() {
        return tableName;
    }

    public Map<String, String> getColumns() {
        return columns;
    }

    public String getRealColumn(String col) {
        return this.columns.get(col.toUpperCase());
    }

    public Map<String, String> getReverseColumns() {
        return reverseColumns;
    }

    public String getInnerColumn(String realCol) {
        return reverseColumns.get(realCol);
    }

    public RowkeyCoder getRowKeyGenerator() {
        return rowKeyGenerator;
    }

    public List<String> getRowKey() {
        return rowKey;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setColumns(Map<String, String> columns) {
        for (Map.Entry<String, String> entry : columns.entrySet()) {
            this.columns.put(entry.getKey(), entry.getValue());
            reverseColumns.put(entry.getValue(), entry.getKey());
        }
    }

    public void setRowKeyGenerator(RowkeyCoder rowKeyGenerator) {
        this.rowKeyGenerator = rowKeyGenerator;
    }

    public void setRowKey(List<String> rowKey) {
        this.rowKey = rowKey;
    }

    public Map<String, HBaseColumnCoder> getColumnCoders() {
        return columnCoders;
    }

    public void setColumnCoders(Map<String, HBaseColumnCoder> columnCoders) {
        this.columnCoders = columnCoders;
    }

    public TableMeta getSchema() {

        return OptimizerContext.getContext().getSchemaManager().getTable(tableName);
    }

}
