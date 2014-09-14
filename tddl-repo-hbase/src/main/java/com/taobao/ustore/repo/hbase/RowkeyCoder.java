package com.taobao.ustore.repo.hbase;

import java.util.Map;

public abstract class RowkeyCoder {

    // public static final String ENCODING = "UTF-8";
    private TablePhysicalSchema schema;

    public RowkeyCoder(TablePhysicalSchema schema){
        this.schema = schema;
    }

    abstract public Map<String, Object> decodeRowKey(byte[] rowKey);

    abstract public byte[] encodeRowKey(Map<String, Object> rowKeyColumnValues);

    public TablePhysicalSchema getPhysicalSchema() {
        return schema;
    }

    public void setSchema(TablePhysicalSchema schema) {
        this.schema = schema;
    }

}
