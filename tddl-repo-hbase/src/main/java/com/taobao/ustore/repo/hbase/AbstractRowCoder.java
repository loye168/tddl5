package com.taobao.ustore.repo.hbase;

import java.util.Map;

public abstract class AbstractRowCoder extends RowkeyCoder {

    public AbstractRowCoder(TablePhysicalSchema schema){
        super(schema);
    }

    @Override
    abstract public Map<String, Object> decodeRowKey(byte[] rowKey);

    @Override
    abstract public byte[] encodeRowKey(Map<String, Object> rowKeyColumnValues);

}
