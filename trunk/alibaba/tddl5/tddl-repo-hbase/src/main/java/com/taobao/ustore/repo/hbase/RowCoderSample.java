package com.taobao.ustore.repo.hbase;

import java.util.HashMap;
import java.util.Map;

public class RowCoderSample extends AbstractRowCoder {

    public RowCoderSample(TablePhysicalSchema schema){
        super(schema);
    }

    @Override
    public Map<String, Object> decodeRowKey(byte[] rowKey) {
        String rowKeyStr = new String(rowKey);

        Map<String, Object> rowKeyColumnValues = new HashMap();
        try {
            rowKeyColumnValues.put("ROWKEY", Integer.valueOf(rowKeyStr));
        } catch (Exception ex) {
            rowKeyColumnValues.put("ROWKEY", null);
        }

        return rowKeyColumnValues;

    }

    @Override
    public byte[] encodeRowKey(Map<String, Object> rowKeyColumnValues) {
        StringBuilder rowKeyStr = new StringBuilder();
        Number rowKey = (Number) rowKeyColumnValues.get("ROWKEY");

        if (rowKey != null) rowKeyStr.append(rowKey);
        else rowKeyStr.append(0L);

        return rowKeyStr.toString().getBytes();
    }

}
