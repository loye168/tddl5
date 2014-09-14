package com.taobao.tddl.executor.codec;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.record.FixedLengthRecord;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.datatype.DataType.DecodeResult;

/**
 * @author mengshi.sunmengshi 2014年3月4日 下午3:19:31
 * @since 5.1.0
 */
public class FixedLengthCodec implements RecordCodec<byte[]> {

    List<ColumnMeta>     columns;
    Map<String, Integer> index;

    public FixedLengthCodec(List<ColumnMeta> columns){
        this.columns = columns;
        this.index = new HashMap(columns.size());
        for (ColumnMeta key : columns) {
            if (!index.containsKey(key.getName())) {
                index.put(key.getName(), index.size());
            }
        }
    }

    @Override
    public byte[] encode(CloneableRecord record) {
        int length = calculateEncodedLength(record, columns);
        byte[] dst = new byte[length];
        int offset = 0;
        for (ColumnMeta c : columns) {
            Object v = record.get(c.getName());
            DataType t = c.getDataType();

            if (v == null && !c.isNullable()) {
                throw new RuntimeException(c + " is not nullable.");
            }

            offset += t.encodeToBytes(v, dst, offset);
        }
        return dst;
    }

    private int calculateEncodedLength(CloneableRecord record, List<ColumnMeta> columns) {
        int length = 0;

        for (ColumnMeta c : columns) {
            Object v = record.get(c.getName());
            DataType t = c.getDataType();

            length += t.getLength(v);

        }
        return length;
    }

    @Override
    public CloneableRecord decode(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        FixedLengthRecord record = new FixedLengthRecord(index, columns);
        int offset = 0;
        for (int i = 0; i < columns.size(); i++) {
            ColumnMeta c = columns.get(i);
            DataType t = c.getDataType();
            Object v = null;

            DecodeResult res = t.decodeFromBytes(bytes, offset);

            v = res.value;
            offset += res.length;

            record.setValueByIndex(index.get(c.getName()), v);
        }
        return record;
    }

    @Override
    public CloneableRecord newEmptyRecord() {
        return new FixedLengthRecord(index, columns);
    }

}
