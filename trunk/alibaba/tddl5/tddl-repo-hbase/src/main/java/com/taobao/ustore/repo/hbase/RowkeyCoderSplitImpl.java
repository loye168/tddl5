package com.taobao.ustore.repo.hbase;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.config.table.HBaseColumnCoder;
import com.taobao.tddl.repo.hbase.cursor.ExtremeValue;
import com.taobao.ustore.repo.hbase.cursor.BytesUtils;

public abstract class RowkeyCoderSplitImpl extends AbstractRowCoder {

    public RowkeyCoderSplitImpl(TablePhysicalSchema schema){
        super(schema);
    }

    public abstract String getSpliter();

    public String getEncoding() {
        return "utf-8";
    }

    @Override
    public Map<String, Object> decodeRowKey(byte[] rowKey) {

        String s;
        try {
            s = new String(rowKey, this.getEncoding());
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        String rowKeyStr[] = TStringUtil.split(s, this.getSpliter());

        Map<String, Object> rowKeyColumnValues = new HashMap();

        if (rowKeyStr.length != this.getPhysicalSchema().getRowKey().size()) {
            throw new RuntimeException("从hbase中拿出来的rowkey中包含的列的数目和schema中rowkey列的数目不一致, rowkey: [" + s + "] spliter:["
                                       + this.getSpliter() + "]," + "rowKeyStr array:" + rowKeyStr
                                       + ",columns in schema:" + this.getPhysicalSchema().getRowKey() + ",schema name:"
                                       + this.getPhysicalSchema().getTableName());
        }
        for (int i = 0; i < this.getPhysicalSchema().getRowKey().size(); i++) {
            String column = this.getPhysicalSchema().getRowKey().get(i);
            String valueStr = rowKeyStr[i];
            ColumnMeta cm = this.getPhysicalSchema().getSchema().getColumn(column);
            if (cm == null) throw new RuntimeException("列: " + column + " 找不到");
            HBaseColumnCoder coder = this.getPhysicalSchema().getColumnCoders().get(column);
            rowKeyColumnValues.put(column, coder.decodeFromString(cm.getDataType(), valueStr));
        }
        return rowKeyColumnValues;

    }

    @Override
    public byte[] encodeRowKey(Map<String, Object> rowKeyColumnValues) {

        StringBuilder rowKeyStr = new StringBuilder();

        boolean isFirst = true;
        for (int i = 0; i < this.getPhysicalSchema().getRowKey().size(); i++) {

            String column = this.getPhysicalSchema().getRowKey().get(i);

            ColumnMeta cm = this.getPhysicalSchema().getSchema().getColumn(column);
            if (cm == null) throw new RuntimeException("列: " + column + " 找不到");

            if (isFirst) {
                isFirst = false;
            } else {
                rowKeyStr.append(this.getSpliter());
            }

            Object value = rowKeyColumnValues.get(column);

            if (value instanceof ExtremeValue) {
                return appendByte(BytesUtils.stringToBytes(rowKeyStr.toString(), this.getEncoding()),
                    ((ExtremeValue) value).getByte());
            }

            HBaseColumnCoder coder = this.getPhysicalSchema().getColumnCoders().get(column);

            String valueStr = coder.encodeToString(cm.getDataType(), value);

            rowKeyStr.append(valueStr);
        }

        return BytesUtils.stringToBytes(rowKeyStr.toString(), this.getEncoding());

    }

    public byte[] appendByte(byte[] a, byte b) {
        if (a == null || a.length == 0) {
            return new byte[] { b };
        }

        byte c[] = new byte[a.length + 1];

        System.arraycopy(a, 0, c, 0, a.length);

        c[a.length] = b;

        return c;

    }

}
