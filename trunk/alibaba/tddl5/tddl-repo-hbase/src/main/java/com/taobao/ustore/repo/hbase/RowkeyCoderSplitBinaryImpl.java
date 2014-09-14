package com.taobao.ustore.repo.hbase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.config.table.HBaseColumnCoder;
import com.taobao.tddl.repo.hbase.cursor.ExtremeValue;

public abstract class RowkeyCoderSplitBinaryImpl extends AbstractRowCoder {

    public RowkeyCoderSplitBinaryImpl(TablePhysicalSchema schema){
        super(schema);
    }

    public abstract byte getSpliter();

    public String getEncoding() {
        return "utf-8";
    }

    @Override
    public Map<String, Object> decodeRowKey(byte[] rowKey) {
        List<byte[]> rowKeyColumnByteValues = this.split(rowKey, new byte[] { this.getSpliter() }, 0);
        Map<String, Object> rowKeyColumnValues = new HashMap();

        if (rowKeyColumnByteValues.size() != this.getPhysicalSchema().getRowKey().size()) {
            throw new RuntimeException("从hbase中拿出来的rowkey中包含的列的数目和schema中rowkey列的数目不一致, rowkey: [" + rowKey
                                       + "] spliter:[" + this.getSpliter() + "]");
        }
        for (int i = 0; i < this.getPhysicalSchema().getRowKey().size(); i++) {
            String column = this.getPhysicalSchema().getRowKey().get(i);
            byte[] valueByte = rowKeyColumnByteValues.get(i);
            ColumnMeta cm = this.getPhysicalSchema().getSchema().getColumn(column);
            if (cm == null) throw new RuntimeException("列: " + column + " 找不到");
            HBaseColumnCoder coder = this.getPhysicalSchema().getColumnCoders().get(column);
            rowKeyColumnValues.put(column, coder.decodeFromBytes(cm.getDataType(), valueByte));
        }
        return rowKeyColumnValues;

    }

    @Override
    public byte[] encodeRowKey(Map<String, Object> rowKeyColumnValues) {

        ByteArrayOutputStream rowKeyStr = new ByteArrayOutputStream();

        boolean isFirst = true;
        for (int i = 0; i < this.getPhysicalSchema().getRowKey().size(); i++) {

            String column = this.getPhysicalSchema().getRowKey().get(i);

            ColumnMeta cm = this.getPhysicalSchema().getSchema().getColumn(column);
            if (cm == null) throw new RuntimeException("列: " + column + " 找不到");

            if (isFirst) {
                isFirst = false;
            } else {
                rowKeyStr.write(this.getSpliter());

            }

            Object value = rowKeyColumnValues.get(column);

            if (value instanceof ExtremeValue) {
                rowKeyStr.write(((ExtremeValue) value).getByte());
                return rowKeyStr.toByteArray();
                // return
                // appendByte(BytesUtils.stringToBytes(rowKeyStr.toByteArray(),
                // this.getEncoding()),
                // ((ExtremeValue) value).getByte());
            }

            HBaseColumnCoder coder = this.getPhysicalSchema().getColumnCoders().get(column);

            byte valueStr[] = coder.encodeToBytes(cm.getDataType(), value);

            try {
                rowKeyStr.write(valueStr);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return rowKeyStr.toByteArray();

    }

    public static byte[] appendByte(byte[] a, byte b) {
        if (a == null || a.length == 0) {
            return new byte[] { b };
        }

        byte c[] = new byte[a.length + 1];

        System.arraycopy(a, 0, c, 0, a.length);

        c[a.length] = b;

        return c;

    }

    public static void main(String[] args) throws UnsupportedEncodingException {

        // byte b[] = (Character.MAX_VALUE + "").getBytes("utf8");
        // //
        // // System.out.println(Integer.toBinaryString((int)(((int)0xff) &
        // b[0])));
        //
        // b = ("s").getBytes("utf8");
        // System.out.println(b[0]);

        // byte b[] = new byte[] { -4, -1, -1, -1 };
        // System.out.println(b[0]);
        // String str = new String(b, "utf8");
        // b = new String(b, "utf8").getBytes("utf8");
        // System.out.println(b[0]);
        //
        // b = (Character.MAX_VALUE + "").getBytes("utf8");
        // System.out.println(b[0]);
        // b[0] = -16;
        //
        // b = new String(b, "gbk").getBytes("gbk");
        // System.out.println(b[0]);

        // System.out.println(Integer.toBinaryString((int)(((int)0xff) &
        // b[0])));
        //
        //
        // System.out.println(StringUtil.split("2013-09-16 1", " ")[1]);

        byte b[] = new byte[] { 1, 2, 3, 4, 5, 6, 7, 7, 8 };

        List l = split(b, new byte[] { 6, 7 }, 0);
    }

    public static List<byte[]> split(byte[] str, byte[] separatorChars, int max) {
        if (str == null) {
            return null;
        }

        int length = str.length;

        if (length == 0) {
            return new ArrayList(0);
        }

        List list = new ArrayList();
        int sizePlus1 = 1;
        int i = 0;
        int start = 0;
        boolean match = false;

        if (separatorChars.length == 1) {
            // 优化分隔符长度为1的情形
            byte sep = separatorChars[0];

            while (i < length) {
                if (str[i] == sep) {
                    if (match) {
                        if (sizePlus1++ == max) {
                            i = length;
                        }

                        list.add(Arrays.copyOfRange(str, start, i));
                        match = false;
                    }

                    start = ++i;
                    continue;
                }

                match = true;
                i++;
            }
        } else {
            // 一般情形
            while (i < length) {

                if (indexOf(separatorChars, str[i]) >= 0) {
                    if (match) {
                        if (sizePlus1++ == max) {
                            i = length;
                        }

                        list.add(Arrays.copyOfRange(str, start, i));
                        match = false;
                    }

                    start = ++i;
                    continue;
                }

                match = true;
                i++;
            }
        }

        if (match) {
            list.add(Arrays.copyOfRange(str, start, i));
        }

        return list;
    }

    static int indexOf(byte[] a, byte b) {
        if (a == null || a.length == 0) {
            return -1;
        }

        if (a.length == 1) {
            return a[0] == b ? 0 : -1;
        }
        for (int i = 0; i < a.length; i++) {
            if (a[i] == b) return i;
        }

        return -1;
    }
}
