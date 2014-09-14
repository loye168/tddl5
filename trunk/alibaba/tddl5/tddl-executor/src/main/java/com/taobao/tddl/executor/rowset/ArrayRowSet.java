package com.taobao.tddl.executor.rowset;

import java.util.Arrays;
import java.util.List;

import com.taobao.tddl.executor.cursor.ICursorMeta;

/**
 * 基于数组的结果集。是最基本的一行数据的形式，效率最快。
 * 
 * @author Whisper
 */
public class ArrayRowSet extends AbstractRowSet implements IRowSet {

    Object[]     row;
    List<byte[]> rowBytes;

    public ArrayRowSet(int capacity, ICursorMeta iCursorMeta){
        super(iCursorMeta);
        row = new Object[capacity];
    }

    public ArrayRowSet(ICursorMeta iCursorMeta, Object[] row){
        super(iCursorMeta);
        this.row = row;
    }

    public ArrayRowSet(ICursorMeta iCursorMeta, Object[] row, List<byte[]> rowBytes){
        super(iCursorMeta);
        this.row = row;
        this.rowBytes = rowBytes;
    }

    @Override
    public byte[] getBytes(int index) {
        if (this.rowBytes != null) {
            return rowBytes.get(index);
        }
        return super.getBytes(index);
    }

    @Override
    public void setBytes(int index, byte[] bytes) {
        rowBytes.set(index, bytes);
    }

    @Override
    public Object getObject(int index) {
        return row[index];
    }

    @Override
    public void setObject(int index, Object value) {
        row[index] = value;
    }

    @Override
    public List<Object> getValues() {
        return Arrays.asList(row);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(row);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        ArrayRowSet other = (ArrayRowSet) obj;
        if (!Arrays.equals(row, other.row)) return false;
        return true;
    }

}
