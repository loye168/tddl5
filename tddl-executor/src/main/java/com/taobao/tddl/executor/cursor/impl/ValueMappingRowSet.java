package com.taobao.tddl.executor.cursor.impl;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;

import com.taobao.tddl.executor.cursor.ICursorMeta;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.rowset.RowSetWrapper;

/**
 * 两个rowset内容相同，但是列顺序不同，可以用此转换
 * 
 * @author mengshi.sunmengshi 2013-12-19 上午11:09:09
 * @since 5.0.0
 */
public class ValueMappingRowSet extends RowSetWrapper {

    public ValueMappingRowSet(ICursorMeta iCursorMeta, IRowSet rowSet,
                              Map<Integer/* 返回列中的index位置 */, Integer/* 实际数据中的index位置 */> mapping){
        super(iCursorMeta, rowSet);
        this.mapping = mapping;
    }

    final Map<Integer/* 返回列中的index位置 */, Integer/* 实际数据中的index位置 */> mapping;

    @Override
    public Object getObject(int index) {
        Integer indexReal = getMappingIndex(index);
        return parentRowSet.getObject(indexReal);
    }

    @Override
    public void setObject(int index, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Integer getInteger(int index) {
        Integer indexReal = getMappingIndex(index);
        return parentRowSet.getInteger(indexReal);
    }

    @Override
    public void setInteger(int index, Integer value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long getLong(int index) {
        Integer indexReal = getMappingIndex(index);
        return parentRowSet.getLong(indexReal);
    }

    @Override
    public void setLong(int index, Long value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getString(int index) {
        Integer indexReal = getMappingIndex(index);
        return parentRowSet.getString(indexReal);
    }

    @Override
    public void setString(int index, String str) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Boolean getBoolean(int index) {
        Integer indexReal = getMappingIndex(index);
        return parentRowSet.getBoolean(indexReal);
    }

    @Override
    public void setBoolean(int index, Boolean bool) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Short getShort(int index) {
        Integer indexReal = getMappingIndex(index);
        return parentRowSet.getShort(indexReal);
    }

    @Override
    public void setShort(int index, Short shortval) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Float getFloat(int index) {
        Integer indexReal = getMappingIndex(index);
        return parentRowSet.getFloat(indexReal);
    }

    @Override
    public void setFloat(int index, Float fl) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Double getDouble(int index) {
        Integer indexReal = getMappingIndex(index);
        return parentRowSet.getDouble(indexReal);
    }

    @Override
    public void setDouble(int index, Double doub) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getBytes(int index) {
        Integer indexReal = getMappingIndex(index);
        return parentRowSet.getBytes(indexReal);
    }

    @Override
    public void setBytes(int index, byte[] bytes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Date getDate(int index) {
        Integer indexReal = getMappingIndex(index);
        return parentRowSet.getDate(indexReal);
    }

    @Override
    public void setDate(int index, Date date) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Timestamp getTimestamp(int index) {
        Integer indexReal = getMappingIndex(index);
        return parentRowSet.getTimestamp(indexReal);
    }

    @Override
    public void setTimestamp(int index, Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Time getTime(int index) {
        Integer indexReal = getMappingIndex(index);
        return parentRowSet.getTime(indexReal);
    }

    @Override
    public void setTime(int index, Time time) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BigDecimal getBigDecimal(int index) {
        Integer indexReal = getMappingIndex(index);
        return parentRowSet.getBigDecimal(indexReal);
    }

    @Override
    public void setBigDecimal(int index, BigDecimal bigDecimal) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Byte getByte(int index) {
        Integer indexReal = getMappingIndex(index);
        return parentRowSet.getByte(indexReal);
    }

    @Override
    public void setByte(int index, Byte b) {
        throw new UnsupportedOperationException();
    }

    private Integer getMappingIndex(int index) {
        Integer indexReal = mapping.get(index);
        if (indexReal == null) {
            indexReal = index;
        }
        return indexReal;
    }
}
