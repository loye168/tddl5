package com.taobao.tddl.executor.rowset;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

import com.taobao.tddl.executor.cursor.ICursorMeta;

/**
 * 可以用来给列改名，去除一个列
 * 
 * @author mengshi.sunmengshi 2013-12-3 上午11:05:57
 * @since 5.0.0
 */
public class RowSetWrapper extends AbstractRowSet implements IRowSet {

    protected final ICursorMeta newCursorMeta;
    protected IRowSet           parentRowSet;

    public RowSetWrapper(ICursorMeta iCursorMeta, IRowSet rowSet){
        super(iCursorMeta);
        this.newCursorMeta = iCursorMeta;
        this.parentRowSet = rowSet;
    }

    @Override
    public Object getObject(int index) {
        return parentRowSet.getObject(index);
    }

    @Override
    public void setObject(int index, Object value) {
        parentRowSet.setObject(index, value);
    }

    @Override
    public Integer getInteger(int index) {
        return parentRowSet.getInteger(index);
    }

    @Override
    public void setInteger(int index, Integer value) {
        parentRowSet.setInteger(index, value);
    }

    @Override
    public Long getLong(int index) {
        return parentRowSet.getLong(index);
    }

    @Override
    public void setLong(int index, Long value) {
        parentRowSet.setLong(index, value);
    }

    @Override
    public List<Object> getValues() {
        return parentRowSet.getValues();
    }

    @Override
    public ICursorMeta getParentCursorMeta() {
        return newCursorMeta;
    }

    @Override
    public String getString(int index) {
        return parentRowSet.getString(index);
    }

    @Override
    public void setString(int index, String str) {
        parentRowSet.setString(index, str);
    }

    @Override
    public Boolean getBoolean(int index) {
        return parentRowSet.getBoolean(index);
    }

    @Override
    public void setBoolean(int index, Boolean bool) {
        parentRowSet.setBoolean(index, bool);
    }

    @Override
    public Short getShort(int index) {
        return parentRowSet.getShort(index);
    }

    @Override
    public void setShort(int index, Short shortval) {
        parentRowSet.setShort(index, shortval);
    }

    @Override
    public Float getFloat(int index) {
        return parentRowSet.getFloat(index);
    }

    @Override
    public void setFloat(int index, Float fl) {
        parentRowSet.setFloat(index, fl);
    }

    @Override
    public Double getDouble(int index) {
        return parentRowSet.getDouble(index);
    }

    @Override
    public void setDouble(int index, Double doub) {
        parentRowSet.setDouble(index, doub);
    }

    @Override
    public byte[] getBytes(int index) {
        return parentRowSet.getBytes(index);
    }

    @Override
    public void setBytes(int index, byte[] bytes) {
        parentRowSet.setBytes(index, bytes);
    }

    @Override
    public Date getDate(int index) {
        return parentRowSet.getDate(index);
    }

    @Override
    public void setDate(int index, Date date) {
        parentRowSet.setDate(index, date);
    }

    @Override
    public Timestamp getTimestamp(int index) {
        return parentRowSet.getTimestamp(index);
    }

    @Override
    public void setTimestamp(int index, Timestamp timestamp) {
        parentRowSet.setTimestamp(index, timestamp);
    }

    @Override
    public Time getTime(int index) {
        return parentRowSet.getTime(index);
    }

    @Override
    public void setTime(int index, Time time) {
        parentRowSet.setTime(index, time);
    }

    @Override
    public BigDecimal getBigDecimal(int index) {
        return parentRowSet.getBigDecimal(index);
    }

    @Override
    public void setBigDecimal(int index, BigDecimal bigDecimal) {
        parentRowSet.setBigDecimal(index, bigDecimal);
    }

    @Override
    public List<byte[]> getBytes() {
        return parentRowSet.getBytes();
    }

    @Override
    public Byte getByte(int index) {
        return parentRowSet.getByte(index);
    }

    @Override
    public void setByte(int index, Byte b) {
        parentRowSet.setByte(index, b);
    }

    public IRowSet getParentRowSet() {
        return parentRowSet;
    }

    public void setParentRowSet(IRowSet parentRowSet) {
        this.parentRowSet = parentRowSet;
    }

}
