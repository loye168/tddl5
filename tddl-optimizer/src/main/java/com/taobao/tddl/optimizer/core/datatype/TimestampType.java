package com.taobao.tddl.optimizer.core.datatype;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.common.model.BaseRowSet;
import com.taobao.tddl.common.utils.convertor.Convertor;

/**
 * {@link Timestamp}类型
 * 
 * @author jianghang 2014-1-21 下午5:36:26
 * @since 5.0.0
 */
public class TimestampType extends AbstractDataType<java.sql.Timestamp> {

    private static final Timestamp maxTimestamp = Timestamp.valueOf("9999-12-31 23:59:59");
    private static final Timestamp minTimestamp = Timestamp.valueOf("1900-01-01 00:00:00");
    private Convertor              longToDate   = null;

    private final Calculator       calculator   = new AbstractCalculator() {

                                                    @Override
                                                    public Object doAdd(Object v1, Object v2) {
                                                        Calendar cal = Calendar.getInstance();
                                                        if (v1 instanceof IntervalType) {
                                                            Timestamp i2 = convertFrom(v2);
                                                            cal.setTime(i2);
                                                            ((IntervalType) v1).process(cal, 1);
                                                        } else if (v2 instanceof IntervalType) {
                                                            Timestamp i1 = convertFrom(v1);
                                                            cal.setTime(i1);
                                                            ((IntervalType) v2).process(cal, 1);
                                                        } else {
                                                            throw new NotSupportException("时间类型不支持算术符操作");
                                                        }

                                                        return convertFrom(cal.getTime());
                                                    }

                                                    @Override
                                                    public Object doSub(Object v1, Object v2) {
                                                        Calendar cal = Calendar.getInstance();
                                                        if (v1 instanceof IntervalType) {
                                                            Timestamp i2 = convertFrom(v2);
                                                            cal.setTime(i2);
                                                            ((IntervalType) v1).process(cal, -1);
                                                        } else if (v2 instanceof IntervalType) {
                                                            Timestamp i1 = convertFrom(v1);
                                                            cal.setTime(i1);
                                                            ((IntervalType) v2).process(cal, -1);
                                                        } else {
                                                            throw new NotSupportException("时间类型不支持算术符操作");
                                                        }

                                                        return convertFrom(cal.getTime());
                                                    }

                                                    @Override
                                                    public Object doMultiply(Object v1, Object v2) {
                                                        throw new NotSupportException("时间类型不支持算术符操作");
                                                    }

                                                    @Override
                                                    public Object doDivide(Object v1, Object v2) {
                                                        throw new NotSupportException("时间类型不支持算术符操作");
                                                    }

                                                    @Override
                                                    public Object doMod(Object v1, Object v2) {
                                                        throw new NotSupportException("时间类型不支持算术符操作");
                                                    }

                                                    @Override
                                                    public Object doAnd(Object v1, Object v2) {
                                                        throw new NotSupportException("时间类型不支持算术符操作");
                                                    }

                                                    @Override
                                                    public Object doOr(Object v1, Object v2) {
                                                        throw new NotSupportException("时间类型不支持算术符操作");
                                                    }

                                                    @Override
                                                    public Object doNot(Object v1) {
                                                        throw new NotSupportException("时间类型不支持算术符操作");
                                                    }

                                                    @Override
                                                    public Object doBitAnd(Object v1, Object v2) {
                                                        throw new NotSupportException("时间类型不支持算术符操作");
                                                    }

                                                    @Override
                                                    public Object doBitOr(Object v1, Object v2) {
                                                        throw new NotSupportException("时间类型不支持算术符操作");
                                                    }

                                                    @Override
                                                    public Object doBitNot(Object v1) {
                                                        throw new NotSupportException("时间类型不支持算术符操作");
                                                    }

                                                    @Override
                                                    public Object doXor(Object v1, Object v2) {
                                                        throw new NotSupportException("时间类型不支持算术符操作");
                                                    }

                                                    @Override
                                                    public Object doBitXor(Object v1, Object v2) {
                                                        throw new NotSupportException("时间类型不支持算术符操作");
                                                    }
                                                };

    public TimestampType(){
        longToDate = this.getConvertor(Long.class);
    }

    @Override
    public ResultGetter getResultGetter() {
        return new ResultGetter() {

            @Override
            public Object get(ResultSet rs, int index) throws SQLException {
                return rs.getTimestamp(index);
            }

            @Override
            public Object get(BaseRowSet rs, int index) {
                Object val = rs.getObject(index);
                return convertFrom(val);
            }
        };
    }

    @Override
    public int encodeToBytes(Object value, byte[] dst, int offset) {
        return DataEncoder.encode(DataType.LongType.convertFrom(value), dst, offset);
    }

    @Override
    public int getLength(Object value) {
        if (value == null) {
            return 1;
        } else {
            return 9;
        }
    }

    @Override
    public DecodeResult decodeFromBytes(byte[] bytes, int offset) {
        Long v = DataDecoder.decodeLongObj(bytes, offset);
        if (v == null) {
            return new DecodeResult(null, getLength(v));
        } else {
            Timestamp date = (Timestamp) longToDate.convert(v, getDataClass());
            return new DecodeResult(date, getLength(v));
        }
    }

    @Override
    public Timestamp incr(Object value) {
        return new Timestamp(((Timestamp) value).getTime() + 1l);
    }

    @Override
    public Timestamp decr(Object value) {
        return new Timestamp(((Timestamp) value).getTime() - 1l);
    }

    @Override
    public Timestamp getMaxValue() {
        return maxTimestamp;
    }

    @Override
    public Timestamp getMinValue() {
        return minTimestamp;
    }

    @Override
    public int compare(Object o1, Object o2) {
        if (o1 == o2) {
            return 0;
        }
        if (o1 == null) {
            return -1;
        }

        if (o2 == null) {
            return 1;
        }

        Timestamp d1 = convertFrom(o1);
        Timestamp d2 = convertFrom(o2);
        return d1.compareTo(d2);
    }

    @Override
    public Calculator getCalculator() {
        return calculator;
    }

    @Override
    public int getSqlType() {
        return java.sql.Types.TIMESTAMP;
    }
}
