package com.taobao.tddl.optimizer.core.datatype;


/**
 * {@link Double}类型
 * 
 * @author jianghang 2014-1-21 下午3:28:33
 * @since 5.0.0
 */
public class DoubleType extends CommonType<Double> {

    private final Calculator calculator = new AbstractCalculator() {

                                            @Override
                                            public Object doAdd(Object v1, Object v2) {
                                                Double i1 = convertFrom(v1);
                                                Double i2 = convertFrom(v2);
                                                return i1 + i2;
                                            }

                                            @Override
                                            public Object doSub(Object v1, Object v2) {
                                                Double i1 = convertFrom(v1);
                                                Double i2 = convertFrom(v2);
                                                return i1 - i2;
                                            }

                                            @Override
                                            public Object doMultiply(Object v1, Object v2) {
                                                Double i1 = convertFrom(v1);
                                                Double i2 = convertFrom(v2);
                                                return i1 * i2;
                                            }

                                            @Override
                                            public Object doDivide(Object v1, Object v2) {
                                                Double i1 = convertFrom(v1);
                                                Double i2 = convertFrom(v2);

                                                if (i2.equals(0.0d)) {
                                                    return null;
                                                }
                                                return i1 / i2;
                                            }

                                            @Override
                                            public Object doMod(Object v1, Object v2) {
                                                Double i1 = convertFrom(v1);
                                                Double i2 = convertFrom(v2);

                                                if (i2.equals(0.0d)) {
                                                    return null;
                                                }

                                                return i1 % i2;
                                            }

                                            @Override
                                            public Object doAnd(Object v1, Object v2) {
                                                Double i1 = convertFrom(v1);
                                                Double i2 = convertFrom(v2);
                                                return (i1 != 0) && (i2 != 0);
                                            }

                                            @Override
                                            public Object doOr(Object v1, Object v2) {
                                                Double i1 = convertFrom(v1);
                                                Double i2 = convertFrom(v2);
                                                return (i1 != 0) || (i2 != 0);
                                            }

                                            @Override
                                            public Object doNot(Object v1) {
                                                Double i1 = convertFrom(v1);

                                                return i1 == 0;
                                            }

                                            @Override
                                            public Object doBitAnd(Object v1, Object v2) {
                                                Double i1 = convertFrom(v1);
                                                Double i2 = convertFrom(v2);
                                                return i1.longValue() & i2.longValue();
                                            }

                                            @Override
                                            public Object doBitOr(Object v1, Object v2) {
                                                Double i1 = convertFrom(v1);
                                                Double i2 = convertFrom(v2);
                                                return i1.longValue() | i2.longValue();
                                            }

                                            @Override
                                            public Object doBitNot(Object v1) {
                                                Double i1 = convertFrom(v1);
                                                return ~i1.longValue();
                                            }

                                            @Override
                                            public Object doXor(Object v1, Object v2) {
                                                Double i1 = convertFrom(v1);
                                                Double i2 = convertFrom(v2);
                                                return (i1 != 0) ^ (i2 != 0);
                                            }

                                            @Override
                                            public Object doBitXor(Object v1, Object v2) {
                                                Double i1 = convertFrom(v1);
                                                Double i2 = convertFrom(v2);
                                                return i1.longValue() ^ i2.longValue();
                                            }
                                        };

    @Override
    public int encodeToBytes(Object value, byte[] dst, int offset) {
        DataEncoder.encode(this.convertFrom(value), dst, offset);
        return getLength(null);
    }

    @Override
    public int getLength(Object value) {
        if (value == null) {
            return 8;
        } else {
            return 8;
        }
    }

    @Override
    public DecodeResult decodeFromBytes(byte[] bytes, int offset) {
        Double v = DataDecoder.decodeDoubleObj(bytes, offset);
        return new DecodeResult(v, getLength(v));
    }

    @Override
    public Double incr(Object value) {
        return this.convertFrom(value) + 0.000001d;
    }

    @Override
    public Double decr(Object value) {
        return this.convertFrom(value) - 0.000001d;
    }

    @Override
    public Double getMaxValue() {
        return Double.MAX_VALUE;
    }

    @Override
    public Double getMinValue() {
        return Double.MIN_VALUE;
    }

    @Override
    public Calculator getCalculator() {
        return calculator;
    }

    @Override
    public int getSqlType() {
        return java.sql.Types.DOUBLE;
    }

}
