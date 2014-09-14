package com.taobao.tddl.optimizer.core.datatype;


/**
 * {@linkplain Long}类型
 * 
 * @author jianghang 2014-1-22 上午10:41:28
 * @since 5.0.0
 */
public class LongType extends CommonType<Long> {

    private final Calculator calculator = new AbstractCalculator() {

                                            @Override
                                            public Object doAdd(Object v1, Object v2) {
                                                Long i1 = convertFrom(v1);
                                                Long i2 = convertFrom(v2);
                                                return i1 + i2;
                                            }

                                            @Override
                                            public Object doSub(Object v1, Object v2) {
                                                Long i1 = convertFrom(v1);
                                                Long i2 = convertFrom(v2);
                                                return i1 - i2;
                                            }

                                            @Override
                                            public Object doMultiply(Object v1, Object v2) {
                                                Long i1 = convertFrom(v1);
                                                Long i2 = convertFrom(v2);
                                                return i1 * i2;
                                            }

                                            @Override
                                            public Object doDivide(Object v1, Object v2) {
                                                Long i1 = convertFrom(v1);
                                                Long i2 = convertFrom(v2);
                                                if (i2 == 0L) {
                                                    return null;
                                                }
                                                return i1 / i2;
                                            }

                                            @Override
                                            public Object doMod(Object v1, Object v2) {
                                                Long i1 = convertFrom(v1);
                                                Long i2 = convertFrom(v2);
                                                if (i2 == 0L) {
                                                    return null;
                                                }
                                                return i1 % i2;
                                            }

                                            @Override
                                            public Object doAnd(Object v1, Object v2) {
                                                Long i1 = convertFrom(v1);
                                                Long i2 = convertFrom(v2);
                                                return (i1 != 0) && (i2 != 0);
                                            }

                                            @Override
                                            public Object doOr(Object v1, Object v2) {
                                                Long i1 = convertFrom(v1);
                                                Long i2 = convertFrom(v2);
                                                return (i1 != 0) || (i2 != 0);
                                            }

                                            @Override
                                            public Object doNot(Object v1) {
                                                Long i1 = convertFrom(v1);

                                                return i1 == 0;
                                            }

                                            @Override
                                            public Object doBitAnd(Object v1, Object v2) {
                                                Long i1 = convertFrom(v1);
                                                Long i2 = convertFrom(v2);
                                                return i1 & i2;
                                            }

                                            @Override
                                            public Object doBitOr(Object v1, Object v2) {
                                                Long i1 = convertFrom(v1);
                                                Long i2 = convertFrom(v2);
                                                return i1 | i2;
                                            }

                                            @Override
                                            public Object doBitNot(Object v1) {
                                                Long i1 = convertFrom(v1);
                                                return ~i1;
                                            }

                                            @Override
                                            public Object doXor(Object v1, Object v2) {
                                                Long i1 = convertFrom(v1);
                                                Long i2 = convertFrom(v2);
                                                return (i1 != 0) ^ (i2 != 0);
                                            }

                                            @Override
                                            public Object doBitXor(Object v1, Object v2) {
                                                Long i1 = convertFrom(v1);
                                                Long i2 = convertFrom(v2);
                                                return i1 ^ i2;
                                            }
                                        };

    @Override
    public int encodeToBytes(Object value, byte[] dst, int offset) {
        return DataEncoder.encode(this.convertFrom(value), dst, offset);
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
        return new DecodeResult(v, getLength(v));
    }

    @Override
    public Long incr(Object value) {
        return convertFrom(value) + 1;
    }

    @Override
    public Long decr(Object value) {
        return convertFrom(value) - 1;
    }

    @Override
    public Long getMaxValue() {
        return Long.MAX_VALUE;
    }

    @Override
    public Long getMinValue() {
        return Long.MIN_VALUE;
    }

    @Override
    public Calculator getCalculator() {
        return calculator;
    }

    @Override
    public int getSqlType() {
        return java.sql.Types.BIGINT;
    }
}
