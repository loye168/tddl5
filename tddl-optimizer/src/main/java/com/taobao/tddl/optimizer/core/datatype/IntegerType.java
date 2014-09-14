package com.taobao.tddl.optimizer.core.datatype;


/**
 * int/Integer类型
 * 
 * @since 5.0.0
 */
public class IntegerType extends CommonType<Integer> {

    private final Calculator calculator = new AbstractCalculator() {

                                            @Override
                                            public Object doAdd(Object v1, Object v2) {
                                                Integer i1 = convertFrom(v1);
                                                Integer i2 = convertFrom(v2);
                                                return i1 + i2;
                                            }

                                            @Override
                                            public Object doSub(Object v1, Object v2) {
                                                Integer i1 = convertFrom(v1);
                                                Integer i2 = convertFrom(v2);
                                                return i1 - i2;
                                            }

                                            @Override
                                            public Object doMultiply(Object v1, Object v2) {
                                                Integer i1 = convertFrom(v1);
                                                Integer i2 = convertFrom(v2);
                                                return i1 * i2;
                                            }

                                            @Override
                                            public Object doDivide(Object v1, Object v2) {
                                                Integer i1 = convertFrom(v1);
                                                Integer i2 = convertFrom(v2);

                                                if (i2 == 0) {
                                                    return null;
                                                }
                                                return i1 / i2;
                                            }

                                            @Override
                                            public Object doMod(Object v1, Object v2) {
                                                Integer i1 = convertFrom(v1);
                                                Integer i2 = convertFrom(v2);

                                                if (i2 == 0) {
                                                    return null;
                                                }

                                                return i1 % i2;
                                            }

                                            @Override
                                            public Object doAnd(Object v1, Object v2) {
                                                Integer i1 = convertFrom(v1);
                                                Integer i2 = convertFrom(v2);
                                                return (i1 != 0) && (i2 != 0);
                                            }

                                            @Override
                                            public Object doOr(Object v1, Object v2) {
                                                Integer i1 = convertFrom(v1);
                                                Integer i2 = convertFrom(v2);
                                                return (i1 != 0) || (i2 != 0);
                                            }

                                            @Override
                                            public Object doNot(Object v1) {
                                                Integer i1 = convertFrom(v1);

                                                return i1 == 0;
                                            }

                                            @Override
                                            public Object doBitAnd(Object v1, Object v2) {
                                                Integer i1 = convertFrom(v1);
                                                Integer i2 = convertFrom(v2);
                                                return i1 & i2;
                                            }

                                            @Override
                                            public Object doBitOr(Object v1, Object v2) {
                                                Integer i1 = convertFrom(v1);
                                                Integer i2 = convertFrom(v2);
                                                return i1 | i2;
                                            }

                                            @Override
                                            public Object doBitNot(Object v1) {
                                                Integer i1 = convertFrom(v1);
                                                return ~i1;
                                            }

                                            @Override
                                            public Object doXor(Object v1, Object v2) {
                                                Integer i1 = convertFrom(v1);
                                                Integer i2 = convertFrom(v2);
                                                return (i1 != 0) ^ (i2 != 0);
                                            }

                                            @Override
                                            public Object doBitXor(Object v1, Object v2) {
                                                Integer i1 = convertFrom(v1);
                                                Integer i2 = convertFrom(v2);
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
            return 5;
        }
    }

    @Override
    public DecodeResult decodeFromBytes(byte[] bytes, int offset) {
        Integer v = DataDecoder.decodeIntegerObj(bytes, offset);
        return new DecodeResult(v, getLength(v));
    }

    @Override
    public Integer incr(Object value) {
        return convertFrom(value) + 1;
    }

    @Override
    public Integer decr(Object value) {
        return convertFrom(value) - 1;
    }

    @Override
    public Integer getMaxValue() {
        return Integer.MAX_VALUE;
    }

    @Override
    public Integer getMinValue() {
        return Integer.MIN_VALUE;
    }

    @Override
    public Calculator getCalculator() {
        return calculator;
    }

    @Override
    public int getSqlType() {
        return java.sql.Types.INTEGER;
    }
}
