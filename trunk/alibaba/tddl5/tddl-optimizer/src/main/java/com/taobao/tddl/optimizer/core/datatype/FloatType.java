package com.taobao.tddl.optimizer.core.datatype;


public class FloatType extends CommonType<Float> {

    private final Calculator calculator = new AbstractCalculator() {

                                            @Override
                                            public Object doAdd(Object v1, Object v2) {
                                                Float i1 = convertFrom(v1);
                                                Float i2 = convertFrom(v2);
                                                return i1 + i2;
                                            }

                                            @Override
                                            public Object doSub(Object v1, Object v2) {
                                                Float i1 = convertFrom(v1);
                                                Float i2 = convertFrom(v2);
                                                return i1 - i2;
                                            }

                                            @Override
                                            public Object doMultiply(Object v1, Object v2) {
                                                Float i1 = convertFrom(v1);
                                                Float i2 = convertFrom(v2);
                                                return i1 * i2;
                                            }

                                            @Override
                                            public Object doDivide(Object v1, Object v2) {
                                                Float i1 = convertFrom(v1);
                                                Float i2 = convertFrom(v2);

                                                if (i2.equals(0.0f)) {
                                                    return null;
                                                }

                                                return i1 / i2;
                                            }

                                            @Override
                                            public Object doMod(Object v1, Object v2) {
                                                Float i1 = convertFrom(v1);
                                                Float i2 = convertFrom(v2);

                                                if (i2.equals(0.0f)) {
                                                    return null;
                                                }
                                                return i1 % i2;
                                            }

                                            @Override
                                            public Object doAnd(Object v1, Object v2) {
                                                Float i1 = convertFrom(v1);
                                                Float i2 = convertFrom(v2);
                                                return (i1 != 0) && (i2 != 0);
                                            }

                                            @Override
                                            public Object doOr(Object v1, Object v2) {
                                                Float i1 = convertFrom(v1);
                                                Float i2 = convertFrom(v2);
                                                return (i1 != 0) || (i2 != 0);
                                            }

                                            @Override
                                            public Object doNot(Object v1) {
                                                Float i1 = convertFrom(v1);

                                                return i1 == 0;
                                            }

                                            @Override
                                            public Object doBitAnd(Object v1, Object v2) {
                                                Float i1 = convertFrom(v1);
                                                Float i2 = convertFrom(v2);
                                                return i1.longValue() & i2.longValue();
                                            }

                                            @Override
                                            public Object doBitOr(Object v1, Object v2) {
                                                Float i1 = convertFrom(v1);
                                                Float i2 = convertFrom(v2);
                                                return i1.longValue() | i2.longValue();
                                            }

                                            @Override
                                            public Object doBitNot(Object v1) {
                                                Float i1 = convertFrom(v1);
                                                return ~i1.longValue();
                                            }

                                            @Override
                                            public Object doXor(Object v1, Object v2) {
                                                Float i1 = convertFrom(v1);
                                                Float i2 = convertFrom(v2);
                                                return (i1 != 0) ^ (i2 != 0);
                                            }

                                            @Override
                                            public Object doBitXor(Object v1, Object v2) {
                                                Float i1 = convertFrom(v1);
                                                Float i2 = convertFrom(v2);
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
            return 4;
        } else {
            return 4;
        }
    }

    @Override
    public DecodeResult decodeFromBytes(byte[] bytes, int offset) {
        Float v = DataDecoder.decodeFloatObj(bytes, offset);
        return new DecodeResult(v, getLength(v));
    }

    @Override
    public Float incr(Object value) {
        return this.convertFrom(value) + 0.000001f;
    }

    @Override
    public Float decr(Object value) {
        return this.convertFrom(value) - 0.000001f;
    }

    @Override
    public Float getMaxValue() {
        return Float.MAX_VALUE;
    }

    @Override
    public Float getMinValue() {
        return Float.MIN_VALUE;
    }

    @Override
    public Calculator getCalculator() {
        return calculator;
    }

    @Override
    public int getSqlType() {
        return java.sql.Types.FLOAT;
    }
}
