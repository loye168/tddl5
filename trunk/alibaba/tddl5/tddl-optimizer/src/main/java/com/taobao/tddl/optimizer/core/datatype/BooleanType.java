package com.taobao.tddl.optimizer.core.datatype;

import com.taobao.tddl.common.exception.NotSupportException;

public class BooleanType extends CommonType<Boolean> {

    private final Calculator calculator = new AbstractCalculator() {

                                            @Override
                                            public Object doAdd(Object v1, Object v2) {
                                                Boolean i1 = convertFrom(v1);
                                                Boolean i2 = convertFrom(v2);
                                                return (i1 ? 1 : 0) + (i2 ? 1 : 0);
                                            }

                                            @Override
                                            public Object doSub(Object v1, Object v2) {
                                                Boolean i1 = convertFrom(v1);
                                                Boolean i2 = convertFrom(v2);
                                                return (i1 ? 1 : 0) - (i2 ? 1 : 0);
                                            }

                                            @Override
                                            public Object doMultiply(Object v1, Object v2) {
                                                Boolean i1 = convertFrom(v1);
                                                Boolean i2 = convertFrom(v2);
                                                return (i1 ? 1 : 0) * (i2 ? 1 : 0);
                                            }

                                            @Override
                                            public Object doDivide(Object v1, Object v2) {
                                                Boolean i1 = convertFrom(v1);
                                                Boolean i2 = convertFrom(v2);

                                                if (i2 == false) {
                                                    return null;
                                                }

                                                return (i1 ? 1 : 0) / (i2 ? 1 : 0);
                                            }

                                            @Override
                                            public Object doMod(Object v1, Object v2) {
                                                Boolean i1 = convertFrom(v1);
                                                Boolean i2 = convertFrom(v2);

                                                if (i2 == false) {
                                                    return null;
                                                }
                                                return (i1 ? 1 : 0) % (i2 ? 1 : 0);
                                            }

                                            @Override
                                            public Object doAnd(Object v1, Object v2) {
                                                Boolean i1 = convertFrom(v1);
                                                Boolean i2 = convertFrom(v2);
                                                return (i1) && (i2);
                                            }

                                            @Override
                                            public Object doOr(Object v1, Object v2) {
                                                Boolean i1 = convertFrom(v1);
                                                Boolean i2 = convertFrom(v2);
                                                return (i1) || (i2);
                                            }

                                            @Override
                                            public Object doNot(Object v1) {
                                                Boolean i1 = convertFrom(v1);

                                                return !i1;
                                            }

                                            @Override
                                            public Object doBitAnd(Object v1, Object v2) {
                                                Boolean i1 = convertFrom(v1);
                                                Boolean i2 = convertFrom(v2);
                                                return i1 & i2;
                                            }

                                            @Override
                                            public Object doBitOr(Object v1, Object v2) {
                                                Boolean i1 = convertFrom(v1);
                                                Boolean i2 = convertFrom(v2);
                                                return i1 | i2;
                                            }

                                            @Override
                                            public Object doBitNot(Object v1) {
                                                Boolean i1 = convertFrom(v1);
                                                return !i1;
                                            }

                                            @Override
                                            public Object doXor(Object v1, Object v2) {
                                                Boolean i1 = convertFrom(v1);
                                                Boolean i2 = convertFrom(v2);
                                                return i1 ^ i2;
                                            }

                                            @Override
                                            public Object doBitXor(Object v1, Object v2) {
                                                Boolean i1 = convertFrom(v1);
                                                Boolean i2 = convertFrom(v2);
                                                return i1 ^ i2;
                                            }
                                        };

    @Override
    public int encodeToBytes(Object value, byte[] dst, int offset) {
        DataEncoder.encode(this.convertFrom(value), dst, offset);
        return getLength(null);
    }

    @Override
    public int getLength(Object value) {
        return 1;
    }

    @Override
    public DecodeResult decodeFromBytes(byte[] bytes, int offset) {
        Boolean v = DataDecoder.decodeBooleanObj(bytes, offset);
        return new DecodeResult(v, getLength(v));
    }

    @Override
    public Boolean incr(Object value) {
        throw new NotSupportException("boolean类型不支持incr操作");
    }

    @Override
    public Boolean decr(Object value) {
        throw new NotSupportException("boolean类型不支持decr操作");
    }

    @Override
    public Boolean getMaxValue() {
        return Boolean.TRUE; // 1代表true
    }

    @Override
    public Boolean getMinValue() {
        return Boolean.FALSE; // 0代表false
    }

    @Override
    public Calculator getCalculator() {
        return calculator;
    }

    @Override
    public int getSqlType() {
        return java.sql.Types.BOOLEAN;
    }
}
