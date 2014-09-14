package com.taobao.tddl.optimizer.core.datatype;


/**
 * {@link Byte} 类型
 * 
 * @author jianghang 2014-1-21 下午3:28:17
 * @since 5.0.0
 */
public class ByteType extends CommonType<Byte> {

    private final Calculator calculator = new AbstractCalculator() {

                                            @Override
                                            public Object doAdd(Object v1, Object v2) {
                                                Byte i1 = convertFrom(v1);
                                                Byte i2 = convertFrom(v2);
                                                return i1 + i2;
                                            }

                                            @Override
                                            public Object doSub(Object v1, Object v2) {
                                                Byte i1 = convertFrom(v1);
                                                Byte i2 = convertFrom(v2);
                                                return i1 - i2;
                                            }

                                            @Override
                                            public Object doMultiply(Object v1, Object v2) {
                                                Byte i1 = convertFrom(v1);
                                                Byte i2 = convertFrom(v2);
                                                return i1 * i2;
                                            }

                                            @Override
                                            public Object doDivide(Object v1, Object v2) {
                                                Byte i1 = convertFrom(v1);
                                                Byte i2 = convertFrom(v2);

                                                if (i2 == 0) {
                                                    return null;
                                                }
                                                return i1 / i2;
                                            }

                                            @Override
                                            public Object doMod(Object v1, Object v2) {
                                                Byte i1 = convertFrom(v1);
                                                Byte i2 = convertFrom(v2);

                                                if (i2 == 0) {
                                                    return null;
                                                }

                                                return i1 % i2;
                                            }

                                            @Override
                                            public Object doAnd(Object v1, Object v2) {
                                                Byte i1 = convertFrom(v1);
                                                Byte i2 = convertFrom(v2);
                                                return (i1 != 0) && (i2 != 0);
                                            }

                                            @Override
                                            public Object doOr(Object v1, Object v2) {
                                                Byte i1 = convertFrom(v1);
                                                Byte i2 = convertFrom(v2);
                                                return (i1 != 0) || (i2 != 0);
                                            }

                                            @Override
                                            public Object doNot(Object v1) {
                                                Byte i1 = convertFrom(v1);

                                                return i1 == 0;
                                            }

                                            @Override
                                            public Object doBitAnd(Object v1, Object v2) {
                                                Byte i1 = convertFrom(v1);
                                                Byte i2 = convertFrom(v2);
                                                return i1 & i2;
                                            }

                                            @Override
                                            public Object doBitOr(Object v1, Object v2) {
                                                Byte i1 = convertFrom(v1);
                                                Byte i2 = convertFrom(v2);
                                                return i1 | i2;
                                            }

                                            @Override
                                            public Object doBitNot(Object v1) {
                                                Byte i1 = convertFrom(v1);
                                                return ~i1;
                                            }

                                            @Override
                                            public Object doXor(Object v1, Object v2) {
                                                Byte i1 = convertFrom(v1);
                                                Byte i2 = convertFrom(v2);
                                                return (i1 != 0) ^ (i2 != 0);
                                            }

                                            @Override
                                            public Object doBitXor(Object v1, Object v2) {
                                                Byte i1 = convertFrom(v1);
                                                Byte i2 = convertFrom(v2);
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
            return 2;
        }
    }

    @Override
    public DecodeResult decodeFromBytes(byte[] bytes, int offset) {
        Byte v = DataDecoder.decodeByteObj(bytes, offset);
        return new DecodeResult(v, getLength(v));
    }

    @Override
    public Byte incr(Object value) {
        return Byte.valueOf(((Integer) (this.convertFrom(value).intValue() + 1)).byteValue());
    }

    @Override
    public Byte decr(Object value) {
        return Byte.valueOf(((Integer) (this.convertFrom(value).intValue() - 1)).byteValue());
    }

    @Override
    public Byte getMaxValue() {
        return Byte.MAX_VALUE;
    }

    @Override
    public Byte getMinValue() {
        return Byte.MIN_VALUE;
    }

    @Override
    public Calculator getCalculator() {
        return calculator;
    }

    @Override
    public int getSqlType() {
        return java.sql.Types.TINYINT;
    }
}
