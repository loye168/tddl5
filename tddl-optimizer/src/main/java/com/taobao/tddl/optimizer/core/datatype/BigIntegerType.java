package com.taobao.tddl.optimizer.core.datatype;

import java.math.BigInteger;

/**
 * {@link BigInteger}类型
 * 
 * @author jianghang 2014-1-21 下午1:49:09
 * @since 5.0.0
 */
public class BigIntegerType extends CommonType<BigInteger> {

    private static final BigInteger maxValue   = BigInteger.valueOf(Long.MAX_VALUE);
    private static final BigInteger minValue   = BigInteger.valueOf(Long.MIN_VALUE);
    private static final BigInteger zeroValue  = BigInteger.valueOf(0);
    private final Calculator        calculator = new AbstractCalculator() {

                                                   @Override
                                                   public Object doAdd(Object v1, Object v2) {
                                                       BigInteger i1 = convertFrom(v1);
                                                       BigInteger i2 = convertFrom(v2);
                                                       return i1.add(i2);
                                                   }

                                                   @Override
                                                   public Object doSub(Object v1, Object v2) {
                                                       BigInteger i1 = convertFrom(v1);
                                                       BigInteger i2 = convertFrom(v2);
                                                       return i1.subtract(i2);
                                                   }

                                                   @Override
                                                   public Object doMultiply(Object v1, Object v2) {
                                                       BigInteger i1 = convertFrom(v1);
                                                       BigInteger i2 = convertFrom(v2);
                                                       return i1.multiply(i2);
                                                   }

                                                   @Override
                                                   public Object doDivide(Object v1, Object v2) {
                                                       BigInteger i1 = convertFrom(v1);
                                                       BigInteger i2 = convertFrom(v2);

                                                       if (i2.equals(BigInteger.ZERO)) {
                                                           return null;
                                                       }

                                                       return i1.divide(i2);
                                                   }

                                                   @Override
                                                   public Object doMod(Object v1, Object v2) {
                                                       BigInteger i1 = convertFrom(v1);
                                                       BigInteger i2 = convertFrom(v2);

                                                       if (i2.equals(BigInteger.ZERO)) {
                                                           return null;
                                                       }

                                                       return i1.remainder(i2);
                                                   }

                                                   @Override
                                                   public Object doAnd(Object v1, Object v2) {
                                                       BigInteger i1 = convertFrom(v1);
                                                       BigInteger i2 = convertFrom(v2);
                                                       return (i1.compareTo(zeroValue) != 0)
                                                              && (i2.compareTo(zeroValue) != 0);
                                                   }

                                                   @Override
                                                   public Object doOr(Object v1, Object v2) {
                                                       BigInteger i1 = convertFrom(v1);
                                                       BigInteger i2 = convertFrom(v2);
                                                       return (i1.compareTo(zeroValue) != 0)
                                                              || (i2.compareTo(zeroValue) != 0);
                                                   }

                                                   @Override
                                                   public Object doNot(Object v1) {
                                                       BigInteger i1 = convertFrom(v1);

                                                       return (i1.compareTo(zeroValue) == 0);
                                                   }

                                                   @Override
                                                   public Object doBitAnd(Object v1, Object v2) {
                                                       BigInteger i1 = convertFrom(v1);
                                                       BigInteger i2 = convertFrom(v2);
                                                       return i1.and(i2);
                                                   }

                                                   @Override
                                                   public Object doBitOr(Object v1, Object v2) {
                                                       BigInteger i1 = convertFrom(v1);
                                                       BigInteger i2 = convertFrom(v2);
                                                       return i1.or(i2);
                                                   }

                                                   @Override
                                                   public Object doBitNot(Object v1) {
                                                       BigInteger i1 = convertFrom(v1);
                                                       return i1.not();
                                                   }

                                                   @Override
                                                   public Object doXor(Object v1, Object v2) {
                                                       BigInteger i1 = convertFrom(v1);
                                                       BigInteger i2 = convertFrom(v2);
                                                       return (i1.compareTo(zeroValue) != 0)
                                                              ^ (i2.compareTo(zeroValue) != 0);
                                                   }

                                                   @Override
                                                   public Object doBitXor(Object v1, Object v2) {
                                                       BigInteger i1 = convertFrom(v1);
                                                       BigInteger i2 = convertFrom(v2);
                                                       return i1.xor(i2);
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
            return DataEncoder.calculateEncodedLength(convertFrom(value));
        }
    }

    @Override
    public DecodeResult decodeFromBytes(byte[] bytes, int offset) {
        BigInteger[] vs = new BigInteger[1];
        int lenght = DataDecoder.decode(bytes, offset, vs);
        return new DecodeResult(vs[0], lenght);
    }

    @Override
    public BigInteger incr(Object value) {
        return convertFrom(value).add(BigInteger.ONE);
    }

    @Override
    public BigInteger decr(Object value) {
        return convertFrom(value).subtract(BigInteger.ONE);
    }

    @Override
    public BigInteger getMaxValue() {
        return maxValue;
    }

    @Override
    public BigInteger getMinValue() {
        return minValue;
    }

    @Override
    public Calculator getCalculator() {
        return calculator;
    }

    @Override
    public int getSqlType() {
        return java.sql.Types.DECIMAL;
    }

}
