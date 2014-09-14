package com.taobao.tddl.optimizer.core.datatype;

import java.math.BigDecimal;

/**
 * {@link BigDecimal}类型
 * 
 * @author jianghang 2014-1-21 下午1:54:11
 * @since 5.0.0
 */
public class BigDecimalType extends CommonType<BigDecimal> {

    private static final BigDecimal maxValue   = BigDecimal.valueOf(Long.MAX_VALUE);
    private static final BigDecimal minValue   = BigDecimal.valueOf(Long.MIN_VALUE);
    private static final BigDecimal zeroValue  = BigDecimal.valueOf(0);
    private final Calculator        calculator = new AbstractCalculator() {

                                                   @Override
                                                   public Object doAdd(Object v1, Object v2) {
                                                       BigDecimal i1 = convertFrom(v1);
                                                       BigDecimal i2 = convertFrom(v2);
                                                       return i1.add(i2);
                                                   }

                                                   @Override
                                                   public Object doSub(Object v1, Object v2) {
                                                       BigDecimal i1 = convertFrom(v1);
                                                       BigDecimal i2 = convertFrom(v2);
                                                       return i1.subtract(i2);
                                                   }

                                                   @Override
                                                   public Object doMultiply(Object v1, Object v2) {
                                                       BigDecimal i1 = convertFrom(v1);
                                                       BigDecimal i2 = convertFrom(v2);
                                                       return i1.multiply(i2);
                                                   }

                                                   @Override
                                                   public Object doDivide(Object v1, Object v2) {
                                                       BigDecimal i1 = convertFrom(v1);
                                                       BigDecimal i2 = convertFrom(v2);

                                                       if (i2.equals(BigDecimal.ZERO)) {
                                                           return null;
                                                       }
                                                       return i1.divide(i2, 4, BigDecimal.ROUND_HALF_UP);
                                                   }

                                                   @Override
                                                   public Object doMod(Object v1, Object v2) {
                                                       BigDecimal i1 = convertFrom(v1);
                                                       BigDecimal i2 = convertFrom(v2);

                                                       if (i2.equals(BigDecimal.ZERO)) {
                                                           return null;
                                                       }

                                                       return i1.remainder(i2);
                                                   }

                                                   @Override
                                                   public Object doAnd(Object v1, Object v2) {
                                                       BigDecimal i1 = convertFrom(v1);
                                                       BigDecimal i2 = convertFrom(v2);
                                                       return (i1.compareTo(zeroValue) != 0)
                                                              && (i2.compareTo(zeroValue) != 0);
                                                   }

                                                   @Override
                                                   public Object doOr(Object v1, Object v2) {
                                                       BigDecimal i1 = convertFrom(v1);
                                                       BigDecimal i2 = convertFrom(v2);
                                                       return (i1.compareTo(zeroValue) != 0)
                                                              || (i2.compareTo(zeroValue) != 0);
                                                   }

                                                   @Override
                                                   public Object doNot(Object v1) {
                                                       BigDecimal i1 = convertFrom(v1);

                                                       return (i1.compareTo(zeroValue) == 0);
                                                   }

                                                   @Override
                                                   public Object doBitAnd(Object v1, Object v2) {
                                                       BigDecimal i1 = convertFrom(v1);
                                                       BigDecimal i2 = convertFrom(v2);
                                                       return i1.toBigInteger().and(i2.toBigInteger());
                                                   }

                                                   @Override
                                                   public Object doBitOr(Object v1, Object v2) {
                                                       BigDecimal i1 = convertFrom(v1);
                                                       BigDecimal i2 = convertFrom(v2);
                                                       return i1.toBigInteger().or(i2.toBigInteger());
                                                   }

                                                   @Override
                                                   public Object doBitNot(Object v1) {
                                                       BigDecimal i1 = convertFrom(v1);
                                                       return i1.toBigInteger().not();
                                                   }

                                                   @Override
                                                   public Object doXor(Object v1, Object v2) {
                                                       BigDecimal i1 = convertFrom(v1);
                                                       BigDecimal i2 = convertFrom(v2);
                                                       return (i1.compareTo(zeroValue) != 0)
                                                              ^ (i2.compareTo(zeroValue) != 0);
                                                   }

                                                   @Override
                                                   public Object doBitXor(Object v1, Object v2) {
                                                       BigDecimal i1 = convertFrom(v1);
                                                       BigDecimal i2 = convertFrom(v2);
                                                       return i1.toBigInteger().xor(i2.toBigInteger());
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
        BigDecimal[] vs = new BigDecimal[1];
        int lenght = DataDecoder.decode(bytes, offset, vs);
        return new DecodeResult(vs[0], lenght);
    }

    @Override
    public BigDecimal incr(Object value) {
        return convertFrom(value).add(BigDecimal.ONE);
    }

    @Override
    public BigDecimal decr(Object value) {
        return convertFrom(value).subtract(BigDecimal.ONE);
    }

    @Override
    public BigDecimal getMaxValue() {
        return maxValue;
    }

    @Override
    public BigDecimal getMinValue() {
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
