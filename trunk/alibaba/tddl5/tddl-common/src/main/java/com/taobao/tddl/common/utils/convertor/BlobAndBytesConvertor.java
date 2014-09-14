package com.taobao.tddl.common.utils.convertor;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.SQLException;

/**
 * Blob <-> bytes类型之间的转化
 * 
 * @author jianghang 2014-1-21 下午6:15:01
 * @since 5.0.0
 */
public class BlobAndBytesConvertor {

    public static final BigDecimal BIGINT_MAX_VALUE = new BigDecimal("18446744073709551615");

    /**
     * Blob -> bytes 转化
     */
    public static class BlobToBytes extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            if (Blob.class.isInstance(src) && destClass.equals(byte[].class)) {
                if (src == null) {
                    return null;
                } else {
                    try {
                        Blob blob = (Blob) src;
                        return blob.getBytes(0, (int) blob.length());
                    } catch (SQLException e) {
                        throw new ConvertorException(e);
                    }
                }
            }

            throw new ConvertorException("Unsupported convert: [" + src.getClass().getName() + ","
                                         + destClass.getName() + "]");
        }
    }

    public static class NumberToBytes extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            if (Number.class.isInstance(src) && destClass.equals(byte[].class)) {
                if (src == null) {
                    return null;
                } else {
                    return String.valueOf((Number) src).getBytes();
                }
            }

            throw new ConvertorException("Unsupported convert: [" + src.getClass().getName() + ","
                                         + destClass.getName() + "]");
        }
    }

    public static class BitBytesToBigDecimal extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            if (src.getClass().equals(byte[].class) && destClass.equals(BigDecimal.class)) {
                String value = new String((byte[]) src);
                return new BigDecimal(value);
                // byte[] bytes = (byte[]) src;
                //
                // long bitValue = 0;
                // int position = 0;
                // for (int shiftBy = 0; shiftBy < 64 && position <
                // bytes.length; shiftBy += 8) {
                // bitValue |= (long) ((bytes[position++] & 0xff) << shiftBy);
                // }
                //
                // return (bitValue >= 0) ? BigDecimal.valueOf(bitValue) :
                // BIGINT_MAX_VALUE.add(BigDecimal.valueOf(1 + bitValue));
            } else if (Number.class.isInstance(src) && destClass.equals(BigDecimal.class)) {
                return ConvertorHelper.commonToCommon.convert(src, destClass);
            }

            throw new ConvertorException("Unsupported convert: [" + src.getClass().getName() + ","
                                         + destClass.getName() + "]");
        }
    }

}
