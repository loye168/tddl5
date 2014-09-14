package com.taobao.ustore.repo.hbase.cursor;

import java.io.UnsupportedEncodingException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 1.0
 * @date 2012-8-27下午04:38:50
 */
public class BytesUtils {

    public static double getDouble(byte[] bits, int offset) {
        long valueAsLong = (bits[offset + 0] & 0xff) | ((long) (bits[offset + 1] & 0xff) << 8)
                           | ((long) (bits[offset + 2] & 0xff) << 16) | ((long) (bits[offset + 3] & 0xff) << 24)
                           | ((long) (bits[offset + 4] & 0xff) << 32) | ((long) (bits[offset + 5] & 0xff) << 40)
                           | ((long) (bits[offset + 6] & 0xff) << 48) | ((long) (bits[offset + 7] & 0xff) << 56);
        return Double.longBitsToDouble(valueAsLong);
    }

    public static float getFloat(byte[] bits, int offset) {
        int asInt = (bits[offset + 0] & 0xff) | ((bits[offset + 1] & 0xff) << 8) | ((bits[offset + 2] & 0xff) << 16)
                    | ((bits[offset + 3] & 0xff) << 24);
        return Float.intBitsToFloat(asInt);
    }

    public static int getInt(byte[] bits, int offset) {
        int valueAsInt;
        if (bits.length == 4) {
            valueAsInt = (bits[offset + 0] & 0xff) | ((bits[offset + 1] & 0xff) << 8)
                         | ((bits[offset + 2] & 0xff) << 16) | ((bits[offset + 3] & 0xff) << 24);
        } else if (bits.length == 3) {
            valueAsInt = (bits[offset + 0] & 0xff) | ((bits[offset + 1] & 0xff) << 8)
                         | ((bits[offset + 2] & 0xff) << 16);
        } else if (bits.length == 2) {
            valueAsInt = (bits[offset + 0] & 0xff) | ((bits[offset + 1] & 0xff) << 8);
        } else if (bits.length == 1) {
            valueAsInt = (bits[offset + 0] & 0xff);
        }else
        {
            throw new RuntimeException("这个整数的位数不支持，请自行在rowcoder中解析"+bits);
        }
        return valueAsInt;
    }

    public static long getLong(byte[] bits, int offset) {
        long valueAsLong = (bits[offset + 0] & 0xff) | ((long) (bits[offset + 1] & 0xff) << 8)
                           | ((long) (bits[offset + 2] & 0xff) << 16) | ((long) (bits[offset + 3] & 0xff) << 24)
                           | ((long) (bits[offset + 4] & 0xff) << 32) | ((long) (bits[offset + 5] & 0xff) << 40)
                           | ((long) (bits[offset + 6] & 0xff) << 48) | ((long) (bits[offset + 7] & 0xff) << 56);
        return valueAsLong;
    }

    public static short getShort(byte[] bits, int offset) {
        short asShort = (short) ((bits[offset + 0] & 0xff) | ((bits[offset + 1] & 0xff) << 8));
        return asShort;
    }

    public static byte getByte(byte[] bits, int offset) {
        byte asByte = (byte) ((bits[offset + 0] & 0xff));
        return asByte;
    }

    public static String getString(byte[] value, int offset, int length, String encoding) {
        String stringVal = null;
        if (encoding == null) {
            stringVal = new String(value);
        } else {
            try {
                stringVal = new String(value, offset, length, encoding);
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }
        return stringVal;
    }

    public static byte[] stringToBytes(String s, String encoding) {
        if (encoding == null) {
            return s.getBytes();
        } else {
            try {
                return s.getBytes(encoding);
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static byte[] doubleToBytes(double d) {
        return longToBytes(Double.doubleToLongBits(d));
    }

    public static byte[] floatToBytes(float f) {
        return intToBytes(Float.floatToIntBits(f));
    }

    public static byte[] intToBytes(int i) {
        byte[] b = new byte[4];
        b[0] = (byte) (i & 0xff);
        b[1] = (byte) (i >>> 8);
        b[2] = (byte) (i >>> 16);
        b[3] = (byte) (i >>> 24);
        return b;
    }

    public static byte[] longToBytes(long l) {
        byte[] b = new byte[8];
        b[0] = (byte) (l & 0xff);
        b[1] = (byte) (l >>> 8);
        b[2] = (byte) (l >>> 16);
        b[3] = (byte) (l >>> 24);
        b[4] = (byte) (l >>> 32);
        b[5] = (byte) (l >>> 40);
        b[6] = (byte) (l >>> 48);
        b[7] = (byte) (l >>> 56);
        return b;
    }

    public static byte[] shortToBytes(short s) {
        byte[] b = new byte[2];
        b[0] = (byte) (s & 0xff);
        b[1] = (byte) (s >>> 8);
        return b;
    }

    public static byte[] byteToBytes(byte b) {
        return new byte[] { b };
    }

    public static byte[] timeToBytes(Time t) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(t);

        byte[] b = new byte[8];
        b[0] = (byte) 0;
        b[1] = (byte) (19 & 0xff);
        b[2] = (byte) (70 & 0xff);
        b[3] = (byte) (1 & 0xff);
        b[4] = (byte) (1 & 0xff);
        b[5] = (byte) (cal.get(Calendar.HOUR) & 0xff);
        b[6] = (byte) (cal.get(Calendar.MINUTE) & 0xff);
        b[7] = (byte) (cal.get(Calendar.SECOND) & 0xff);
        return b;
    }

    public static byte[] dateToBytes(Date d) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(d);

        byte[] b = new byte[5];
        b[0] = (byte) 0;
        b[1] = (byte) (cal.get(Calendar.YEAR) & 0xff);
        b[2] = (byte) (cal.get(Calendar.YEAR) >>> 8);
        b[3] = (byte) (cal.get(Calendar.MONTH) & 0xff);
        b[4] = (byte) (cal.get(Calendar.DATE) & 0xff);
        return b;
    }

    public static byte[] timeStampToBytes(Timestamp ts) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(ts);

        byte[] b = new byte[12];
        b[0] = (byte) 0;
        b[1] = (byte) (cal.get(Calendar.YEAR) & 0xff);
        b[2] = (byte) (cal.get(Calendar.YEAR) >>> 8);
        b[3] = (byte) (cal.get(Calendar.MONTH) & 0xff);
        b[4] = (byte) (cal.get(Calendar.DATE) & 0xff);
        b[5] = (byte) (cal.get(Calendar.HOUR) & 0xff);
        b[6] = (byte) (cal.get(Calendar.MINUTE) & 0xff);
        b[7] = (byte) (cal.get(Calendar.SECOND) & 0xff);
        b[8] = (byte) (ts.getNanos() & 0xff);
        b[9] = (byte) (ts.getNanos() >>> 8);
        b[10] = (byte) (ts.getNanos() >>> 16);
        b[11] = (byte) (ts.getNanos() >>> 24);
        return b;
    }
}
