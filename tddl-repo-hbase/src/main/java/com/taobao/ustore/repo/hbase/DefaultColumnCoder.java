package com.taobao.ustore.repo.hbase;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hbase.util.Bytes;

import com.taobao.tddl.optimizer.config.table.HBaseColumnCoder;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.ustore.repo.hbase.cursor.BytesUtils;

public class DefaultColumnCoder implements HBaseColumnCoder {

    @Deprecated
    public static String ENCODING = "utf-8";

    public String getEncoding() {
        return "UTF-8";
    }

    @Override
    public byte[] encodeToBytes(DataType type, Object v) {

        if (type == DataType.LongType) return encodeLongToBytes(v);
        if (type == DataType.IntegerType) return encodeIntToBytes(v);
        if (type == DataType.StringType) return encodeStringToBytes(v);
        if (type == DataType.DoubleType) return encodeDoubleToBytes(v);
        if (type == DataType.DateType) return encodeDateToBytes(v);
        if (type == DataType.TimestampType) return encodeTimestampToBytes(v);
        if (type == DataType.BooleanType) return encodeBooleanToBytes(v);
        if (type == DataType.BytesType) return encodeBytesToBytes(v);
        if (type == DataType.FloatType) return encodeFloatToBytes(v);
        if (type == DataType.ShortType) return encodeShortToBytes(v);

        throw new RuntimeException("not supported DATA_TYPE:" + type.getDataClass());

    }

    protected byte[] encodeShortToBytes(Object v) {
        return Bytes.toBytes((Short) v);
    }

    protected byte[] encodeFloatToBytes(Object v) {
        return Bytes.toBytes((Float) v);
    }

    protected byte[] encodeBytesToBytes(Object v) {
        return (byte[]) v;
    }

    protected byte[] encodeBooleanToBytes(Object v) {
        Boolean b = (Boolean) v;
        if (b) {
            return new byte[] { 1 };
        } else {
            return new byte[] { 0 };
        }
    }

    protected byte[] encodeTimestampToBytes(Object v) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String s = format.format((Date) v);
        return BytesUtils.stringToBytes(s, getEncoding());
    }

    protected byte[] encodeDateToBytes(Object v) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String s = format.format((Date) v);
        return BytesUtils.stringToBytes(s, getEncoding());
    }

    protected byte[] encodeDoubleToBytes(Object v) {
        return Bytes.toBytes((Double) v);
    }

    protected byte[] encodeStringToBytes(Object v) {
        return BytesUtils.stringToBytes((String) v, getEncoding());
    }

    protected byte[] encodeIntToBytes(Object v) {
        return Bytes.toBytes((Integer) v);
    }

    protected byte[] encodeLongToBytes(Object v) {
        return Bytes.toBytes((Long) v);
    }

    @Override
    public Object decodeFromBytes(DataType type, byte[] v) {

        if (type == DataType.LongType) return decodeLongFromBytes(v);
        if (type == DataType.IntegerType) return decodeIntFromBytes(v);
        if (type == DataType.StringType) return decodeStringFromBytes(v);
        if (type == DataType.DoubleType) return decodeDoubleFromBytes(v);
        if (type == DataType.DateType) return decodeDateFromBytes(v);
        if (type == DataType.TimestampType) return decodeTimestampFromBytes(v);
        if (type == DataType.BooleanType) return decodeBooleanFromBytes(v);
        if (type == DataType.BytesType) return decodeBytesFromBytes(v);
        if (type == DataType.FloatType) return decodeFloatFromBytes(v);
        if (type == DataType.ShortType) return decodeShortFromBytes(v);

        throw new RuntimeException("not supported DATA_TYPE:" + type.getDataClass());

    }

    protected Object decodeShortFromBytes(byte[] v) {
        return Bytes.toShort(v);
    }

    protected Object decodeFloatFromBytes(byte[] v) {
        return Bytes.toFloat(v);
    }

    protected Object decodeBytesFromBytes(byte[] v) {
        return v;
    }

    protected Object decodeBooleanFromBytes(byte[] v) {
        return v[0] == 0 ? false : true;
    }

    protected Object decodeTimestampFromBytes(byte[] v) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateStr = BytesUtils.getString(v, 0, v.length, getEncoding());
        try {
            return format.parse(dateStr);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    protected Object decodeDateFromBytes(byte[] v) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateStr = BytesUtils.getString(v, 0, v.length, getEncoding());
        try {
            return format.parse(dateStr);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    protected Object decodeDoubleFromBytes(byte[] v) {
        return Bytes.toDouble(v);
    }

    protected Object decodeStringFromBytes(byte[] v) {
        return BytesUtils.getString(v, 0, v.length, getEncoding());
    }

    protected Object decodeIntFromBytes(byte[] v) {
        return Bytes.toInt(v);
    }

    protected Object decodeLongFromBytes(byte[] v) {
        return Bytes.toLong(v);
    }

    @Override
    public Object decodeFromString(DataType type, String v) {

        if (type == DataType.LongType) return decodeLongFromString(v);
        if (type == DataType.IntegerType) return decodeIntFromString(v);
        if (type == DataType.StringType) return decodeStringFromString(v);
        if (type == DataType.DoubleType) return decodeDoubleFromString(v);
        if (type == DataType.DateType) return decodeDateFromString(v);
        if (type == DataType.TimestampType) return decodeTimestampFromString(v);
        if (type == DataType.BooleanType) return decodeBooleanFromString(v);
        if (type == DataType.BytesType) return decodeBytesFromString(v);
        if (type == DataType.FloatType) return decodeFloatFromString(v);
        if (type == DataType.ShortType) return decodeShortFromString(v);

        throw new RuntimeException("not supported DATA_TYPE:" + type.getDataClass());

    }

    protected Object decodeShortFromString(String v) {
        return Short.valueOf(v);
    }

    protected Object decodeFloatFromString(String v) {
        return Float.valueOf(v);
    }

    protected Object decodeBytesFromString(String v) {
        throw new UnsupportedOperationException("decodeBytesFromString is not supported");
    }

    protected Object decodeBooleanFromString(String v) {
        return Boolean.valueOf(v);
    }

    protected Object decodeTimestampFromString(String v) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            return format.parse(v);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    protected Object decodeDateFromString(String v) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        try {
            return format.parse(v);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    protected Object decodeDoubleFromString(String v) {
        return Double.valueOf(v);
    }

    protected Object decodeStringFromString(String v) {
        return v;
    }

    protected Object decodeIntFromString(String v) {
        return Integer.valueOf(v);
    }

    protected Object decodeLongFromString(String v) {
        return Long.valueOf(v);
    }

    @Override
    public String encodeToString(DataType type, Object v) {

        if (type == DataType.LongType) return encodeLongToString(v);
        if (type == DataType.IntegerType) return encodeIntToString(v);
        if (type == DataType.StringType) return encodeStringToString(v);
        if (type == DataType.DoubleType) return encodeDoubleToString(v);
        if (type == DataType.DateType) return encodeDateToString(v);
        if (type == DataType.TimestampType) return encodeTimestampToString(v);
        if (type == DataType.BooleanType) return encodeBooleanToString(v);
        if (type == DataType.BytesType) return encodeBytesToString(v);
        if (type == DataType.FloatType) return encodeFloatToString(v);
        if (type == DataType.ShortType) return encodeShortToString(v);

        throw new RuntimeException("not supported DATA_TYPE:" + type.getClass());
    }

    protected String encodeShortToString(Object v) {
        if (v == null) {
            StringBuilder sb = new StringBuilder();
            sb.append('\0');
            return sb.toString();
        }
        return String.valueOf(v);
    }

    protected String encodeFloatToString(Object v) {
        if (v == null) {
            StringBuilder sb = new StringBuilder();
            sb.append('\0');
            return sb.toString();
        }
        return String.valueOf(v);
    }

    protected String encodeBytesToString(Object v) {
        if (v == null) {
            StringBuilder sb = new StringBuilder();
            sb.append('\0');
            return sb.toString();
        }
        return String.valueOf(v);
    }

    protected String encodeBooleanToString(Object v) {
        if (v == null) {
            StringBuilder sb = new StringBuilder();
            sb.append('\0');
            return sb.toString();
        }
        return String.valueOf(v);
    }

    protected String encodeTimestampToString(Object v) {
        if (v == null) {
            StringBuilder sb = new StringBuilder();
            sb.append('\0');
            return sb.toString();
        }
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String s = format.format((Date) v);
        return s;
    }

    protected String encodeDateToString(Object v) {
        if (v == null) {
            StringBuilder sb = new StringBuilder();
            sb.append('\0');
            return sb.toString();
        }
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        String s = format.format((Date) v);
        return s;
    }

    protected String encodeDoubleToString(Object v) {
        if (v == null) {
            StringBuilder sb = new StringBuilder();
            sb.append('\0');
            return sb.toString();
        }
        return String.valueOf(v);
    }

    protected String encodeStringToString(Object v) {
        if (v == null) {
            StringBuilder sb = new StringBuilder();
            sb.append('\0');
            return sb.toString();
        }
        return String.valueOf(v);
    }

    protected String encodeIntToString(Object v) {
        if (v == null) {
            StringBuilder sb = new StringBuilder();
            sb.append('\0');
            return sb.toString();
        }
        return String.valueOf(v);
    }

    protected String encodeLongToString(Object v) {
        if (v == null) {
            StringBuilder sb = new StringBuilder();
            sb.append('\0');
            return sb.toString();
        }
        return String.valueOf(v);
    }

}
