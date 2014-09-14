package com.taobao.tddl.optimizer.core.datatype;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.expression.bean.NullValue;
import com.taobao.tddl.optimizer.exception.OptimizerException;

public class DataTypeUtil {

    public static DataType getTypeOfObject(Object v) {
        if (v == null || v instanceof NullValue) {
            return DataType.NullType;
        }
        Class clazz = v.getClass();
        if (clazz == Integer.class || clazz == int.class) {
            return DataType.IntegerType;
        }
        if (clazz == Long.class || clazz == long.class) {
            return DataType.LongType;
        }
        if (clazz == Short.class || clazz == short.class) {
            return DataType.ShortType;
        }
        if (clazz == Float.class || clazz == float.class) {
            return DataType.FloatType;
        }
        if (clazz == Double.class || clazz == double.class) {
            return DataType.DoubleType;
        }
        if (clazz == Byte.class || clazz == byte.class) {
            return DataType.ByteType;
        }
        if (clazz == Boolean.class || clazz == boolean.class) {
            return DataType.BooleanType;
        }
        if (clazz == BigInteger.class) {
            return DataType.BigIntegerType;
        }
        if (clazz == BigDecimal.class) {
            return DataType.BigDecimalType;
        }

        if (clazz == Timestamp.class || clazz == java.util.Date.class || Calendar.class.isAssignableFrom(clazz)) {
            return DataType.TimestampType;
        }
        if (clazz == java.sql.Date.class) {
            return DataType.DateType;
        }
        if (clazz == Time.class) {
            return DataType.TimeType;
        }

        if (clazz == Byte[].class || clazz == byte[].class || Blob.class.isAssignableFrom(clazz)) {
            return DataType.BytesType;
        }

        if (clazz == String.class || Clob.class.isAssignableFrom(clazz)) {
            return DataType.StringType;
        }

        if (v instanceof ISelectable) {
            return ((ISelectable) v).getDataType();
        }

        throw new OptimizerException("type: " + v.getClass().getSimpleName() + " is not supported");
    }

    /**
     * 判断是否为时间类型
     */
    public static boolean isDateType(Object value) {
        return isDateType(getTypeOfObject(value));
    }

    /**
     * 判断是否为时间类型
     */
    public static boolean isDateType(DataType type) {
        return type != null
               && (type == DataType.DateType || type == DataType.TimeType || type == DataType.TimestampType
                   || type == DataType.DatetimeType || type == DataType.YearType);
    }

}
