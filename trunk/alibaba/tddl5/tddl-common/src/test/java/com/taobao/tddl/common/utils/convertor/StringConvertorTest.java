package com.taobao.tddl.common.utils.convertor;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Calendar;
import java.util.Date;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.common.utils.convertor.Convertor;
import com.taobao.tddl.common.utils.convertor.ConvertorHelper;

/**
 * convertor相关的单元测试
 * 
 * @author jianghang 2011-5-26 上午11:17:36
 */
public class StringConvertorTest {

    private ConvertorHelper helper = new ConvertorHelper();

    @Test
    public void testStringToCommon() {
        String strValue = "10";
        int value = 10;
        // 基本变量
        Object intValue = helper.getConvertor(String.class, int.class).convert(strValue, int.class);
        Assert.assertEquals(intValue, value);

        Object integerValue = helper.getConvertor(String.class, Integer.class).convert(strValue, Integer.class);
        Assert.assertEquals(integerValue, value);

        Object byteValue = helper.getConvertor(String.class, byte.class).convert(strValue, byte.class);
        Assert.assertEquals(byteValue, Byte.valueOf((byte) value));

        Object shortValue = helper.getConvertor(String.class, short.class).convert(strValue, short.class);
        Assert.assertEquals(shortValue, Short.valueOf((short) value));

        Object longValue = helper.getConvertor(String.class, long.class).convert(strValue, long.class);
        Assert.assertEquals(longValue, Long.valueOf((long) value));

        Object floatValue = helper.getConvertor(String.class, float.class).convert(strValue, float.class);
        Assert.assertEquals(floatValue, Float.valueOf((float) value));

        Object doubleValue = helper.getConvertor(String.class, double.class).convert(strValue, double.class);
        Assert.assertEquals(doubleValue, Double.valueOf((double) value));

        Object bigIntegerValue = helper.getConvertor(String.class, BigInteger.class)
            .convert(strValue, BigInteger.class);
        Assert.assertEquals(bigIntegerValue, BigInteger.valueOf(value));

        Object bigDecimalValue = helper.getConvertor(String.class, BigDecimal.class)
            .convert(strValue, BigDecimal.class);
        Assert.assertEquals(bigDecimalValue, BigDecimal.valueOf(value));

        Object boolValue = helper.getConvertor(String.class, boolean.class).convert(strValue, boolean.class);
        Assert.assertEquals(boolValue, Boolean.valueOf(value > 0 ? true : false));

        Object charValue = helper.getConvertor(String.class, char.class).convert(strValue, char.class);
        Assert.assertEquals(charValue, Character.valueOf((char) value));
    }

    @Test
    public void testStringAndDateDefault() {
        Convertor stringDate = helper.getConvertor(String.class, Date.class);
        Convertor dateString = helper.getConvertor(Date.class, String.class);

        Convertor stringCalendar = helper.getConvertor(String.class, Calendar.class);
        Convertor calendarString = helper.getConvertor(Calendar.class, String.class);

        String time = "2010-10-01 23:59:59";
        Calendar c1 = Calendar.getInstance();
        c1.set(2010, 10 - 1, 01, 23, 59, 59);
        c1.set(Calendar.MILLISECOND, 0);
        Date timeDate = c1.getTime();

        // 验证默认的转化器
        Object stringDateValue = stringDate.convert(time, Date.class);
        Assert.assertTrue(timeDate.equals(stringDateValue));
        Object dateStringValue = dateString.convert(timeDate, String.class);
        Assert.assertTrue(time.equals(dateStringValue));

        Object stringCalendarValue = stringCalendar.convert(time, Calendar.class);
        Assert.assertTrue(c1.equals(stringCalendarValue));
        Object calendarStringValue = calendarString.convert(c1, String.class);
        Assert.assertTrue(time.equals(calendarStringValue));
    }

}
