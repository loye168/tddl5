/*
 * Copyright 2013 Alibaba.com All right reserved. This software is the
 * confidential and proprietary information of Alibaba.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Alibaba.com.
 */
package com.alibaba.icbu.da.tddl.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * ICBU DA tddl路由辅助方法代码。 注意： 请勿随意修改该类。这个类中的代码必须要与tddl import工具中的同名类代码保持一致。 如果需要做修改的话，两边的代码要同时修改。
 * 否则tddl数据导入和读取的路由规则很可能会出现不一致。 tddl import工具svn地址：
 * http://svn.alibaba-inc.com/repos/warehouse/etl/cbu_hadoop/trunk/01_common/ImportTddl/dhwImportTddl_new
 * 
 * @author terry.chenzq 2013-6-5 上午09:54:02
 */
public class B2bIcbuDaPartitionMethod {

    private static final int DEFAULT_STR_HEAD_LEN = 8;

    public static int partitionStr(String val, int partitionCnt) {
        return partitionStrPartHash(val, val.length(), partitionCnt);
    }

    // 这个方法是用来让tddl枚举所有的可能分区结果，然后准备数据库链接的
    public static int partitionStr(Integer val, int partitionCnt) {
        return Math.abs(val.hashCode()) % partitionCnt;
    }

    public static int partitionStrPartHash(String val, int partitionCnt) {
        return partitionStrPartHash(val, DEFAULT_STR_HEAD_LEN, partitionCnt);
    }

    // 这个方法是用来让tddl枚举所有的可能分区结果，然后准备数据库链接的
    public static int partitionStrPartHash(Integer val, int partitionCnt) {
        return Math.abs(val.hashCode()) % partitionCnt;
    }

    public static int partitionStrPartHash(String val, int len, int partitionCnt) {
        long h = 0;
        len = Math.min(len, val.length());
        for (int i = 0; i < len; ++i) {
            h = (h << 5) - h + val.charAt(i);
        }
        return (int) (Math.abs(h) % partitionCnt);
    }

    // 这个方法是用来让tddl枚举所有的可能分区结果，然后准备数据库链接的
    public static int partitionStrPartHash(Integer val, int len, int partitionCnt) {
        return Math.abs(val.hashCode()) % partitionCnt;
    }

    public static int partitionNumber(long val, int partitionCnt) {
        return (int) (Math.abs(val) % partitionCnt);
    }

    public static int partitionNumber(String val, int partitionCnt) {
        return partitionNumber(Long.valueOf(val), partitionCnt);
    }

    private static long ONE_HOUR = 60 * 60 * 1000;
    private static long ONE_DAY  = 24 * ONE_HOUR;
    private static long ONE_WEEK = 7 * ONE_DAY;

    /**
     * 按天进行分区。 分区计算逻辑为 : (val与1970-01-01之间的天数差) % partitionCnt <br/>
     * <b>注意：val所对应的日期，在当前系统的默认时区下，解析出的时、分、秒都应该是0。 val如果是从字符串格式解析来的，那么解析所用的时区必须与执行该方 法的系统所设置的默认时区相同，否则会有误差。</b>
     */
    public static int partitionDate(Date val, int partitionCnt) throws ParseException {
        Date start = new SimpleDateFormat("yyyy-MM-dd").parse("1970-01-01");
        // 通过加12小时来抗夏令时（西7）-冬令（西8）时造成的1个小时误差
        return (int) ((val.getTime() + 12 * ONE_HOUR - start.getTime()) / ONE_DAY) % partitionCnt;
    }

    /**
     * 按天进行分区。 分区计算逻辑为 : (val与1970-01-01之间的天数差) % partitionCnt <br/>
     * <b>注意：val所对应的日期格式必须为 "yyyy-MM-dd"
     */
    public static int partitionDate(String val, int partitionCnt) throws ParseException {
        return partitionDate(new SimpleDateFormat("yyyy-MM-dd").parse(val), partitionCnt);
    }

    /**
     * 按周进行分区。 分区计算逻辑为 : (val与1970-01-01之间的周数差) % partitionCnt <br/>
     * <b>注意：val所对应的日期，在当前系统的默认时区下，解析出的时、分、秒都应该是0。 val如果是从字符串格式解析来的，那么解析所用的时区必须与执行该方 法的系统所设置的默认时区相同，否则会有误差。</b>
     */
    public static int partitionWeek(Date val, int partitionCnt) throws ParseException {
        Date start = new SimpleDateFormat("yyyy-MM-dd").parse("1970-01-01");
        // 通过加12小时来抗夏令时（西7）-冬令（西8）时造成的1个小时误差
        return (int) ((val.getTime() + 12 * ONE_HOUR - start.getTime()) / ONE_WEEK) % partitionCnt;
    }

    /**
     * 按周进行分区。 分区计算逻辑为 : (val与1970-01-01之间的周数差) % partitionCnt <br/>
     * <b>注意：val所对应的日期格式必须为 "yyyy-MM-dd"
     */
    public static int partitionWeek(String val, int partitionCnt) throws ParseException {
        return partitionWeek(new SimpleDateFormat("yyyy-MM-dd").parse(val), partitionCnt);
    }

    /**
     * 按月进行分区。 分区计算逻辑为 : (val与1970-01-01之间的月份数差) % partitionCnt <br/>
     * <b>注意：val所对应的日期，在当前系统的默认时区下，解析出的时、分、秒都应该是0。 val如果是从字符串格式解析来的，那么解析所用的时区必须与执行该方 法的系统所设置的默认时区相同，否则会有误差。</b>
     */
    public static int partitionMonth(Date val, int partitionCnt) throws ParseException {
        Calendar startCal = Calendar.getInstance();
        startCal.setTime(new SimpleDateFormat("yyyy-MM-dd").parse("1970-01-01"));
        Calendar endCal = Calendar.getInstance();
        endCal.setTime(val);
        int yearDelta = endCal.get(Calendar.YEAR) - startCal.get(Calendar.YEAR);
        int monthDelta = endCal.get(Calendar.MONTH) - startCal.get(Calendar.MONTH);
        monthDelta += yearDelta * 12;
        return monthDelta % partitionCnt;
    }

    /**
     * 按月进行分区。 分区计算逻辑为 : (val与1970-01-01之间的月份数差) % partitionCnt <br/>
     * <b>注意：val所对应的日期格式必须为 "yyyy-MM-dd"
     */
    public static int partitionMonth(String val, int partitionCnt) throws ParseException {
        return partitionMonth(new SimpleDateFormat("yyyy-MM-dd").parse(val), partitionCnt);
    }

    private static void assertEqual(int accept, int expect) {
        if (accept != expect) {
            throw new RuntimeException("Assert failed: accept=" + accept + ", expect=" + expect);
        }
    }

    public static void main(String[] args) throws ParseException {
        assertEqual(partitionStr("123456789", 8), 5);
        assertEqual(partitionStrPartHash("123456789", 8), 4);
        assertEqual(partitionStrPartHash("123456789", 7, 8), 4);
        assertEqual(partitionStrPartHash("123456789", 10, 8), 5);
        assertEqual(partitionStrPartHash("123456789", 0, 8), 0);
        assertEqual(partitionStrPartHash("123456789", -1, 8), 0);
        assertEqual(partitionNumber(Integer.valueOf(10), 8), 2);
        assertEqual(partitionNumber(Long.valueOf(10), 8), 2);
        assertEqual(partitionNumber(Short.valueOf((short) 10), 8), 2);
        assertEqual(partitionNumber(Byte.valueOf((byte) 10), 8), 2);
        assertEqual(partitionNumber("10", 8), 2);
        assertEqual(partitionDate("2013-06-05", 8), 5);
        assertEqual(partitionDate(new SimpleDateFormat("yyyy-MM-dd").parse("2013-06-05"), 8), 5);
    }
}
