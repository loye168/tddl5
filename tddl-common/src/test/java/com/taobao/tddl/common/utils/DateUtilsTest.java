package com.taobao.tddl.common.utils;

import java.util.Date;

import org.junit.Test;

public class DateUtilsTest {

    @Test
    public void test_day() {
        String time = "2014-03-26";
        Date date = DateUtils.str_to_time(time);
        System.out.println(date);
    }

    @Test
    public void test_date() {
        String time = "2014-03-26 12:12:12";
        Date date = DateUtils.str_to_time(time);
        System.out.println(date);
    }

    @Test
    public void test_timestamp() {
        String time = "2014-03-26 12:12:12.339";
        Date date = DateUtils.str_to_time(time);
        System.out.println(date);
    }

    @Test
    public void test_time() {
        String time = "200801";
        Date date = DateUtils.str_to_time(time);
        System.out.println(date);
    }
}
