package com.taobao.tddl.optimizer.core.datatype;

import java.util.Calendar;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.taobao.tddl.common.utils.TddlToStringStyle;

/**
 * mysql interval时间类型
 * 
 * @author jianghang 2014-4-16 下午1:34:00
 * @since 5.0.7
 */
public class IntervalType extends TimestampType {

    private int     year             = 0;
    private boolean isYearSet        = false;
    private int     month            = 0;
    private boolean isMonthSet       = false;
    private int     day              = 0;
    private boolean isDaySet         = false;
    private int     hour             = 0;
    private boolean isHourSet        = false;
    private int     minute           = 0;
    private boolean isMinuteSet      = false;
    private int     second           = 0;
    private boolean isSecondSet      = false;
    private int     microsecond      = 0;
    private boolean isMicrosecondSet = false;
    private int     factor           = 1;

    public void process(Calendar cal, int factor) {
        factor = this.factor * factor;
        if (isYearSet) {
            cal.add(Calendar.YEAR, year * factor);
        }
        if (isMonthSet) {
            cal.add(Calendar.MONTH, month * factor);
        }
        if (isDaySet) {
            cal.add(Calendar.DAY_OF_YEAR, day * factor);
        }
        if (isHourSet) {
            cal.add(Calendar.HOUR_OF_DAY, hour * factor);
        }
        if (isMinuteSet) {
            cal.add(Calendar.MINUTE, minute * factor);
        }
        if (isSecondSet) {
            cal.add(Calendar.SECOND, second * factor);
        }
        // 微秒的支持会有问题，目前只能到毫秒单位
        if (isMicrosecondSet) {
            cal.add(Calendar.MILLISECOND, (microsecond / 1000) * factor);
        }
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
        this.isYearSet = true;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
        this.isMonthSet = true;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
        this.isDaySet = true;
    }

    public int getHour() {
        return hour;
    }

    public void setHour(int hour) {
        this.hour = hour;
        this.isHourSet = true;
    }

    public int getMinute() {
        return minute;
    }

    public void setMinute(int minute) {
        this.minute = minute;
        this.isMinuteSet = true;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
        this.isSecondSet = true;
    }

    public int getMicrosecond() {
        return microsecond;
    }

    public void setMicrosecond(int microsecond) {
        this.microsecond = microsecond;
        this.isMicrosecondSet = true;
    }

    public void setFactor(int factor) {
        this.factor = factor;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }

}
