package com.taobao.tddl.executor.function.scalar.datatime;

import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.exception.FunctionException;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.datatype.IntervalType;

/**
 * mysql interval函数
 * 
 * <pre>
 * The INTERVAL keyword and the unit specifier are not case sensitive.
 * 
 * The following table shows the expected form of the expr argument for each unit value.
 * 
 * unit Value  Expected expr Format
 * MICROSECOND MICROSECONDS
 * SECOND  SECONDS
 * MINUTE  MINUTES
 * HOUR    HOURS
 * DAY DAYS
 * WEEK    WEEKS
 * MONTH   MONTHS
 * QUARTER QUARTERS
 * YEAR    YEARS
 * SECOND_MICROSECOND  'SECONDS.MICROSECONDS'
 * MINUTE_MICROSECOND  'MINUTES:SECONDS.MICROSECONDS'
 * MINUTE_SECOND   'MINUTES:SECONDS'
 * HOUR_MICROSECOND    'HOURS:MINUTES:SECONDS.MICROSECONDS'
 * HOUR_SECOND 'HOURS:MINUTES:SECONDS'
 * HOUR_MINUTE 'HOURS:MINUTES'
 * DAY_MICROSECOND 'DAYS HOURS:MINUTES:SECONDS.MICROSECONDS'
 * DAY_SECOND  'DAYS HOURS:MINUTES:SECONDS'
 * DAY_MINUTE  'DAYS HOURS:MINUTES'
 * DAY_HOUR    'DAYS HOURS'
 * YEAR_MONTH  'YEARS-MONTHS'
 * </pre>
 * 
 * @author jianghang 2014-4-16 下午1:34:00
 * @since 5.0.7
 */
public class Interval extends ScalarFunction {

    public enum Interval_Unit {

        MICROSECOND(null, "S"), SECOND(null, "s"), MINUTE(null, "m"), HOUR(null, "H"), DAY(null, "d"), WEEK(null, "w"),
        MONTH(null, "M"), QUARTER(null, null), YEAR(null, "yyyy"),
        /** */
        SECOND_MICROSECOND("^\\s*(\\d+)?\\.?(\\d+)?\\s*$", "sSSSSSS"),
        /** */
        MINUTE_MICROSECOND("^\\s*(\\d+)?\\:?(\\d+)?\\.?(\\d+)?\\s*$", "mssSSSSSS"),
        /** */
        MINUTE_SECOND("^\\s*(\\d+)?\\:?(\\d+)?\\s*$", "mss"),
        /** */
        HOUR_MICROSECOND("^\\s*(\\d+)?\\:?(\\d+)?\\:?(\\d+)?\\.?(\\d+)?\\s*$", "HmmssSSSSSS"),
        /** */
        HOUR_SECOND("^^\\s*(\\d+)?\\:?(\\d+)?\\:?(\\d+)?\\s*$", "Hmmss"),
        /** */
        HOUR_MINUTE("^\\s*(\\d+)\\:(\\d+)\\s*$", "Hmm"),
        /** */
        DAY_MICROSECOND("^\\s*(\\d+)\\s+(\\d+)\\:(\\d+)\\:(\\d+)\\.(\\d+)\\s*$", "dHHmmssSSSSSS"),
        /** */
        DAY_SECOND("^\\s*(\\d+)\\s+(\\d+)\\:(\\d+)\\:(\\d+)\\s*$", "dHHmmss"),
        /** */
        DAY_MINUTE("^\\s*(\\d+)\\s+(\\d+)\\:(\\d+)\\s*$", "dHHmm"),
        /** */
        DAY_HOUR("^\\s*(\\d+)\\s+(\\d+)\\s*$", "dHH"),
        /** */
        YEAR_MONTH("^\\s*(\\d+)-(\\d+)\\s*$", "yyyyMM");

        Interval_Unit(){
            this(null, null);
        }

        Interval_Unit(String pattern, String format){
            this.pattern = pattern;
            this.format = format;
        }

        String pattern;
        String format;
    }

    private static LoadingCache<String, Pattern> patterns = CacheBuilder.newBuilder()
                                                              .build(new CacheLoader<String, Pattern>() {

                                                                  @Override
                                                                  public Pattern load(String regex) throws Exception {
                                                                      return Pattern.compile(regex);
                                                                  }
                                                              });

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (ExecUtils.isNull(arg)) {
                return null;
            }
        }

        return paseIntervalDate(args[0], args[1]);
    }

    protected static IntervalType paseIntervalDate(Object value, Object unitObj) {
        try {
            String unit = DataType.StringType.convertFrom(unitObj);
            IntervalType interval = new IntervalType();
            if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.MICROSECOND.name())) {
                interval.setMicrosecond(DataType.IntegerType.convertFrom(value));
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.SECOND.name())) {
                interval.setSecond(DataType.IntegerType.convertFrom(value));
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.MINUTE.name())) {
                interval.setMinute(DataType.IntegerType.convertFrom(value));
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.HOUR.name())) {
                interval.setHour(DataType.IntegerType.convertFrom(value));
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.DAY.name())) {
                interval.setDay(DataType.IntegerType.convertFrom(value));
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.WEEK.name())) {
                interval.setDay(DataType.IntegerType.convertFrom(value) * 7);// 7天
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.MONTH.name())) {
                interval.setMonth(DataType.IntegerType.convertFrom(value));
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.QUARTER.name())) {
                interval.setMonth(DataType.IntegerType.convertFrom(value) * 3);// 3个月
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.YEAR.name())) {
                interval.setYear(DataType.IntegerType.convertFrom(value));
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.SECOND_MICROSECOND.name())) {
                String str = DataType.StringType.convertFrom(value);
                Matcher match = patterns.get(Interval_Unit.SECOND_MICROSECOND.pattern).matcher(str);
                if (!match.matches()) {
                    throw new FunctionException("interval parser error");
                }

                String sec = match.group(1);
                if (sec != null) {
                    interval.setSecond(DataType.IntegerType.convertFrom(sec));
                }
                String mic = match.group(2);
                if (mic != null) {
                    interval.setMicrosecond(DataType.IntegerType.convertFrom(mic));
                }
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.MINUTE_MICROSECOND.name())) {
                String str = DataType.StringType.convertFrom(value);
                Matcher match = patterns.get(Interval_Unit.MINUTE_MICROSECOND.pattern).matcher(str);
                if (!match.matches()) {
                    throw new FunctionException("interval parser error");
                }

                String min = match.group(1);
                if (min != null) {
                    interval.setMinute(DataType.IntegerType.convertFrom(min));
                }
                String sec = match.group(2);
                if (sec != null) {
                    interval.setSecond(DataType.IntegerType.convertFrom(sec));
                }
                String mic = match.group(3);
                if (mic != null) {
                    interval.setMicrosecond(DataType.IntegerType.convertFrom(mic));
                }
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.MINUTE_SECOND.name())) {
                String str = DataType.StringType.convertFrom(value);
                Matcher match = patterns.get(Interval_Unit.MINUTE_SECOND.pattern).matcher(str);
                if (!match.matches()) {
                    throw new FunctionException("interval parser error");
                }

                String min = match.group(1);
                if (min != null) {
                    interval.setMinute(DataType.IntegerType.convertFrom(min));
                }
                String sec = match.group(2);
                if (sec != null) {
                    interval.setSecond(DataType.IntegerType.convertFrom(sec));
                }
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.HOUR_MICROSECOND.name())) {
                String str = DataType.StringType.convertFrom(value);
                Matcher match = patterns.get(Interval_Unit.HOUR_MICROSECOND.pattern).matcher(str);
                if (!match.matches()) {
                    throw new FunctionException("interval parser error");
                }

                String hour = match.group(1);
                if (hour != null) {
                    interval.setHour(DataType.IntegerType.convertFrom(hour));
                }
                String min = match.group(2);
                if (min != null) {
                    interval.setMinute(DataType.IntegerType.convertFrom(min));
                }
                String sec = match.group(3);
                if (sec != null) {
                    interval.setSecond(DataType.IntegerType.convertFrom(sec));
                }
                String mic = match.group(4);
                if (mic != null) {
                    interval.setMicrosecond(DataType.IntegerType.convertFrom(mic));
                }
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.HOUR_SECOND.name())) {
                String str = DataType.StringType.convertFrom(value);
                Matcher match = patterns.get(Interval_Unit.HOUR_SECOND.pattern).matcher(str);
                if (!match.matches()) {
                    throw new FunctionException("interval parser error");
                }

                String hour = match.group(1);
                if (hour != null) {
                    interval.setHour(DataType.IntegerType.convertFrom(hour));
                }
                String min = match.group(2);
                if (min != null) {
                    interval.setMinute(DataType.IntegerType.convertFrom(min));
                }
                String sec = match.group(3);
                if (sec != null) {
                    interval.setSecond(DataType.IntegerType.convertFrom(sec));
                }
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.HOUR_MINUTE.name())) {
                String str = DataType.StringType.convertFrom(value);
                Matcher match = patterns.get(Interval_Unit.HOUR_MINUTE.pattern).matcher(str);
                if (!match.matches()) {
                    throw new FunctionException("interval parser error");
                }

                String hour = match.group(1);
                if (hour != null) {
                    interval.setHour(DataType.IntegerType.convertFrom(hour));
                }
                String min = match.group(2);
                if (min != null) {
                    interval.setMinute(DataType.IntegerType.convertFrom(min));
                }
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.DAY_MICROSECOND.name())) {
                String str = DataType.StringType.convertFrom(value);
                Matcher match = patterns.get(Interval_Unit.DAY_MICROSECOND.pattern).matcher(str);
                if (!match.matches()) {
                    throw new FunctionException("interval parser error");
                }

                String day = match.group(1);
                if (day != null) {
                    interval.setDay(DataType.IntegerType.convertFrom(day));
                }
                String hour = match.group(2);
                if (hour != null) {
                    interval.setHour(DataType.IntegerType.convertFrom(hour));
                }
                String min = match.group(3);
                if (min != null) {
                    interval.setMinute(DataType.IntegerType.convertFrom(min));
                }
                String sec = match.group(4);
                if (sec != null) {
                    interval.setSecond(DataType.IntegerType.convertFrom(sec));
                }
                String mic = match.group(5);
                if (mic != null) {
                    interval.setMicrosecond(DataType.IntegerType.convertFrom(mic));
                }
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.DAY_SECOND.name())) {
                String str = DataType.StringType.convertFrom(value);
                Matcher match = patterns.get(Interval_Unit.DAY_SECOND.pattern).matcher(str);
                if (!match.matches()) {
                    throw new FunctionException("interval parser error");
                }

                String day = match.group(1);
                if (day != null) {
                    interval.setDay(DataType.IntegerType.convertFrom(day));
                }
                String hour = match.group(2);
                if (hour != null) {
                    interval.setHour(DataType.IntegerType.convertFrom(hour));
                }
                String min = match.group(3);
                if (min != null) {
                    interval.setMinute(DataType.IntegerType.convertFrom(min));
                }
                String sec = match.group(4);
                if (sec != null) {
                    interval.setSecond(DataType.IntegerType.convertFrom(sec));
                }
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.DAY_MINUTE.name())) {
                String str = DataType.StringType.convertFrom(value);
                Matcher match = patterns.get(Interval_Unit.DAY_MINUTE.pattern).matcher(str);
                if (!match.matches()) {
                    throw new FunctionException("interval parser error");
                }

                String day = match.group(1);
                if (day != null) {
                    interval.setDay(DataType.IntegerType.convertFrom(day));
                }
                String hour = match.group(2);
                if (hour != null) {
                    interval.setHour(DataType.IntegerType.convertFrom(hour));
                }
                String min = match.group(3);
                if (min != null) {
                    interval.setMinute(DataType.IntegerType.convertFrom(min));
                }
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.DAY_HOUR.name())) {
                String str = DataType.StringType.convertFrom(value);
                Matcher match = patterns.get(Interval_Unit.DAY_HOUR.pattern).matcher(str);
                if (!match.matches()) {
                    throw new FunctionException("interval parser error");
                }

                String day = match.group(1);
                if (day != null) {
                    interval.setDay(DataType.IntegerType.convertFrom(day));
                }
                String hour = match.group(2);
                if (hour != null) {
                    interval.setHour(DataType.IntegerType.convertFrom(hour));
                }
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.YEAR_MONTH.name())) {
                String str = DataType.StringType.convertFrom(value);
                Matcher match = patterns.get(Interval_Unit.YEAR_MONTH.pattern).matcher(str);
                if (!match.matches()) {
                    throw new FunctionException("interval parser error");
                }

                String year = match.group(1);
                if (year != null) {
                    interval.setYear(DataType.IntegerType.convertFrom(year));
                }
                String mon = match.group(2);
                if (mon != null) {
                    interval.setMonth(DataType.IntegerType.convertFrom(mon));
                }
            }

            return interval;
        } catch (ExecutionException e) {
            throw new FunctionException(e);
        }
    }

    @Override
    public DataType getReturnType() {
        // 直接先返回时间类型
        return DataType.IntervalType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "INTERVAL_PRIMARY" };
    }

    public static void main(String args[]) throws ExecutionException {
        printMatch(Interval_Unit.SECOND_MICROSECOND.pattern, "10.20");
        printMatch(Interval_Unit.SECOND_MICROSECOND.pattern, ".20");
        printMatch(Interval_Unit.SECOND_MICROSECOND.pattern, "1");
        printMatch(Interval_Unit.MINUTE_MICROSECOND.pattern, "59:10.20");
        printMatch(Interval_Unit.MINUTE_SECOND.pattern, "59:10");
        printMatch(Interval_Unit.HOUR_MICROSECOND.pattern, "11:59:10.20");
        printMatch(Interval_Unit.HOUR_SECOND.pattern, "11:59:10");
        printMatch(Interval_Unit.HOUR_MINUTE.pattern, "11:59");
        printMatch(Interval_Unit.DAY_MICROSECOND.pattern, "1 11:59:10.20");
        printMatch(Interval_Unit.DAY_SECOND.pattern, "1 11:59:10");
        printMatch(Interval_Unit.DAY_MINUTE.pattern, "1 11:59");
        printMatch(Interval_Unit.DAY_HOUR.pattern, "1 11");
        printMatch(Interval_Unit.YEAR_MONTH.pattern, "1-2");
    }

    private static void printMatch(String pattern, String text) throws ExecutionException {
        Pattern p = patterns.get(pattern);
        Matcher match = p.matcher(text);
        System.out.println("-----" + text);
        if (match.matches()) {
            for (int i = 1; i <= match.groupCount(); i++) {
                System.out.print(match.group(i) + " ");
            }
        }
        System.out.println("\n-----");
    }
}
