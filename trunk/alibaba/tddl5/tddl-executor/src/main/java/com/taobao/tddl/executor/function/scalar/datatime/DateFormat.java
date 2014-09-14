package com.taobao.tddl.executor.function.scalar.datatime;

import java.text.SimpleDateFormat;
import java.util.Locale;

import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.exception.FunctionException;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * http://dev.mysql.com/doc/refman/5.6/en/date-and-time-functions.html#
 * function_date-format
 * 
 * @author jianghang 2014-4-17 上午12:19:18
 * @since 5.0.7
 */
public class DateFormat extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (ExecUtils.isNull(arg)) {
                return null;
            }
        }

        java.sql.Timestamp timestamp = DataType.TimestampType.convertFrom(args[0]);
        String format = DataType.StringType.convertFrom(args[1]);
        SimpleDateFormat dateFormat = new SimpleDateFormat(convertToJavaDataFormat(format), Locale.ENGLISH);
        return dateFormat.format(timestamp);
    }

    @Override
    public DataType getReturnType() {
        return DataType.StringType;
    }

    protected static String convertToJavaDataFormat(String format) {
        format = StringUtils.replace(format, "%a", "EEE");
        format = StringUtils.replace(format, "%b", "MMM");
        format = StringUtils.replace(format, "%c", "M");
        format = StringUtils.replace(format, "%d", "dd");
        format = StringUtils.replace(format, "%e", "d");
        format = StringUtils.replace(format, "%f", "SSSSSS");
        format = StringUtils.replace(format, "%H", "HH");
        format = StringUtils.replace(format, "%h", "hh");
        format = StringUtils.replace(format, "%I", "hh");
        format = StringUtils.replace(format, "%i", "mm");
        format = StringUtils.replace(format, "%j", "DDD");
        format = StringUtils.replace(format, "%k", "H");
        format = StringUtils.replace(format, "%l", "h");
        format = StringUtils.replace(format, "%M", "MMMM");
        format = StringUtils.replace(format, "%m", "MM");
        format = StringUtils.replace(format, "%p", "a");
        format = StringUtils.replace(format, "%r", "hh:mm:ss a");
        format = StringUtils.replace(format, "%S", "ss");
        format = StringUtils.replace(format, "%s", "ss");
        format = StringUtils.replace(format, "%T", "HH:mm:ss");
        format = StringUtils.replace(format, "%W", "EEEE");
        format = StringUtils.replace(format, "%Y", "yyyy");
        format = StringUtils.replace(format, "%y", "yy");
        format = StringUtils.replace(format, "%v", "ww");

        if (StringUtils.contains(format, "%D")) {
            // Day of the month with English suffix (0th, 1st, 2nd, 3rd, …)
            throw new FunctionException("java不支持的format格式:%D");
        }
        if (StringUtils.contains(format, "%w")) {
            // Day of the month with English suffix (0th, 1st, 2nd, 3rd, …)
            throw new FunctionException("java不支持的format格式:%w");
        }
        if (StringUtils.contains(format, "%U")) {
            throw new FunctionException("java不支持的format格式:%U");
        }
        if (StringUtils.contains(format, "%u")) {
            throw new FunctionException("java不支持的format格式:%u");
        }
        if (StringUtils.contains(format, "%V")) {
            throw new FunctionException("java不支持的format格式:%V");
        }
        if (StringUtils.contains(format, "%X")) {
            throw new FunctionException("java不支持的format格式:%X");
        }
        if (StringUtils.contains(format, "%x")) {
            throw new FunctionException("java不支持的format格式:%x");
        }
        if (StringUtils.contains(format, "%%")) {
            throw new FunctionException("java不支持的format格式:%%");
        }
        return format;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "DATE_FORMAT" };
    }
}
