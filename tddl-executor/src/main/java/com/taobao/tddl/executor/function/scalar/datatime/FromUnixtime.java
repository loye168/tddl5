package com.taobao.tddl.executor.function.scalar.datatime;

import java.text.SimpleDateFormat;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Returns a representation of the unix_timestamp argument as a value in
 * 'YYYY-MM-DD HH:MM:SS' or YYYYMMDDHHMMSS format, depending on whether the
 * function is used in a string or numeric context. The value is expressed in
 * the current time zone. unix_timestamp is an internal timestamp value such as
 * is produced by the UNIX_TIMESTAMP() function. If format is given, the result
 * is formatted according to the format string, which is used the same way as
 * listed in the entry for the DATE_FORMAT() function.
 * 
 * <pre>
 * mysql> SELECT FROM_UNIXTIME(1196440219);
 *         -> '2007-11-30 10:30:19'
 * mysql> SELECT FROM_UNIXTIME(1196440219) + 0;
 *         -> 20071130103019.000000
 * mysql> SELECT FROM_UNIXTIME(UNIX_TIMESTAMP(),
 *     ->                      '%Y %D %M %h:%i:%s %x');
 *         -> '2007 30th November 10:30:59 2007'
 * </pre>
 * 
 * Note: If you use UNIX_TIMESTAMP() and FROM_UNIXTIME() to convert between
 * TIMESTAMP values and Unix timestamp values, the conversion is lossy because
 * the mapping is not one-to-one in both directions. For details, see the
 * description of the UNIX_TIMESTAMP() function.
 * 
 * @author jianghang 2014-4-17 上午12:24:11
 * @since 5.0.7
 */
public class FromUnixtime extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (ExecUtils.isNull(arg)) {
                return null;
            }
        }

        Long time = DataType.LongType.convertFrom(args[0]);
        java.sql.Timestamp timestamp = DataType.TimestampType.convertFrom(time * 1000);

        DataType type = getReturnType();
        if (args.length >= 2) {
            String format = DataType.StringType.convertFrom(args[1]);
            SimpleDateFormat dateFormat = new SimpleDateFormat(DateFormat.convertToJavaDataFormat(format));
            return dateFormat.format(timestamp);
        } else {
            return type.convertFrom(timestamp);
        }
    }

    @Override
    public DataType getReturnType() {
        return DataType.StringType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "FROM_UNIXTIME" };
    }
}
