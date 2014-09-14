package com.taobao.tddl.executor.function.scalar.datatime;

import java.util.Calendar;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * With a single argument, this function returns the date or datetime expression
 * expr as a datetime value. With two arguments, it adds the time expression
 * expr2 to the date or datetime expression expr1 and returns the result as a
 * datetime value.
 * 
 * <pre>
 * mysql> SELECT TIMESTAMP('2003-12-31');
 *         -> '2003-12-31 00:00:00'
 * mysql> SELECT TIMESTAMP('2003-12-31 12:00:00','12:00:00');
 *         -> '2004-01-01 00:00:00'
 * </pre>
 * 
 * @author jianghang 2014-4-16 下午11:16:06
 * @since 5.0.7
 */
public class Timestamp extends ScalarFunction {

    @SuppressWarnings("deprecation")
    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (ExecUtils.isNull(arg)) {
                return null;
            }
        }

        java.sql.Timestamp timestamp = DataType.TimestampType.convertFrom(args[0]);
        if (args.length >= 2) {
            Calendar cal = Calendar.getInstance();
            cal.setTime(timestamp);

            java.sql.Time time = DataType.TimeType.convertFrom(args[1]);

            cal.add(Calendar.HOUR_OF_DAY, time.getHours());
            cal.add(Calendar.MINUTE, time.getMinutes());
            cal.add(Calendar.SECOND, time.getSeconds());
            DataType type = getReturnType();
            return type.convertFrom(cal.getTime());
        } else {
            return timestamp;
        }
    }

    @Override
    public DataType getReturnType() {
        return DataType.TimestampType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "TIMESTAMP" };
    }
}
