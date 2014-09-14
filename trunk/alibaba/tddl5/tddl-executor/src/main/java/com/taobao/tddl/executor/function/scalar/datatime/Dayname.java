package com.taobao.tddl.executor.function.scalar.datatime;

import java.util.Calendar;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.exception.FunctionException;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Returns the name of the weekday for date. As of MySQL 5.1.12, the language
 * used for the name is controlled by the value of the lc_time_names system
 * variable (Section 10.7, “MySQL Server Locale Support”).
 * 
 * <pre>
 * mysql> SELECT DAYNAME('2007-02-03');
 *         -> 'Saturday'
 * </pre>
 * 
 * @author jianghang 2014-4-16 下午5:52:04
 * @since 5.0.7
 */
public class Dayname extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (ExecUtils.isNull(arg)) {
                return null;
            }
        }

        java.sql.Timestamp timestamp = DataType.TimestampType.convertFrom(args[0]);
        Calendar cal = Calendar.getInstance();
        cal.setTime(timestamp);

        int dayname = cal.get(Calendar.DAY_OF_WEEK);
        switch (dayname) {
            case Calendar.SUNDAY:
                return "Sunday";
            case Calendar.MONDAY:
                return "Monday";
            case Calendar.TUESDAY:
                return "Tuesday";
            case Calendar.WEDNESDAY:
                return "Wednesday";
            case Calendar.THURSDAY:
                return "Thursday";
            case Calendar.FRIDAY:
                return "Friday";
            case Calendar.SATURDAY:
                return "Saturday";
            default:
                throw new FunctionException("impossbile");
        }
    }

    @Override
    public DataType getReturnType() {
        return DataType.StringType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "DAYNAME" };
    }
}
