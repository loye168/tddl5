package com.taobao.tddl.executor.function.scalar.datatime;

import java.util.Calendar;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Returns the weekday index for date (0 = Monday, 1 = Tuesday, … 6 = Sunday).
 * 
 * <pre>
 * mysql> SELECT WEEKDAY('2008-02-03 22:23:00');
 *         -> 6
 * mysql> SELECT WEEKDAY('2007-11-06');
 *         -> 1
 * </pre>
 * 
 * @author jianghang 2014-4-16 下午6:50:22
 * @since 5.0.7
 */
public class Weekday extends ScalarFunction {

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

        int week = cal.get(Calendar.DAY_OF_WEEK);
        if (week == Calendar.SUNDAY) {
            return 6;
        } else {
            return week - 2;
        }
    }

    @Override
    public DataType getReturnType() {
        return DataType.LongType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "WEEKDAY" };
    }
}
