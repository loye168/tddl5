package com.taobao.tddl.executor.function.scalar.datatime;

import java.util.Calendar;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * SUBTIME() returns expr1 – expr2 expressed as a value in the same format as
 * expr1. expr1 is a time or datetime expression, and expr2 is a time
 * expression.
 * 
 * <pre>
 * mysql> SELECT SUBTIME('2007-12-31 23:59:59','1:1:1');
 *         -> '2007-12-30 22:58:58.999997'
 * </pre>
 * 
 * @author jianghang 2014-4-16 下午11:29:18
 * @since 5.0.7
 */
public class SubTime extends ScalarFunction {

    @SuppressWarnings("deprecation")
    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (ExecUtils.isNull(arg)) {
                return null;
            }
        }

        java.sql.Timestamp timestamp = DataType.TimestampType.convertFrom(args[0]);
        java.sql.Time time = DataType.TimeType.convertFrom(args[1]);

        Calendar cal = Calendar.getInstance();
        cal.setTime(timestamp);

        cal.add(Calendar.HOUR_OF_DAY, -1 * time.getHours());
        cal.add(Calendar.MINUTE, -1 * time.getMinutes());
        cal.add(Calendar.SECOND, -1 * time.getSeconds());
        DataType type = getReturnType();
        return type.convertFrom(cal.getTime());
    }

    @Override
    public DataType getReturnType() {
        return getFirstArgType();
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "SUBTIME" };
    }
}
