package com.taobao.tddl.executor.function.scalar.datatime;

import java.util.Calendar;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Returns the hour for time. The range of the return value is 0 to 23 for
 * time-of-day values. However, the range of TIME values actually is much
 * larger, so HOUR can return values greater than 23.
 * 
 * <pre>
 * mysql> SELECT HOUR('10:05:03');
 *         -> 10
 * mysql> SELECT HOUR('272:59:59');
 *         -> 272
 * </pre>
 * 
 * @author jianghang 2014-4-16 下午6:16:05
 * @since 5.0.7
 */
public class Hour extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (ExecUtils.isNull(arg)) {
                return null;
            }
        }

        java.sql.Time time = DataType.TimeType.convertFrom(args[0]);
        Calendar cal = Calendar.getInstance();
        cal.setTime(time);

        return cal.get(Calendar.HOUR_OF_DAY);
    }

    @Override
    public DataType getReturnType() {
        return DataType.LongType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "HOUR" };
    }
}
