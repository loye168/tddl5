package com.taobao.tddl.executor.function.scalar.datatime;

import java.util.Calendar;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Returns the second for time, in the range 0 to 59.
 * 
 * <pre>
 * mysql> SELECT SECOND('10:05:03');
 *         -> 3
 * </pre>
 * 
 * @author jianghang 2014-4-16 下午6:41:08
 * @since 5.0.7
 */
public class Second extends ScalarFunction {

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

        return cal.get(Calendar.SECOND);
    }

    @Override
    public DataType getReturnType() {
        return DataType.LongType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "SECOND" };
    }
}
