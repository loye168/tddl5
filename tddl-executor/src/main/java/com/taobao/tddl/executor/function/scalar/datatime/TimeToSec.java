package com.taobao.tddl.executor.function.scalar.datatime;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Returns the time argument, converted to seconds.
 * 
 * <pre>
 * mysql> SELECT TIME_TO_SEC('22:23:00');
 *         -> 80580
 * mysql> SELECT TIME_TO_SEC('00:39:38');
 *         -> 2378
 * </pre>
 * 
 * @author jianghang 2014-4-16 下午11:02:01
 * @since 5.0.7
 */
public class TimeToSec extends ScalarFunction {

    @SuppressWarnings("deprecation")
    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (ExecUtils.isNull(arg)) {
                return null;
            }
        }

        java.sql.Time time = DataType.TimeType.convertFrom(args[0]);
        return time.getHours() * 60 * 60 + time.getMinutes() * 60 + time.getSeconds();
    }

    @Override
    public DataType getReturnType() {
        return DataType.LongType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "TIME_TO_SEC" };
    }

}
