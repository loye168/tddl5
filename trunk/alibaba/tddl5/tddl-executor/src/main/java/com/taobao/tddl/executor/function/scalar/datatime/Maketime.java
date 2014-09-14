package com.taobao.tddl.executor.function.scalar.datatime;

import java.util.Calendar;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Returns a time value calculated from the hour, minute, and second arguments.
 * As of MySQL 5.6.4, the second argument can have a fractional part.
 * 
 * <pre>
 * mysql> SELECT MAKETIME(12,15,30);
 *         -> '12:15:30'
 * </pre>
 * 
 * @author jianghang 2014-4-17 上午12:39:07
 * @since 5.0.7
 */
public class Maketime extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (ExecUtils.isNull(arg)) {
                return null;
            }
        }

        Integer hour = DataType.IntegerType.convertFrom(args[0]);
        Integer minute = DataType.IntegerType.convertFrom(args[1]);
        Integer second = DataType.IntegerType.convertFrom(args[2]);

        Calendar cal = Calendar.getInstance();
        cal.set(0, 0, 0, hour, minute, second);
        DataType type = getReturnType();
        return type.convertFrom(cal.getTime());
    }

    @Override
    public DataType getReturnType() {
        return DataType.TimeType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "MAKETIME" };
    }
}
