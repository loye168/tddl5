package com.taobao.tddl.executor.function.scalar.datatime;

import java.util.Calendar;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Returns the seconds argument, converted to hours, minutes, and seconds, as a
 * TIME value. The range of the result is constrained to that of the TIME data
 * type. A warning occurs if the argument corresponds to a value outside that
 * range.
 * 
 * <pre>
 * mysql> SELECT SEC_TO_TIME(2378);
 *         -> '00:39:38'
 * mysql> SELECT SEC_TO_TIME(2378) + 0;
 *         -> 3938
 * </pre>
 * 
 * @author jianghang 2014-4-16 下午6:43:41
 * @since 5.0.7
 */
public class SecToTime extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (ExecUtils.isNull(arg)) {
                return null;
            }
        }

        Integer sec = DataType.IntegerType.convertFrom(args[0]);
        Calendar cal = Calendar.getInstance();
        cal.set(0, 0, 0, 0, 0, 0);
        cal.set(Calendar.SECOND, sec);

        DataType type = getReturnType();
        return type.convertFrom(cal.getTime());
    }

    @Override
    public DataType getReturnType() {
        return DataType.TimeType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "SEC_TO_TIME" };
    }
}
