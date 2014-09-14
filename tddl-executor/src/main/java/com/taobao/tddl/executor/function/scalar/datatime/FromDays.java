package com.taobao.tddl.executor.function.scalar.datatime;

import java.util.Calendar;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Given a day number N, returns a DATE value.
 * 
 * <pre>
 * mysql> SELECT FROM_DAYS(730669);
 *         -> '2007-07-03'
 * </pre>
 * 
 * Use FROM_DAYS() with caution on old dates. It is not intended for use with
 * values that precede the advent of the Gregorian calendar (1582). See Section
 * 12.8, “What Calendar Is Used By MySQL?”.
 * 
 * @author jianghang 2014-4-16 下午6:07:32
 * @since 5.0.7
 */
public class FromDays extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (ExecUtils.isNull(arg)) {
                return null;
            }
        }

        Integer days = DataType.IntegerType.convertFrom(args[0]);
        Calendar cal = Calendar.getInstance();
        cal.set(0, 0, 0, 0, 0, 0);
        cal.set(Calendar.DAY_OF_YEAR, days + 1);

        DataType type = getReturnType();
        return type.convertFrom(cal.getTime());
    }

    @Override
    public DataType getReturnType() {
        return DataType.DateType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "FROM_DAYS" };
    }
}
