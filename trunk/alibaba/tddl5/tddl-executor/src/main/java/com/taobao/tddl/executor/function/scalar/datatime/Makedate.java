package com.taobao.tddl.executor.function.scalar.datatime;

import java.util.Calendar;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Returns a date, given year and day-of-year values. dayofyear must be greater
 * than 0 or the result is NULL.
 * 
 * <pre>
 * mysql> SELECT MAKEDATE(2011,31), MAKEDATE(2011,32);
 *         -> '2011-01-31', '2011-02-01'
 * mysql> SELECT MAKEDATE(2011,365), MAKEDATE(2014,365);
 *         -> '2011-12-31', '2014-12-31'
 * mysql> SELECT MAKEDATE(2011,0);
 *         -> NULL
 * </pre>
 * 
 * @author jianghang 2014-4-17 上午12:35:57
 * @since 5.0.7
 */
public class Makedate extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (ExecUtils.isNull(arg)) {
                return null;
            }
        }

        Integer year = DataType.IntegerType.convertFrom(args[0]);
        Integer dayOfYear = DataType.IntegerType.convertFrom(args[1]);
        if (dayOfYear <= 0) {
            return null;
        }

        Calendar cal = Calendar.getInstance();
        cal.set(year, 0, 0, 0, 0, 0);
        cal.set(Calendar.DAY_OF_YEAR, dayOfYear);
        DataType type = getReturnType();
        return type.convertFrom(cal.getTime());
    }

    @Override
    public DataType getReturnType() {
        return DataType.DateType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "MAKEDATE" };
    }
}
