package com.taobao.tddl.executor.function.scalar.datatime;

import java.util.Calendar;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Takes a date or datetime value and returns the corresponding value for the
 * last day of the month. Returns NULL if the argument is invalid.
 * 
 * <pre>
 * mysql> SELECT LAST_DAY('2003-02-05');
 *         -> '2003-02-28'
 * mysql> SELECT LAST_DAY('2004-02-05');
 *         -> '2004-02-29'
 * mysql> SELECT LAST_DAY('2004-01-01 01:01:01');
 *         -> '2004-01-31'
 * mysql> SELECT LAST_DAY('2003-03-32');
 *         -> NULL
 * </pre>
 * 
 * @author jianghang 2014-4-16 下午6:24:45
 * @since 5.0.7
 */
public class LastDay extends ScalarFunction {

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

        cal.set(Calendar.DAY_OF_MONTH, cal.getActualMaximum(Calendar.DAY_OF_MONTH));
        DataType type = getReturnType();
        return type.convertFrom(cal.getTime());
    }

    @Override
    public DataType getReturnType() {
        return DataType.DateType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "LAST_DAY" };
    }
}
