package com.taobao.tddl.executor.function.scalar.datatime;

import java.util.Calendar;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Returns the day of the month for date, in the range 1 to 31, or 0 for dates
 * such as '0000-00-00' or '2008-00-00' that have a zero day part.
 * 
 * <pre>
 * mysql> SELECT DAYOFMONTH('2007-02-03');
 *         -> 3
 * </pre>
 * 
 * @author jianghang 2014-4-16 下午5:53:49
 * @since 5.0.7
 */
public class Dayofmonth extends ScalarFunction {

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

        return cal.get(Calendar.DAY_OF_MONTH);
    }

    @Override
    public DataType getReturnType() {
        return DataType.LongType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "DAYOFMONTH" };
    }
}
