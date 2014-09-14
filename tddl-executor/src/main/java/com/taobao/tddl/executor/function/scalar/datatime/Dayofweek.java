package com.taobao.tddl.executor.function.scalar.datatime;

import java.util.Calendar;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Returns the weekday index for date (1 = Sunday, 2 = Monday, …, 7 = Saturday).
 * These index values correspond to the ODBC standard.
 * 
 * <pre>
 * mysql> SELECT DAYOFWEEK('2007-02-03');
 *         -> 7
 * </pre>
 * 
 * @author jianghang 2014-4-16 下午5:56:00
 * @since 5.0.7
 */
public class Dayofweek extends ScalarFunction {

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

        return cal.get(Calendar.DAY_OF_WEEK);
    }

    @Override
    public DataType getReturnType() {
        return DataType.LongType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "DAYOFWEEK" };
    }
}
