package com.taobao.tddl.executor.function.scalar.datatime;

import java.util.Calendar;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Returns the quarter of the year for date, in the range 1 to 4.
 * 
 * <pre>
 * mysql> SELECT QUARTER('2008-04-01');
 *         -> 2
 * </pre>
 * 
 * @author jianghang 2014-4-16 下午6:41:30
 * @since 5.0.7
 */
public class Quarter extends ScalarFunction {

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

        return (cal.get(Calendar.MONTH) / 3) + 1;
    }

    @Override
    public DataType getReturnType() {
        return DataType.LongType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "QUARTER" };
    }
}
