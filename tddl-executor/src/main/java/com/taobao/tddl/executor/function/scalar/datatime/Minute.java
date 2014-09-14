package com.taobao.tddl.executor.function.scalar.datatime;

import java.util.Calendar;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Returns the minute for time, in the range 0 to 59.
 * 
 * <pre>
 * mysql> SELECT MINUTE('2008-02-03 10:05:03');
 *         -> 5
 * </pre>
 * 
 * @author jianghang 2014-4-16 下午6:33:30
 * @since 5.0.7
 */
public class Minute extends ScalarFunction {

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

        return cal.get(Calendar.MINUTE);
    }

    @Override
    public DataType getReturnType() {
        return DataType.LongType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "MINUTE" };
    }
}
