package com.taobao.tddl.executor.function.scalar.datatime;

import java.util.Calendar;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Returns the month for date, in the range 1 to 12 for January to December, or
 * 0 for dates such as '0000-00-00' or '2008-00-00' that have a zero month part.
 * 
 * <pre>
 * mysql> SELECT MONTH('2008-02-03');
 *         -> 2
 * </pre>
 * 
 * @author jianghang 2014-4-16 下午6:35:53
 * @since 5.0.7
 */
public class Month extends ScalarFunction {

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

        return cal.get(Calendar.MONTH) + 1;
    }

    @Override
    public DataType getReturnType() {
        return DataType.LongType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "MONTH" };
    }
}
