package com.taobao.tddl.executor.function.scalar.datatime;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * returns expr1 – expr2 expressed as a value in days from one date to the
 * other. expr1 and expr2 are date or date-and-time expressions. Only the date
 * parts of the values are used in the calculation.
 * 
 * <pre>
 * mysql> SELECT DATEDIFF('2007-12-31 23:59:59','2007-12-30');
 *         -> 1
 * mysql> SELECT DATEDIFF('2010-11-30 23:59:59','2010-12-31');
 *         -> -31
 * </pre>
 * 
 * @author jianghang 2014-4-17 上午10:10:23
 * @since 5.0.7
 */
public class DateDiff extends ScalarFunction {

    private static final Long DAY = 24 * 60 * 60 * 1000L;

    @SuppressWarnings("deprecation")
    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (ExecUtils.isNull(arg)) {
                return null;
            }
        }

        java.sql.Timestamp timestamp1 = DataType.TimestampType.convertFrom(args[0]);
        timestamp1.setHours(0);
        timestamp1.setMinutes(0);
        timestamp1.setSeconds(0);
        java.sql.Timestamp timestamp2 = DataType.TimestampType.convertFrom(args[1]);
        timestamp2.setHours(0);
        timestamp2.setMinutes(0);
        timestamp2.setSeconds(0);
        return (timestamp1.getTime() - timestamp2.getTime()) / DAY;
    }

    @Override
    public DataType getReturnType() {
        return DataType.LongType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "DATEDIFF" };
    }
}
