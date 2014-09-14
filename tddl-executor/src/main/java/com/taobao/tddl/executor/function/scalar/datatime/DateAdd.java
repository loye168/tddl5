package com.taobao.tddl.executor.function.scalar.datatime;

import java.util.Calendar;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.datatype.IntervalType;

/**
 * ADDDATE(date,INTERVAL expr unit), ADDDATE(expr,days) When invoked with the
 * INTERVAL form of the second argument, ADDDATE() is a synonym for DATE_ADD().
 * The related function SUBDATE() is a synonym for DATE_SUB(). For information
 * on the INTERVAL unit argument, see the discussion for DATE_ADD().
 * 
 * <pre>
 * mysql> SELECT DATE_ADD('2008-01-02', INTERVAL 31 DAY);
 *         -> '2008-02-02'
 * mysql> SELECT ADDDATE('2008-01-02', INTERVAL 31 DAY);
 *         -> '2008-02-02'
 * </pre>
 * 
 * When invoked with the days form of the second argument, MySQL treats it as an
 * integer number of days to be added to expr.
 * 
 * <pre>
 * mysql> SELECT ADDDATE('2008-01-02', 31);
 *         -> '2008-02-02'
 * </pre>
 * 
 * @author jianghang 2014-4-16 下午4:00:49
 * @since 5.0.7
 */
public class DateAdd extends ScalarFunction {

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

        Object day = args[1];
        if (day instanceof IntervalType) {
            ((IntervalType) day).process(cal, 1);
        } else {
            cal.add(Calendar.DAY_OF_YEAR, DataType.IntegerType.convertFrom(day));
        }

        DataType type = getReturnType();
        return type.convertFrom(cal);
    }

    @Override
    public DataType getReturnType() {
        DataType type = getFirstArgType();
        return type;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "DATE_ADD", "ADDDATE" };
    }
}
