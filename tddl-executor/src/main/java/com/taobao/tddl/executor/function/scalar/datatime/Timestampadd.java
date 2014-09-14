package com.taobao.tddl.executor.function.scalar.datatime;

import java.util.Calendar;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.datatype.IntervalType;

/**
 * TIMESTAMPADD(unit,interval,datetime_expr) Adds the integer expression
 * interval to the date or datetime expression datetime_expr. The unit for
 * interval is given by the unit argument, which should be one of the following
 * values: MICROSECOND (microseconds), SECOND, MINUTE, HOUR, DAY, WEEK, MONTH,
 * QUARTER, or YEAR. The unit value may be specified using one of keywords as
 * shown, or with a prefix of SQL_TSI_. For example, DAY and SQL_TSI_DAY both
 * are legal.
 * 
 * <pre>
 * mysql> SELECT TIMESTAMPADD(MINUTE,1,'2003-01-02');
 *         -> '2003-01-02 00:01:00'
 * mysql> SELECT TIMESTAMPADD(WEEK,1,'2003-01-02');
 *         -> '2003-01-09'
 * </pre>
 * 
 * @author jianghang 2014-4-16 下午11:11:30
 * @since 5.0.7
 */
public class Timestampadd extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (ExecUtils.isNull(arg)) {
                return null;
            }
        }

        java.sql.Timestamp timestamp = DataType.TimestampType.convertFrom(args[2]);
        Calendar cal = Calendar.getInstance();
        cal.setTime(timestamp);

        IntervalType date = Interval.paseIntervalDate(args[1], args[0]);
        date.process(cal, 1);

        DataType type = getReturnType();
        return type.convertFrom(cal.getTime());
    }

    @Override
    public DataType getReturnType() {
        return getArgType(function.getArgs().get(2));
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "TIMESTAMPADD" };
    }
}
