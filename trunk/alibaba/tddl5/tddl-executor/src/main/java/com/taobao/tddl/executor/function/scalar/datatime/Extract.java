package com.taobao.tddl.executor.function.scalar.datatime;

import java.text.SimpleDateFormat;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.function.scalar.datatime.Interval.Interval_Unit;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * The EXTRACT() function uses the same kinds of unit specifiers as DATE_ADD()
 * or DATE_SUB(), but extracts parts from the date rather than performing date
 * arithmetic.
 * 
 * <pre>
 * mysql> SELECT EXTRACT(YEAR FROM '2009-07-02');
 *        -> 2009
 * mysql> SELECT EXTRACT(YEAR_MONTH FROM '2009-07-02 01:02:03');
 *        -> 200907
 * mysql> SELECT EXTRACT(DAY_MINUTE FROM '2009-07-02 01:02:03');
 *        -> 20102
 * mysql> SELECT EXTRACT(MICROSECOND
 *     ->                FROM '2003-01-02 10:30:00.000123');
 *         -> 123
 * </pre>
 * 
 * @author jianghang 2014-4-17 上午11:41:12
 * @since 5.0.7
 */
public class Extract extends ScalarFunction {

    @SuppressWarnings("deprecation")
    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (ExecUtils.isNull(arg)) {
                return null;
            }
        }

        String interval = DataType.StringType.convertFrom(args[0]);
        Interval_Unit unit = Interval_Unit.valueOf(interval);
        java.sql.Timestamp timestamp = DataType.TimestampType.convertFrom(args[1]);
        if (unit == Interval_Unit.QUARTER) {
            return timestamp.getMonth() / 3 + 1;
        } else {
            SimpleDateFormat format = new SimpleDateFormat(unit.format);
            String value = format.format(timestamp);
            DataType type = getReturnType();
            return type.convertFrom(value);
        }
    }

    @Override
    public DataType getReturnType() {
        return DataType.LongType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "EXTRACT" };
    }
}
