package com.taobao.tddl.executor.function.scalar.datatime;

import java.util.Calendar;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * ADDTIME() adds expr2 to expr1 and returns the result. expr1 is a time or
 * datetime expression, and expr2 is a time expression.
 * 
 * @author jianghang 2014-4-16 下午4:00:49
 * @since 5.0.7
 */
public class AddTime extends ScalarFunction {

    @SuppressWarnings("deprecation")
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

        java.sql.Time time = DataType.TimeType.convertFrom(args[1]);

        cal.add(Calendar.HOUR_OF_DAY, time.getHours());
        cal.add(Calendar.MINUTE, time.getMinutes());
        cal.add(Calendar.SECOND, time.getSeconds());
        DataType type = getReturnType();
        return type.convertFrom(cal.getTime());
    }

    @Override
    public DataType getReturnType() {
        DataType type = getFirstArgType();
        if (type == DataType.DateType) {
            return DataType.TimestampType;
        }

        return type;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "ADDTIME" };
    }
}
