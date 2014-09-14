package com.taobao.tddl.executor.function.scalar.datatime;

import java.text.SimpleDateFormat;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

public class TimeFormat extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (ExecUtils.isNull(arg)) {
                return null;
            }
        }

        java.sql.Time time = DataType.TimeType.convertFrom(args[0]);
        String format = DataType.StringType.convertFrom(args[1]);
        SimpleDateFormat dateFormat = new SimpleDateFormat(DateFormat.convertToJavaDataFormat(format));
        return dateFormat.format(time);
    }

    @Override
    public DataType getReturnType() {
        return DataType.StringType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "TIME_FORMAT" };
    }
}
