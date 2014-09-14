package com.taobao.tddl.executor.function.scalar.datatime;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Returns the current time as a value in 'HH:MM:SS' or HHMMSS.uuuuuu format,
 * depending on whether the function is used in a string or numeric context. The
 * value is expressed in the current time zone.
 * 
 * @author jianghang 2014-4-15 上午11:31:31
 * @since 5.0.7
 */
public class Curtime extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        DataType type = getReturnType();
        return type.convertFrom(new java.util.Date());
    }

    @Override
    public DataType getReturnType() {
        return DataType.TimeType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "CURTIME", "CURRENT_TIME" };
    }
}
