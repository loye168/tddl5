package com.taobao.tddl.executor.function.scalar.datatime;

import java.util.TimeZone;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * @author jianghang 2014-4-16 下午10:28:45
 * @since 5.0.7
 */
public class UtcTimestamp extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        DataType type = getReturnType();
        long offest = TimeZone.getDefault().getRawOffset();
        return type.convertFrom(System.currentTimeMillis() - offest);
    }

    @Override
    public DataType getReturnType() {
        return DataType.TimestampType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "UTC_TIMESTAMP" };
    }

}
