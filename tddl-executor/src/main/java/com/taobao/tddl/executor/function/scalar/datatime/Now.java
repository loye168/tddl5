package com.taobao.tddl.executor.function.scalar.datatime;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * @author jianghang 2014-4-15 上午11:30:52
 * @since 5.0.7
 */
public class Now extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        DataType type = getReturnType();
        return type.convertFrom(new java.util.Date());
    }

    @Override
    public DataType getReturnType() {
        return DataType.TimestampType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "NOW", "CURRENT_TIMESTAMP", "LOCALTIME", "LOCALTIMESTAMP", "SYSDATE" };
    }
}
