package com.taobao.tddl.executor.function.scalar;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * @author jianghang 2014-4-15 上午11:30:52
 * @since 5.0.7
 */
public class LastInsertId extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        return ec.getConnection().getLastInsertId();
    }

    @Override
    public DataType getReturnType() {
        return DataType.LongType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "LAST_INSERT_ID" };
    }
}
