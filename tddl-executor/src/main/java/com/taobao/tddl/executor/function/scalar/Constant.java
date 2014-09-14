package com.taobao.tddl.executor.function.scalar;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * 对应select 1中的1常量
 * 
 * @author jianghang 2014-4-15 上午11:33:08
 * @since 5.0.7
 */
public class Constant extends ScalarFunction {

    @Override
    public DataType getReturnType() {
        return getFirstArgType();
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "CONSTANT" };
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        return args[0];
    }

}
