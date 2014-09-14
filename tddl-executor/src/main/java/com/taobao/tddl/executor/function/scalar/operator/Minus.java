package com.taobao.tddl.executor.function.scalar.operator;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * mysql的Minus函数,取负数操作
 * 
 * @author jianghang 2014-2-13 上午11:43:32
 * @since 5.0.0
 */
public class Minus extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        DataType type = getReturnType();
        // -min(id) = min(id) * -1
        return type.getCalculator().multiply(args[0], -1);
    }

    @Override
    public DataType getReturnType() {
        return getFirstArgType();
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "MINUS" };
    }
}
