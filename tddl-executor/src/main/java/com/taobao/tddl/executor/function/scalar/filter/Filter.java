package com.taobao.tddl.executor.function.scalar.filter;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.expression.ISelectable;

public abstract class Filter extends ScalarFunction {

    Object result = false;

    protected abstract Object computeInner(Object[] args, ExecutionContext ec);

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            // 有一个没计算的，就直接返回
            if (arg instanceof ISelectable) {
                return this;
            }
        }
        return this.computeInner(args, ec);
    }

    public DataType getArgType() {
        return getFirstArgType();
    }

    @Override
    public DataType getReturnType() {
        return DataType.BooleanType;
    }

}
