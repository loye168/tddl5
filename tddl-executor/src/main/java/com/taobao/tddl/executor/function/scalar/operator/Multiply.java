package com.taobao.tddl.executor.function.scalar.operator;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;

public class Multiply extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        DataType type = this.getReturnType();
        return type.getCalculator().multiply(args[0], args[1]);
    }

    @Override
    public DataType getReturnType() {
        return getMixedMathType();
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "*", "MULTIPLY" };
    }
}
