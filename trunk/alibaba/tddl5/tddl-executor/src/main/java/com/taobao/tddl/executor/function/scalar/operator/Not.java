package com.taobao.tddl.executor.function.scalar.operator;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * @since 5.0.0
 */
public class Not extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        DataType type = getReturnType();
        Object obj = type.convertFrom(args[0]);
        if (obj instanceof Number) {
            return ((Number) args[0]).longValue() == 0 ? 1 : 0;
        } else if (obj instanceof Boolean) {
            return !(Boolean) obj;
        } else {
            // 非数字类型全为false，一般不会走到这逻辑
            return false;
        }
    }

    @Override
    public DataType getReturnType() {
        DataType type = getFirstArgType();
        if (type == DataType.BooleanType) {
            return DataType.BooleanType;
        } else {
            return DataType.LongType;
        }
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "NOT", "!" };
    }
}
