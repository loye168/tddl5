package com.taobao.tddl.executor.function.scalar.filter;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * @since 5.0.0
 */
public class Or extends Filter {

    @Override
    protected Boolean computeInner(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (DataType.BooleanType.convertFrom(arg)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "OR", "||" };
    }
}
