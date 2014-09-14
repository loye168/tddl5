package com.taobao.tddl.executor.function.scalar.filter;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * @since 5.0.0
 */
public class IsFalse extends Filter {

    @Override
    protected Boolean computeInner(Object[] args, ExecutionContext ec) {
        Object arg = args[0];
        if (arg == null) {
            return false;
        }

        Long bool = DataType.LongType.convertFrom(arg);
        return (bool != 0) == false;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "IS FALSE" };
    }

}
