package com.taobao.tddl.executor.function.scalar.filter;

import com.taobao.tddl.executor.common.ExecutionContext;

/**
 * @since 5.0.0
 */
public class IsNotNull extends Filter {

    @Override
    protected Boolean computeInner(Object[] args, ExecutionContext ec) {
        Object arg = args[0];
        return arg != null;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "IS NOT NULL" };
    }

}
