package com.taobao.tddl.executor.function.scalar.filter;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * @since 5.0.0
 */
public class GreaterEqual extends Filter {

    @Override
    protected Boolean computeInner(Object[] args, ExecutionContext ec) {
        DataType type = this.getArgType();
        return type.compare(args[0], args[1]) >= 0;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { ">=" };
    }

    public DataType getArgType() {
        return getMixedFirstType();
    }
}
