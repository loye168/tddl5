package com.taobao.tddl.executor.function.scalar.filter.any;

import java.util.List;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.scalar.filter.Filter;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;

public class GreaterEqualAny extends Filter {

    @Override
    public String[] getFunctionNames() {
        return new String[] { OPERATION.GT_EQ_ANY.getOPERATIONString() };
    }

    @Override
    protected Object computeInner(Object[] args, ExecutionContext ec) {
        // 子查询没有被计算，返回自身
        if (!(args[1] instanceof List)) {
            return this;
        }

        Object left = args[0];
        List rights = (List) args[1];
        DataType type = this.getArgType();
        for (Object right : rights) {
            if (type.compare(left, right) >= 0) {
                return true;
            }
        }

        return false;
    }

}
