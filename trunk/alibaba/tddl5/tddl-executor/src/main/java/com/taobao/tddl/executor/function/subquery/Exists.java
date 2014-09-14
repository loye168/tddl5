package com.taobao.tddl.executor.function.subquery;

import java.util.List;

import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.scalar.filter.Filter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;

public class Exists extends Filter {

    @Override
    public String[] getFunctionNames() {
        return new String[] { OPERATION.EXISTS.getOPERATIONString() };
    }

    @Override
    protected Object computeInner(Object[] args, ExecutionContext ec) {
        List arg = (List) args[0];

        if (!GeneralUtil.isEmpty(arg)) {
            return true;
        } else {
            return false;
        }

    }

}
