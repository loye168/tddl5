package com.taobao.tddl.executor.function.scalar.filter;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * @since 5.0.0
 */
public class IsTrue extends Filter {

    @Override
    protected Boolean computeInner(Object[] args, ExecutionContext ec) {
        Object arg = args[0];
        if (arg == null) {
            return false;
        }

        // mysql中数字1会被当作1处理,'true'字符串会被当作0处理
        Long bool = DataType.LongType.convertFrom(arg);
        return (bool != 0) == true;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "IS TRUE" };
    }

}
