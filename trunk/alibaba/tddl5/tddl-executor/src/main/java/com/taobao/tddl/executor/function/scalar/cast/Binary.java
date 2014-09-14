package com.taobao.tddl.executor.function.scalar.cast;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * The BINARY operator casts the string following it to a binary string. This is
 * an easy way to force a column comparison to be done byte by byte rather than
 * character by character. This causes the comparison to be case sensitive even
 * if the column is not defined as BINARY or BLOB. BINARY also causes trailing
 * spaces to be significant.
 * 
 * @author jianghang 2014-7-1 上午11:08:14
 * @since 5.1.6
 */
public class Binary extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (ExecUtils.isNull(arg)) {
                return null;
            }
        }
        DataType type = getReturnType();
        return type.convertFrom(args[0]);
    }

    @Override
    public DataType getReturnType() {
        return DataType.BytesType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "BINARY" };
    }
}
