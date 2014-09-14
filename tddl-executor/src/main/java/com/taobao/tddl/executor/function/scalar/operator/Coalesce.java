package com.taobao.tddl.executor.function.scalar.operator;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Returns the first non-NULL value in the list, or NULL if there are no
 * non-NULL values.
 * 
 * <pre>
 * mysql> SELECT COALESCE(NULL,1);
 *         -> 1
 * mysql> SELECT COALESCE(NULL,NULL,NULL);
 *         -> NULL
 * </pre>
 * 
 * @author jianghang 2014-4-21 下午6:09:13
 * @since 5.0.7
 */
public class Coalesce extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        DataType type = getReturnType();
        for (Object arg : args) {
            if (!ExecUtils.isNull(arg)) {
                return type.convertFrom(arg);
            }
        }

        return null;
    }

    @Override
    public DataType getReturnType() {
        return getMixedStringType();
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "COALESCE" };
    }
}
