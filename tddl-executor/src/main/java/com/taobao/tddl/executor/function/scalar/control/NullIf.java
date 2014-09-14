package com.taobao.tddl.executor.function.scalar.control;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Returns NULL if expr1 = expr2 is true, otherwise returns expr1. This is the
 * same as CASE WHEN expr1 = expr2 THEN NULL ELSE expr1 END.
 * 
 * <pre>
 * mysql> SELECT NULLIF(1,1);
 *         -> NULL
 * mysql> SELECT NULLIF(1,2);
 *         -> 1
 * </pre>
 * 
 * Note that MySQL evaluates expr1 twice if the arguments are not equal.
 * 
 * @author jianghang 2014-4-15 上午11:01:37
 * @since 5.0.7
 */
public class NullIf extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (ExecUtils.isNull(args[0])) {
            return null;
        } else if (ExecUtils.isNull(args[1])) {
            return args[0];
        } else {
            DataType type = getReturnType();
            Object t1 = type.convertFrom(args[0]);
            Object t2 = type.convertFrom(args[1]);

            if (t1.equals(t2)) {
                return null;
            } else {
                return t1;
            }
        }
    }

    @Override
    public DataType getReturnType() {
        return getFirstArgType();
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "NULLIF" };
    }
}
