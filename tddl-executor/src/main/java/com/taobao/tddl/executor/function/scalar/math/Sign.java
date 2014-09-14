package com.taobao.tddl.executor.function.scalar.math;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Returns the sign of the argument as -1, 0, or 1, depending on whether X is
 * negative, zero, or positive.
 * 
 * <pre>
 * mysql> SELECT SIGN(-32);
 *         -> -1
 * mysql> SELECT SIGN(0);
 *         -> 0
 * mysql> SELECT SIGN(234);
 *         -> 1
 * </pre>
 * 
 * @author jianghang 2014-4-14 下午10:50:20
 * @since 5.0.7
 */
public class Sign extends ScalarFunction {

    private Object computeInner(Object[] args) {
        DataType type = this.getReturnType();
        Object arg = type.convertFrom(args[0]);
        Object zero = type.convertFrom(0);

        return type.convertFrom(type.compare(arg, zero));
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        return this.computeInner(args);
    }

    @Override
    public DataType getReturnType() {
        return getFirstArgType();
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "SIGN" };
    }

}
