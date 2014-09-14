package com.taobao.tddl.executor.function.scalar.math;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Returns the absolute value of X.
 * 
 * <pre>
 * mysql> SELECT ABS(2);
 *         -> 2
 * mysql> SELECT ABS(-32);
 *         -> 32
 * </pre>
 * 
 * This function is safe to use with BIGINT values.
 * 
 * @author jianghang 2014-4-14 下午9:35:18
 * @since 5.0.7
 */
public class Abs extends ScalarFunction {

    private Object computeInner(Object[] args) {
        DataType type = this.getReturnType();
        if (ExecUtils.isNull(args[0])) {
            return null;
        }

        Object arg = type.convertFrom(args[0]);
        Object zero = type.convertFrom(0);

        if (type.compare(arg, zero) < 0) {
            return type.getCalculator().multiply(arg, -1);
        } else {
            return arg;
        }
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
        return new String[] { "ABS" };
    }

}
