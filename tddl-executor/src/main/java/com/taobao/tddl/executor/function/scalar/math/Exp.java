package com.taobao.tddl.executor.function.scalar.math;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Returns the value of e (the base of natural logarithms) raised to the power
 * of X. The inverse of this function is LOG() (using a single argument only) or
 * LN().
 * 
 * <pre>
 * mysql> SELECT EXP(2);
 *         -> 7.3890560989307
 * mysql> SELECT EXP(-2);
 *         -> 0.13533528323661
 * mysql> SELECT EXP(0);
 *         -> 1
 * </pre>
 * 
 * @author jianghang 2014-4-14 下午10:18:31
 * @since 5.0.7
 */
public class Exp extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        DataType type = getReturnType();
        if (ExecUtils.isNull(args[0])) {
            return null;
        }

        Double d = (Double) type.convertFrom(args[0]);
        return Math.exp(d);
    }

    @Override
    public DataType getReturnType() {
        return DataType.DoubleType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "EXP" };
    }

}
