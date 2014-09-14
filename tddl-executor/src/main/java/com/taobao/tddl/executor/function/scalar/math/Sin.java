package com.taobao.tddl.executor.function.scalar.math;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Returns the sine of X, where X is given in radians.
 * 
 * <pre>
 * mysql> SELECT SIN(PI());
 *         -> 1.2246063538224e-16
 * mysql> SELECT ROUND(SIN(PI()));
 *         -> 0
 * </pre>
 * 
 * @author jianghang 2014-4-14 下午10:51:31
 * @since 5.0.7
 */
public class Sin extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        DataType type = getReturnType();
        if (ExecUtils.isNull(args[0])) {
            return null;
        }

        Double d = (Double) type.convertFrom(args[0]);
        return Math.sin(d);
    }

    @Override
    public DataType getReturnType() {
        return DataType.DoubleType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "SIN" };
    }

}
