package com.taobao.tddl.executor.function.scalar.math;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Returns the tangent of X, where X is given in radians.
 * 
 * <pre>
 * mysql> SELECT TAN(PI());
 *         -> -1.2246063538224e-16
 * mysql> SELECT TAN(PI()+1);
 *         -> 1.5574077246549
 * </pre>
 * 
 * @author jianghang 2014-4-14 下午10:52:11
 * @since 5.0.7
 */
public class Tan extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        DataType type = getReturnType();
        if (ExecUtils.isNull(args[0])) {
            return null;
        }

        Double d = (Double) type.convertFrom(args[0]);
        return Math.tan(d);
    }

    @Override
    public DataType getReturnType() {
        return DataType.DoubleType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "TAN" };
    }

}
