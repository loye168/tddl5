package com.taobao.tddl.executor.function.scalar.math;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Returns the argument X, converted from radians to degrees.
 * 
 * <pre>
 * mysql> SELECT DEGREES(PI());
 *         -> 180
 * mysql> SELECT DEGREES(PI() / 2);
 *         -> 90
 * </pre>
 * 
 * @author jianghang 2014-4-14 下午10:17:36
 * @since 5.0.7
 */
public class Degrees extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        DataType type = getReturnType();
        if (ExecUtils.isNull(args[0])) {
            return null;
        }

        Double d = (Double) type.convertFrom(args[0]);
        return Math.toDegrees(d);
    }

    @Override
    public DataType getReturnType() {
        return DataType.DoubleType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "DEGREES" };
    }

}
