package com.taobao.tddl.executor.function.scalar.math;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Returns the value of X raised to the power of Y.
 * 
 * <pre>
 * mysql> SELECT POW(2,2);
 *         -> 4
 * mysql> SELECT POW(2,-2);
 *         -> 0.25
 * </pre>
 * 
 * @author jianghang 2014-4-14 下午10:45:20
 * @since 5.0.7
 */
public class Pow extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        DataType type = getReturnType();
        if (args.length < 2) {
            return null;
        }

        if (ExecUtils.isNull(args[0]) || ExecUtils.isNull(args[1])) {
            return null;
        }

        Double x = (Double) type.convertFrom(args[0]);
        Double y = (Double) type.convertFrom(args[1]);
        return Math.pow(x, y);
    }

    @Override
    public DataType getReturnType() {
        return DataType.DoubleType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "POW", "POWER" };
    }

}
