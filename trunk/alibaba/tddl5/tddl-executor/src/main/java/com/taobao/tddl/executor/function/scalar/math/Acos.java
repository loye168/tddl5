package com.taobao.tddl.executor.function.scalar.math;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Returns the arc cosine of X, that is, the value whose cosine is X. Returns
 * NULL if X is not in the range -1 to 1.
 * 
 * <pre>
 * mysql> SELECT ACOS(1);
 *         -> 0
 * mysql> SELECT ACOS(1.0001);
 *         -> NULL
 * mysql> SELECT ACOS(0);
 *         -> 1.5707963267949
 * </pre>
 * 
 * @author jianghang 2014-4-14 下午9:39:21
 * @since 5.0.7
 */
public class Acos extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        DataType type = getReturnType();

        if (ExecUtils.isNull(args[0])) {
            return null;
        }

        Double d = (Double) type.convertFrom(args[0]);
        if (d > 1 || d < -1) {
            return null;
        } else {
            return Math.acos(d);
        }
    }

    @Override
    public DataType getReturnType() {
        return DataType.DoubleType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "ACOS" };
    }

}
