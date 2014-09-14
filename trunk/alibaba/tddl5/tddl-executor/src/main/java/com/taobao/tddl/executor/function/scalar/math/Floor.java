package com.taobao.tddl.executor.function.scalar.math;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Returns the largest integer value not greater than X.
 * 
 * <pre>
 * mysql> SELECT FLOOR(1.23);
 *         -> 1
 * mysql> SELECT FLOOR(-1.23);
 *         -> -2
 * </pre>
 * 
 * For exact-value numeric arguments, the return value has an exact-value
 * numeric type. For string or floating-point arguments, the return value has a
 * floating-point type.
 * 
 * @author jianghang 2014-4-14 下午10:19:56
 * @since 5.0.7
 */
public class Floor extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        DataType type = getReturnType();
        if (ExecUtils.isNull(args[0])) {
            return null;
        }

        Double d = DataType.DoubleType.convertFrom(args[0]);
        return type.convertFrom(Math.floor(d));
    }

    @Override
    public DataType getReturnType() {
        return getFirstArgType();
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "FLOOR" };
    }
}
