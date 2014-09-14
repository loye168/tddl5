package com.taobao.tddl.executor.function.scalar.math;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Returns the square root of a nonnegative number X.
 * 
 * <pre>
 * mysql> SELECT SQRT(4);
 *         -> 2
 * mysql> SELECT SQRT(20);
 *         -> 4.4721359549996
 * mysql> SELECT SQRT(-16);
 *         -> NULL
 * </pre>
 * 
 * @author jianghang 2014-4-14 下午10:51:54
 * @since 5.0.7
 */
public class Sqrt extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        DataType type = getReturnType();
        if (ExecUtils.isNull(args[0])) {
            return null;
        }

        Double d = (Double) type.convertFrom(args[0]);
        Long l = d.longValue();
        if (l < 0) {
            return null;
        } else {
            return Math.sqrt(d);
        }
    }

    @Override
    public DataType getReturnType() {
        return DataType.DoubleType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "SQRT" };
    }

}
