package com.taobao.tddl.executor.function.scalar.math;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Returns the base-10 logarithm of X.
 * 
 * <pre>
 * mysql> SELECT LOG10(2);
 *         -> 0.30102999566398
 * mysql> SELECT LOG10(100);
 *         -> 2
 * mysql> SELECT LOG10(-100);
 *         -> NULL
 * </pre>
 * 
 * LOG10(X) is equivalent to LOG(10,X).
 * 
 * @author jianghang 2014-4-14 下午10:40:54
 * @since 5.0.7
 */
public class Log10 extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        DataType type = getReturnType();
        if (ExecUtils.isNull(args[0])) {
            return null;
        }

        Double d = (Double) type.convertFrom(args[0]);
        Long l = d.longValue();
        if (l <= 0) {
            return null;
        }
        return Math.log10(d);
    }

    @Override
    public DataType getReturnType() {
        return DataType.DoubleType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "LOG10" };
    }

}
