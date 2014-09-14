package com.taobao.tddl.executor.function.scalar.math;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * If called with one parameter, this function returns the natural logarithm of
 * X. If X is less than or equal to 0, then NULL is returned. The inverse of
 * this function (when called with a single argument) is the EXP() function.
 * 
 * <pre>
 * mysql> SELECT LOG(2);
 *         -> 0.69314718055995
 * mysql> SELECT LOG(-2);
 *         -> NULL
 * </pre>
 * 
 * If called with two parameters, this function returns the logarithm of X to
 * the base B. If X is less than or equal to 0, or if B is less than or equal to
 * 1, then NULL is returned.
 * 
 * <pre>
 * mysql> SELECT LOG(2,65536);
 *         -> 16
 * mysql> SELECT LOG(10,100);
 *         -> 2
 * mysql> SELECT LOG(1,100);
 *         -> NULL
 * </pre>
 * 
 * LOG(B,X) is equivalent to LOG(X) / LOG(B).
 * 
 * @author jianghang 2014-4-14 下午10:30:20
 * @since 5.0.7
 */
public class Log extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        DataType type = getReturnType();
        if (ExecUtils.isNull(args[0])) {
            return null;
        }

        Double d = (Double) type.convertFrom(args[0]);
        if (d <= 0) {
            return null;
        }

        if (args.length >= 2) {
            if (args[1] == null) {
                return null;
            }
            Double b = (Double) type.convertFrom(args[1]);
            if (b <= 0) {
                return null;
            }

            Double t = Math.log(d);
            if (t == 0.0) {
                return null;
            }
            return Math.log(b) / t;
        } else {
            return Math.log(d);
        }
    }

    @Override
    public DataType getReturnType() {
        return DataType.DoubleType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "LOG" };
    }

}
