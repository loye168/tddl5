package com.taobao.tddl.executor.function.scalar.math;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Returns the base-2 logarithm of X.
 * 
 * <pre>
 * mysql> SELECT LOG2(65536);
 *         -> 16
 * mysql> SELECT LOG2(-100);
 *         -> NULL
 * </pre>
 * 
 * LOG2() is useful for finding out how many bits a number requires for storage.
 * This function is equivalent to the expression LOG(X) / LOG(2).
 * 
 * @author jianghang 2014-4-14 下午10:38:09
 * @since 5.0.7
 */
public class Log2 extends ScalarFunction {

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
        return Math.log(d) / Math.log(2);
    }

    @Override
    public DataType getReturnType() {
        return DataType.DoubleType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "LOG2" };
    }

    public static void main(String args[]) {
        System.out.println(Math.log(65536) / Math.log(2));
    }
}
