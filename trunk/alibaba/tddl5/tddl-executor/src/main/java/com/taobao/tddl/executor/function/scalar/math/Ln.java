package com.taobao.tddl.executor.function.scalar.math;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Returns the natural logarithm of X; that is, the base-e logarithm of X. If X
 * is less than or equal to 0, then NULL is returned.
 * 
 * <pre>
 * mysql> SELECT LN(2);
 *         -> 0.69314718055995
 * mysql> SELECT LN(-2);
 *         -> NULL
 * </pre>
 * 
 * This function is synonymous with LOG(X). The inverse of this function is the
 * EXP() function.
 * 
 * @author jianghang 2014-4-14 下午10:20:59
 * @since 5.0.7
 */
public class Ln extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        DataType type = getReturnType();
        if (ExecUtils.isNull(args[0])) {
            return null;
        }

        Double d = (Double) type.convertFrom(args[0]);
        if (d <= 0) {
            return null;
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
        return new String[] { "LN" };
    }

    public static void main(String args[]) {
        System.out.println(Math.log(1));
    }
}
