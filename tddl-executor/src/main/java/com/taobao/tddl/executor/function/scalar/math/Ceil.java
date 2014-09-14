package com.taobao.tddl.executor.function.scalar.math;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Returns the smallest integer value not less than X.
 * 
 * <pre>
 * mysql> SELECT CEILING(1.23);
 *         -> 2
 * mysql> SELECT CEILING(-1.23);
 *         -> -1
 * </pre>
 * 
 * @author jianghang 2014-4-14 下午9:51:36
 * @since 5.0.7
 */
public class Ceil extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        DataType type = getReturnType();
        if (ExecUtils.isNull(args[0])) {
            return null;
        }

        Double d = DataType.DoubleType.convertFrom(args[0]);
        return type.convertFrom(Math.ceil(d));
    }

    @Override
    public DataType getReturnType() {
        return getFirstArgType();
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "CEIL", "CEILING" };
    }

    public static void main(String args[]) {
        System.out.println(Math.ceil(1.23));
        System.out.println(Math.ceil(-1.23));
    }
}
