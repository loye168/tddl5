package com.taobao.tddl.executor.function.scalar.math;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * <pre>
 * Returns the value of π (pi). The default number of decimal places displayed is seven, but MySQL uses the full double-precision value internally.
 * 
 * mysql> SELECT PI();
 *         -> 3.141593
 * mysql> SELECT PI()+0.000000000000000000;
 *         -> 3.141592653589793116
 * </pre>
 * 
 * @author jianghang 2014-4-14 下午10:44:26
 * @since 5.0.7
 */
public class Pi extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        return Math.PI;
    }

    @Override
    public DataType getReturnType() {
        return DataType.DoubleType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "PI" };
    }

    public static void main(String args[]) {
        System.out.println(Math.PI);
    }
}
