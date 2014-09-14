package com.taobao.tddl.executor.function.scalar.math;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Returns the cotangent of X.
 * 
 * <pre>
 * mysql> SELECT COT(12);
 *         -> -1.5726734063977
 * mysql> SELECT COT(0);
 *         -> NULL
 * </pre>
 * 
 * @author jianghang 2014-4-14 下午10:13:20
 * @since 5.0.7
 */
public class Cot extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        DataType type = getReturnType();
        if (ExecUtils.isNull(args[0])) {
            return null;
        }

        Double d = (Double) type.convertFrom(args[0]);
        Double t = Math.tan(d);
        if (t == 0) {
            return null;
        } else {
            return 1.0 / t;
        }
    }

    @Override
    public DataType getReturnType() {
        return DataType.DoubleType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "COT" };
    }

}
