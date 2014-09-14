package com.taobao.tddl.executor.function.scalar.math;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Returns the arc tangent of X, that is, the value whose tangent is X.
 * 
 * <pre>
 * mysql> SELECT ATAN(2);
 *         -> 1.1071487177941
 * mysql> SELECT ATAN(-2);
 *         -> -1.1071487177941
 *         
 * mysql> SELECT ATAN(-2,2);
 *         -> -0.78539816339745
 * mysql> SELECT ATAN2(PI(),0);
 *         -> 1.5707963267949
 * </pre>
 * 
 * @author jianghang 2014-4-14 下午9:43:36
 * @since 5.0.7
 */
public class Atan extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        DataType type = getReturnType();
        if (ExecUtils.isNull(args[0])) {
            return null;
        }

        Double d = (Double) type.convertFrom(args[0]);
        if (args.length >= 2) {
            if (ExecUtils.isNull(args[1])) {
                return null;
            }

            Double y = (Double) type.convertFrom(args[1]);
            return Math.atan2(d, y);
        } else {
            return Math.atan(d);
        }
    }

    @Override
    public DataType getReturnType() {
        return DataType.DoubleType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "ATAN", "ATAN2" };
    }

}
