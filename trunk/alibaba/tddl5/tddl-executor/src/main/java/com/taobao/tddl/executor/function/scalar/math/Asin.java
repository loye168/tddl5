package com.taobao.tddl.executor.function.scalar.math;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Returns the arc sine of X, that is, the value whose sine is X. Returns NULL
 * if X is not in the range -1 to 1.
 * 
 * <pre>
 * mysql> SELECT ASIN(0.2);
 *         -> 0.20135792079033
 * mysql> SELECT ASIN('foo');
 * 
 * +-------------+
 * | ASIN('foo') |
 * +-------------+
 * |           0 |
 * +-------------+
 * 1 row in set, 1 warning (0.00 sec)
 * </pre>
 * 
 * @author jianghang 2014-4-14 下午9:40:41
 * @since 5.0.7
 */
public class Asin extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        DataType type = getReturnType();
        if (ExecUtils.isNull(args[0])) {
            return null;
        }

        Double d = (Double) type.convertFrom(args[0]);
        Long l = d.longValue();
        if (l > 1 || l < -1) {
            return null;
        } else {
            return Math.asin(d);
        }
    }

    @Override
    public DataType getReturnType() {
        return DataType.DoubleType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "ASIN" };
    }

}
