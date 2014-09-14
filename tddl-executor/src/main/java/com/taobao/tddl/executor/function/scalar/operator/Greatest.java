package com.taobao.tddl.executor.function.scalar.operator;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * With two or more arguments, returns the largest (maximum-valued) argument.
 * The arguments are compared using the same rules as for LEAST().
 * 
 * <pre>
 * mysql> SELECT GREATEST(2,0);
 *         -> 2
 * mysql> SELECT GREATEST(34.0,3.0,5.0,767.0);
 *         -> 767.0
 * mysql> SELECT GREATEST('B','A','C');
 *         -> 'C'
 * </pre>
 * 
 * @author jianghang 2014-4-21 下午6:00:26
 * @since 5.0.7
 */
public class Greatest extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (ExecUtils.isNull(arg)) {
                return null;
            }
        }

        DataType type = getReturnType();
        Object first = type.convertFrom(args[0]);
        Object max = first;
        for (int i = 1; i < args.length; i++) {
            Object cp = type.convertFrom(args[i]);
            max = (type.compare(max, cp) >= 0 ? max : cp);
        }

        return max;
    }

    @Override
    public DataType getReturnType() {
        return getMixedStringType();
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "GREATEST" };
    }
}
