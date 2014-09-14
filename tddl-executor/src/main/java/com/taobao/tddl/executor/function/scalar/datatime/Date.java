package com.taobao.tddl.executor.function.scalar.datatime;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Extracts the date part of the date or datetime expression expr.
 * 
 * <pre>
 * mysql> SELECT DATE('2003-12-31 01:02:03');
 *         -> '2003-12-31'
 * </pre>
 * 
 * @author jianghang 2014-4-16 下午5:25:10
 * @since 5.0.7
 */
public class Date extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (ExecUtils.isNull(arg)) {
                return null;
            }
        }

        DataType type = getReturnType();
        return type.convertFrom(args[0]);
    }

    @Override
    public DataType getReturnType() {
        return DataType.DateType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "DATE" };
    }
}
