package com.taobao.tddl.executor.function.scalar.string;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * <pre>
 * RIGHT(str,len)
 * 
 * Returns the rightmost len characters from the string str, or NULL if any argument is NULL.
 * 
 * mysql> SELECT RIGHT('foobarbar', 4);
 *         -> 'rbar'
 * </pre>
 * 
 * @author mengshi.sunmengshi 2014年4月11日 下午6:11:40
 * @since 5.1.0
 */
public class Right extends ScalarFunction {

    @Override
    public DataType getReturnType() {
        return DataType.StringType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "RIGHT" };
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (ExecUtils.isNull(arg)) {
                return null;
            }
        }
        String str = DataType.StringType.convertFrom(args[0]);
        Integer len = DataType.IntegerType.convertFrom(args[1]);
        if (len < 0) {
            return "";
        }
        return str.substring(str.length() - len, str.length());

    }
}
