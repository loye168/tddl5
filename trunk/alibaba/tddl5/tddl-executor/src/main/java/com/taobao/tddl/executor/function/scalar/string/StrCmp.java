package com.taobao.tddl.executor.function.scalar.string;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * <pre>
 * STRCMP(expr1,expr2)
 * 
 * STRCMP() returns 0 if the strings are the same, -1 if the first argument is smaller than the second according to the current sort order, and 1 otherwise.
 * 
 * mysql> SELECT STRCMP('text', 'text2');
 *         -> -1
 * mysql> SELECT STRCMP('text2', 'text');
 *         -> 1
 * mysql> SELECT STRCMP('text', 'text');
 *         -> 0
 * </pre>
 * 
 * @author mengshi.sunmengshi 2014年4月11日 下午6:27:36
 * @since 5.1.0
 */
public class StrCmp extends ScalarFunction {

    @Override
    public DataType getReturnType() {
        return DataType.IntegerType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "STRCMP" };
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (ExecUtils.isNull(arg)) {
                return null;
            }
        }
        String str1 = DataType.StringType.convertFrom(args[0]);
        String str2 = DataType.StringType.convertFrom(args[1]);

        return DataType.StringType.compare(str1, str2);
    }

}
