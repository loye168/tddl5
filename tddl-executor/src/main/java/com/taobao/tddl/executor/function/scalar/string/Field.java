package com.taobao.tddl.executor.function.scalar.string;

import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * <pre>
 * FIELD(str,str1,str2,str3,...)
 * 
 * Returns the index (position) of str in the str1, str2, str3, ... list. Returns 0 if str is not found.
 * 
 * All arguments are compared as strings.
 * 
 * If str is NULL, the return value is 0 because NULL fails equality comparison with any value. FIELD() is the complement of ELT().
 * 
 * mysql> SELECT FIELD('ej', 'Hej', 'ej', 'Heja', 'hej', 'foo');
 *         -> 2
 * mysql> SELECT FIELD('fo', 'Hej', 'ej', 'Heja', 'hej', 'foo');
 *         -> 0
 * </pre>
 * 
 * @author mengshi.sunmengshi 2014年4月11日 下午2:18:25
 * @since 5.1.0
 */
public class Field extends ScalarFunction {

    @Override
    public DataType getReturnType() {
        return DataType.IntegerType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "FIELD" };
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (ExecUtils.isNull(args[0])) {
            return 0;
        }

        String str = DataType.StringType.convertFrom(args[0]);

        for (int i = 1; i < args.length; i++) {
            if (ExecUtils.isNull(args[i])) {
                continue;
            }

            if (TStringUtil.equals(str, DataType.StringType.convertFrom(args[i]))) {
                return i;
            }
        }

        return 0;

    }

}
