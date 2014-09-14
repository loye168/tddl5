package com.taobao.tddl.executor.function.scalar.string;

import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * <pre>
 * REPEAT(str,count)
 * 
 * Returns a string consisting of the string str repeated count times. If count is less than 1, returns an empty string. Returns NULL if str or count are NULL.
 * 
 * mysql> SELECT REPEAT('MySQL', 3);
 *         -> 'MySQLMySQLMySQL'
 * </pre>
 * 
 * @author mengshi.sunmengshi 2014年4月11日 下午6:39:43
 * @since 5.1.0
 */
public class Repeat extends ScalarFunction {

    @Override
    public DataType getReturnType() {
        return DataType.StringType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "REPEAT" };
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (ExecUtils.isNull(arg)) {
                return null;
            }
        }
        String str = DataType.StringType.convertFrom(args[0]);
        Integer count = DataType.IntegerType.convertFrom(args[1]);
        if (count < 1) {
            return "";
        }

        return TStringUtil.repeat(str, count);

    }
}
