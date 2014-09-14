package com.taobao.tddl.executor.function.scalar.string;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * <pre>
 * RTRIM(str)
 * 
 * Returns the string str with trailing space characters removed.
 * 
 * mysql> SELECT RTRIM('barbar   ');
 *         -> 'barbar'
 * </pre>
 * 
 * @author mengshi.sunmengshi 2014年4月11日 下午6:03:35
 * @since 5.1.0
 */
public class Rtrim extends ScalarFunction {

    @Override
    public DataType getReturnType() {
        return DataType.StringType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "RTRIM" };
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (ExecUtils.isNull(args[0])) {
            return null;
        }

        String str = DataType.StringType.convertFrom(args[0]);

        int len = str.length();

        while ((0 < len) && (str.charAt(len - 1) <= ' ')) {
            len--;
        }

        if (len < str.length()) return str.substring(0, len);
        else return str;
    }
}
