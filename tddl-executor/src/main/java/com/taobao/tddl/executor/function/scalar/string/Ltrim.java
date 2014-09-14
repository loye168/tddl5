package com.taobao.tddl.executor.function.scalar.string;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * <pre>
 * LTRIM(str)
 * 
 * Returns the string str with leading space characters removed.
 * 
 * mysql> SELECT LTRIM('  barbar');
 *         -> 'barbar'
 * </pre>
 * 
 * @author mengshi.sunmengshi 2014年4月11日 下午5:56:44
 * @since 5.1.0
 */
public class Ltrim extends ScalarFunction {

    @Override
    public DataType getReturnType() {
        return DataType.StringType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "LTRIM" };
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (ExecUtils.isNull(args[0])) {
            return null;
        }

        String str = DataType.StringType.convertFrom(args[0]);
        int st = 0;
        while (st < str.length() && str.charAt(st) <= ' ') {
            st++;
        }
        if (st > 0) return str.substring(st, str.length());
        else return str;
    }

}
