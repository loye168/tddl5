package com.taobao.tddl.executor.function.scalar.string;

import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * <pre>
 * REPLACE(str,from_str,to_str)
 * 
 * Returns the string str with all occurrences of the string from_str replaced by the string to_str. REPLACE() performs a case-sensitive match when searching for from_str.
 * 
 * mysql> SELECT REPLACE('www.mysql.com', 'w', 'Ww');
 *         -> 'WwWwWw.mysql.com'
 * 
 * </pre>
 * 
 * @author mengshi.sunmengshi 2014年4月15日 下午5:32:36
 * @since 5.1.0
 */
public class Replace extends ScalarFunction {

    @Override
    public DataType getReturnType() {
        return DataType.StringType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "REPLACE" };
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {

        for (Object arg : args) {
            if (ExecUtils.isNull(arg)) {
                return null;
            }
        }
        String str = DataType.StringType.convertFrom(args[0]);
        String fromStr = DataType.StringType.convertFrom(args[1]);
        String toStr = DataType.StringType.convertFrom(args[2]);

        return TStringUtil.replace(str, fromStr, toStr);

    }

}
