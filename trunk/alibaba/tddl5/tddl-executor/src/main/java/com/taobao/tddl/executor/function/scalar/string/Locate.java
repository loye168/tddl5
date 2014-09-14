package com.taobao.tddl.executor.function.scalar.string;

import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * <pre>
 * LOCATE(substr,str), LOCATE(substr,str,pos)
 * 
 * The first syntax returns the position of the first occurrence of substring substr in string str. The second syntax returns the position of the first occurrence of substring substr in string str, starting at position pos. Returns 0 if substr is not in str.
 * 
 * mysql> SELECT LOCATE('bar', 'foobarbar');
 *         -> 4
 * mysql> SELECT LOCATE('xbar', 'foobar');
 *         -> 0
 * mysql> SELECT LOCATE('bar', 'foobarbar', 5);
 *         -> 7
 * 
 * </pre>
 * 
 * @author mengshi.sunmengshi 2014年4月15日 下午2:37:28
 * @since 5.1.0
 */
public class Locate extends ScalarFunction {

    @Override
    public DataType getReturnType() {
        return DataType.IntegerType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "LOCATE" };
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (ExecUtils.isNull(arg)) {
                return null;
            }
        }

        String substr = DataType.StringType.convertFrom(args[0]);
        String str = DataType.StringType.convertFrom(args[1]);

        Integer pos = null;
        if (args.length == 3) {
            pos = DataType.IntegerType.convertFrom(args[2]);
        }
        if (pos == null) {
            return TStringUtil.indexOf(str, substr) + 1;
        } else {
            return TStringUtil.indexOf(str, substr, pos + 1) + 1;
        }

    }

}
