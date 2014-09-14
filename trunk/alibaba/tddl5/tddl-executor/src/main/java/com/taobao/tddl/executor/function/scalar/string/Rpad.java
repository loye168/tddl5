package com.taobao.tddl.executor.function.scalar.string;

import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * <pre>
 * RPAD(str,len,padstr)
 * 
 * Returns the string str, right-padded with the string padstr to a length of len characters. If str is longer than len, the return value is shortened to len characters.
 * 
 * mysql> SELECT RPAD('hi',5,'?');
 *         -> 'hi???'
 * mysql> SELECT RPAD('hi',1,'?');
 *         -> 'h'
 * 
 * </pre>
 * 
 * @author mengshi.sunmengshi 2014年4月11日 下午6:23:39
 * @since 5.1.0
 */
public class Rpad extends ScalarFunction {

    @Override
    public DataType getReturnType() {
        return DataType.StringType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "RPAD" };
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
        String padStr = DataType.StringType.convertFrom(args[2]);

        if (len == str.length()) {
            return str;
        }

        if (len < 0) {
            return null;
        }
        if (len < str.length()) {
            return str.substring(0, len);
        }

        return TStringUtil.rightPad(str, len, padStr);

    }

}
