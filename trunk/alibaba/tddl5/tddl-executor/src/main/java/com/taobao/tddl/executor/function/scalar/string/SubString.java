package com.taobao.tddl.executor.function.scalar.string;

import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * <pre>
 * SUBSTRING(str,pos), SUBSTRING(str FROM pos), SUBSTRING(str,pos,len), SUBSTRING(str FROM pos FOR len)
 * 
 * The forms without a len argument return a substring from string str starting at position pos. The forms with a len argument return a substring len characters long from string str, starting at position pos. The forms that use FROM are standard SQL syntax. It is also possible to use a negative value for pos. In this case, the beginning of the substring is pos characters from the end of the string, rather than the beginning. A negative value may be used for pos in any of the forms of this function.
 * 
 * For all forms of SUBSTRING(), the position of the first character in the string from which the substring is to be extracted is reckoned as 1.
 * 
 * mysql> SELECT SUBSTRING('Quadratically',5);
 *         -> 'ratically'
 * mysql> SELECT SUBSTRING('foobarbar' FROM 4);
 *         -> 'barbar'
 * mysql> SELECT SUBSTRING('Quadratically',5,6);
 *         -> 'ratica'
 * mysql> SELECT SUBSTRING('Sakila', -3);
 *         -> 'ila'
 * mysql> SELECT SUBSTRING('Sakila', -5, 3);
 *         -> 'aki'
 * mysql> SELECT SUBSTRING('Sakila' FROM -4 FOR 2);
 *         -> 'ki'
 * 
 * If len is less than 1, the result is the empty string.
 * </pre>
 * 
 * @author mengshi.sunmengshi 2014年4月15日 下午1:39:12
 * @since 5.1.0
 */
public class SubString extends ScalarFunction {

    @Override
    public DataType getReturnType() {
        return DataType.StringType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "SUBSTRING" };
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (ExecUtils.isNull(args[0])) {
            return null;
        }

        String str = DataType.StringType.convertFrom(args[0]);

        Integer pos = DataType.IntegerType.convertFrom(args[1]);

        Integer len = null;

        if (args.length == 3) {
            len = DataType.IntegerType.convertFrom(args[2]);
        }

        if (pos < 0) {
            pos = str.length() + pos + 1;
        }
        if (len == null) {
            return TStringUtil.substring(str, pos - 1);
        } else {

            if (len < 1) {
                return "";

            } else {
                return TStringUtil.substring(str, pos - 1, pos - 1 + len);
            }
        }

    }
}
