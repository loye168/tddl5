package com.taobao.tddl.executor.function.scalar.string;

import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * <pre>
 * TRIM([{BOTH | LEADING | TRAILING} [remstr] FROM] str), TRIM([remstr FROM] str)
 * 
 * Returns the string str with all remstr prefixes or suffixes removed. If none of the specifiers BOTH, LEADING, or TRAILING is given, BOTH is assumed. remstr is optional and, if not specified, spaces are removed.
 * 
 * mysql> SELECT TRIM('  bar   ');
 *         -> 'bar'
 * mysql> SELECT TRIM(LEADING 'x' FROM 'xxxbarxxx');
 *         -> 'barxxx'
 * mysql> SELECT TRIM(BOTH 'x' FROM 'xxxbarxxx');
 *         -> 'bar'
 * mysql> SELECT TRIM(TRAILING 'xyz' FROM 'barxxyz');
 *         -> 'barx'
 * 
 * </pre>
 * 
 * @author mengshi.sunmengshi 2014年4月15日 下午4:07:06
 * @since 5.1.0
 */
public class Trim extends ScalarFunction {

    @Override
    public DataType getReturnType() {
        return DataType.StringType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "TRIM" };
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {

        for (Object arg : args) {
            if (ExecUtils.isNull(arg)) {
                return null;
            }
        }
        String str = DataType.StringType.convertFrom(args[0]);

        if (args.length == 2) {
            str = TStringUtil.trim(str);
        }

        if (args.length == 3) {
            String trimStr = DataType.StringType.convertFrom(args[1]);
            String direction = DataType.StringType.convertFrom(args[2]);

            if ("BOTH".equals(direction)) {
                while (str.endsWith(trimStr)) {
                    str = TStringUtil.removeEnd(str, trimStr);
                }

                while (str.startsWith(trimStr)) {
                    str = TStringUtil.removeStart(str, trimStr);
                }
            } else if ("TRAILING".equals(direction)) {
                while (str.endsWith(trimStr)) {
                    str = TStringUtil.removeEnd(str, trimStr);
                }

            } else if ("LEADING".equals(direction)) {

                while (str.startsWith(trimStr)) {
                    str = TStringUtil.removeStart(str, trimStr);
                }

            }

        }

        return str;
    }
}
