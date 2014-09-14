package com.taobao.tddl.executor.function.scalar.string;

import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.expression.bean.NullValue;

/**
 * <pre>
 * CONCAT(str1,str2,...)
 * 
 * Returns the string that results from concatenating the arguments. May have one or more arguments. If all arguments are nonbinary strings, the result is a nonbinary string. If the arguments include any binary strings, the result is a binary string. A numeric argument is converted to its equivalent string form. This is a nonbinary string as of MySQL 5.5.3. Before 5.5.3, it is a binary string; to to avoid that and produce a nonbinary string, you can use an explicit type cast, as in this example:
 * 
 * SELECT CONCAT(CAST(int_col AS CHAR), char_col);
 * 
 * CONCAT() returns NULL if any argument is NULL.
 * 
 * mysql> SELECT CONCAT('My', 'S', 'QL');
 *         -> 'MySQL'
 * mysql> SELECT CONCAT('My', NULL, 'QL');
 *         -> NULL
 * mysql> SELECT CONCAT(14.3);
 *         -> '14.3'
 * </pre>
 * 
 * @author mengshi.sunmengshi 2014年4月11日 下午1:27:46
 * @since 5.1.0
 */
public class Concat extends ScalarFunction {

    @Override
    public DataType getReturnType() {
        return DataType.StringType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "CONCAT" };
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {

        for (Object arg : args) {
            if (arg == null || arg instanceof NullValue) {
                return null;
            }
        }

        StringBuilder str = new StringBuilder();

        for (Object arg : args) {
            String argStr = DataType.StringType.convertFrom(arg);

            if (!TStringUtil.isEmpty(argStr)) {
                str.append(argStr);
            }
        }

        return str.toString();

    }

}
