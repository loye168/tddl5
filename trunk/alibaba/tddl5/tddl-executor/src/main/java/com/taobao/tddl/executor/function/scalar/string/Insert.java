package com.taobao.tddl.executor.function.scalar.string;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * <pre>
 * INSERT(str,pos,len,newstr)
 * 
 * Returns the string str, with the substring beginning at position pos and len characters long replaced by the string newstr. Returns the original string if pos is not within the length of the string. Replaces the rest of the string from position pos if len is not within the length of the rest of the string. Returns NULL if any argument is NULL.
 * 
 * mysql> SELECT INSERT('Quadratic', 3, 4, 'What');
 *         -> 'QuWhattic'
 * mysql> SELECT INSERT('Quadratic', -1, 4, 'What');
 *         -> 'Quadratic'
 * mysql> SELECT INSERT('Quadratic', 3, 100, 'What');
 *         -> 'QuWhat'
 * </pre>
 * 
 * @author mengshi.sunmengshi 2014年4月11日 下午4:17:42
 * @since 5.1.0
 */
public class Insert extends ScalarFunction {

    @Override
    public DataType getReturnType() {
        return DataType.StringType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "INSERT" };
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (ExecUtils.isNull(arg)) {
                return null;

            }
        }

        String str1 = DataType.StringType.convertFrom(args[0]);

        Integer pos = DataType.IntegerType.convertFrom(args[1]);

        Integer len = DataType.IntegerType.convertFrom(args[2]);

        String str2 = DataType.StringType.convertFrom(args[3]);

        if (pos <= 0 || pos > str1.length()) {
            return str1;
        }
        StringBuilder newStr = new StringBuilder();
        if (pos + len > str1.length()) {

            newStr.append(str1.substring(0, pos - 1));
            newStr.append(str2);

            return newStr.toString();
        } else {
            newStr.append(str1.substring(0, pos - 1));
            newStr.append(str2);
            newStr.append(str1.substring(pos + len - 1, str1.length()));

            return newStr.toString();
        }

    }

}
