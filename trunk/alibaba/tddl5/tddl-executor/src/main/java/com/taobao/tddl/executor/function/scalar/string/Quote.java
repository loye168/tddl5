package com.taobao.tddl.executor.function.scalar.string;

import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * <pre>
 * QUOTE(str)
 * 
 * Quotes a string to produce a result that can be used as a properly escaped data value in an SQL statement. The string is returned enclosed by single quotation marks and with each instance of backslash (“\”), single quote (“'”), ASCII NUL, and Control+Z preceded by a backslash. If the argument is NULL, the return value is the word “NULL” without enclosing single quotation marks.
 * 
 * mysql> SELECT QUOTE('Don\'t!');
 *         -> 'Don\'t!'
 * mysql> SELECT QUOTE(NULL);
 *         -> NULL
 * 
 * </pre>
 * 
 * @author mengshi.sunmengshi 2014年4月15日 下午4:52:06
 * @since 5.1.0
 */
public class Quote extends ScalarFunction {

    @Override
    public DataType getReturnType() {
        return DataType.StringType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "QUOTE" };
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (ExecUtils.isNull(args[0])) {
            return "NULL";
        }

        String str = DataType.StringType.convertFrom(args[0]);
        StringBuilder sb = new StringBuilder();
        sb.append("'").append(TStringUtil.replace(str, "'", "\\'")).append("'");
        return sb.toString();
    }

}
