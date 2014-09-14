package com.taobao.tddl.executor.function.scalar.string;

import java.util.ArrayList;
import java.util.List;

import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * <pre>
 * CONCAT_WS(separator,str1,str2,...)
 * 
 * CONCAT_WS() stands for Concatenate With Separator and is a special form of CONCAT(). The first argument is the separator for the rest of the arguments. The separator is added between the strings to be concatenated. The separator can be a string, as can the rest of the arguments. If the separator is NULL, the result is NULL.
 * 
 * mysql> SELECT CONCAT_WS(',','First name','Second name','Last Name');
 *         -> 'First name,Second name,Last Name'
 * mysql> SELECT CONCAT_WS(',','First name',NULL,'Last Name');
 *         -> 'First name,Last Name'
 * 
 * CONCAT_WS() does not skip empty strings. However, it does skip any NULL values after the separator argument.
 * </pre>
 * 
 * @author mengshi.sunmengshi 2014年4月11日 下午1:26:56
 * @since 5.1.0
 */
public class ConcatWS extends ScalarFunction {

    @Override
    public DataType getReturnType() {
        return DataType.StringType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "CONCAT_WS" };
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {

        Object arg0 = args[0];

        if (ExecUtils.isNull(arg0)) {
            return null;
        }

        String sep = DataType.StringType.convertFrom(arg0);

        List<String> strs = new ArrayList(args.length - 1);

        for (int i = 1; i < args.length; i++) {
            if (ExecUtils.isNull(args[i])) {
                continue;
            }
            String argStr = DataType.StringType.convertFrom(args[i]);
            strs.add(argStr);
        }

        return TStringUtil.join(strs, sep);

    }
}
