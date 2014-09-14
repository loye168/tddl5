package com.taobao.tddl.executor.function.scalar.string;

import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * <pre>
 * CHAR_LENGTH(str)
 * 
 * Returns the length of the string str, measured in characters. A multi-byte character counts as a single character. This means that for a string containing five 2-byte characters, LENGTH() returns 10, whereas CHAR_LENGTH() returns 5.
 * </pre>
 * 
 * @author mengshi.sunmengshi 2014年4月11日 下午5:18:53
 * @since 5.1.0
 */
public class CharLength extends ScalarFunction {

    @Override
    public DataType getReturnType() {
        return DataType.IntegerType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "CHARACTER_LENGTH", "CHAR_LENGTH" };
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        Object arg = args[0];

        if (ExecUtils.isNull(arg)) {
            return null;
        }

        String str = DataType.StringType.convertFrom(arg);

        if (TStringUtil.isEmpty(str)) {
            return 0;
        }

        return str.length();
    }

}
