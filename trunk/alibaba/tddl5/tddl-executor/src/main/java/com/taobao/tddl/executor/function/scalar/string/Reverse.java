package com.taobao.tddl.executor.function.scalar.string;

import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * <pre>
 * REVERSE(str)
 * 
 * Returns the string str with the order of the characters reversed.
 * 
 * mysql> SELECT REVERSE('abc');
 *         -> 'cba'
 * </pre>
 * 
 * @author mengshi.sunmengshi 2014年4月11日 下午6:37:57
 * @since 5.1.0
 */
public class Reverse extends ScalarFunction {

    @Override
    public DataType getReturnType() {
        return DataType.StringType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "REVERSE" };
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (ExecUtils.isNull(args[0])) {
            return null;
        }

        String str = DataType.StringType.convertFrom(args[0]);

        return TStringUtil.reverse(str);

    }
}
