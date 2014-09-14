package com.taobao.tddl.executor.function.scalar.string;

import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * <pre>
 * BIT_LENGTH(str)
 * 
 * Returns the length of the string str in bits.
 * 
 * mysql> SELECT BIT_LENGTH('text');
 *         -> 32
 * </pre>
 * 
 * @author mengshi.sunmengshi 2014年4月11日 下午1:31:31
 * @since 5.1.0
 */
public class BitLength extends ScalarFunction {

    @Override
    public DataType getReturnType() {
        return DataType.LongType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "BIT_LENGTH" };
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
        return str.getBytes().length * 8;
    }
}
