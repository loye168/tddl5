package com.taobao.tddl.executor.function.scalar.string;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * <pre>
 * SPACE(N)
 * 
 * Returns a string consisting of N space characters.
 * 
 * mysql> SELECT SPACE(6);
 *         -> '      '
 * </pre>
 * 
 * @author mengshi.sunmengshi 2014年4月11日 下午6:33:08
 * @since 5.1.0
 */
public class Space extends ScalarFunction {

    @Override
    public DataType getReturnType() {
        return DataType.StringType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "SPACE" };
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (ExecUtils.isNull(args[0])) {
            return null;
        }

        Integer len = DataType.IntegerType.convertFrom(args[0]);
        if (len <= 0) {
            return "";

        }
        StringBuilder sb = new StringBuilder(len);

        for (int i = 0; i < len; i++) {
            sb.append(' ');
        }
        return sb.toString();

    }
}
