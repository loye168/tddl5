package com.taobao.tddl.executor.function.scalar.string;

import java.math.BigInteger;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * <pre>
 *  Returns a string representation of the octal value of N, where N is a longlong (BIGINT) number. This is equivalent to CONV(N,10,8). Returns NULL if N is NULL.
 * 
 * mysql> SELECT OCT(12);
 *         -> '14'
 * 
 * </pre>
 * 
 * @author mengshi.sunmengshi 2014年4月11日 下午6:42:47
 * @since 5.1.0
 */
public class Oct extends ScalarFunction {

    @Override
    public DataType getReturnType() {
        return DataType.StringType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "OCT" };
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        Object arg = args[0];

        if (ExecUtils.isNull(arg)) {
            return null;
        }

        BigInteger longlong = DataType.BigIntegerType.convertFrom(arg);

        if (longlong == null) {
            return "0";
        }

        return longlong.toString(8);
    }

}
