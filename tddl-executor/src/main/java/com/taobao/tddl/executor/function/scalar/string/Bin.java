package com.taobao.tddl.executor.function.scalar.string;

import java.math.BigInteger;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * <pre>
 * BIN(N)
 * 
 * Returns a string representation of the binary value of N, where N is a longlong (BIGINT) number. This is equivalent to CONV(N,10,2). Returns NULL if N is NULL.
 * 
 * mysql> SELECT BIN(12);
 *         -> '1100'
 * </pre>
 * 
 * @author mengshi.sunmengshi 2014年4月11日 下午1:29:30
 * @since 5.1.0
 */
public class Bin extends ScalarFunction {

    @Override
    public DataType getReturnType() {
        return DataType.StringType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "BIN" };
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

        return longlong.toString(2);
    }

    public static void main(String args[]) {
        System.out.println((new BigInteger("128").toString(2)));
    }
}
