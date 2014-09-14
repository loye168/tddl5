package com.taobao.tddl.executor.function.scalar.string;

import java.math.BigInteger;

import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * <pre>
 * ORD(str)
 * 
 * If the leftmost character of the string str is a multi-byte character, returns the code for that character, calculated from the numeric values of its constituent bytes using this formula:
 * 
 *   (1st byte code)
 * + (2nd byte code * 256)
 * + (3rd byte code * 2562) ...
 * 
 * If the leftmost character is not a multi-byte character, ORD() returns the same value as the ASCII() function.
 * 
 * mysql> SELECT ORD('2');
 *         -> 50
 * 
 * </pre>
 * 
 * @author mengshi.sunmengshi 2014年4月15日 下午5:16:15
 * @since 5.1.0
 */
public class Ord extends ScalarFunction {

    @Override
    public DataType getReturnType() {
        return DataType.BigIntegerType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "ORD" };
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

        Character leftMost = str.charAt(0);

        byte[] bytes = leftMost.toString().getBytes();

        BigInteger value = BigInteger.valueOf(0l);
        for (int i = 0; i < bytes.length; i++) {
            value = value.add(BigInteger.valueOf(bytes[i] & 0xff).pow(i + 1));
        }

        return value;

    }

}
