package com.taobao.tddl.executor.function.scalar.string;

import java.math.BigInteger;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.datatype.DataTypeUtil;

/**
 * <pre>
 * HEX(str), HEX(N)
 * 
 * For a string argument str, HEX() returns a hexadecimal string representation of str where each byte of each character in str is converted to two hexadecimal digits. (Multi-byte characters therefore become more than two digits.) The inverse of this operation is performed by the UNHEX() function.
 * 
 * For a numeric argument N, HEX() returns a hexadecimal string representation of the value of N treated as a longlong (BIGINT) number. This is equivalent to CONV(N,10,16). The inverse of this operation is performed by CONV(HEX(N),16,10).
 * 
 * mysql> SELECT 0x616263, HEX('abc'), UNHEX(HEX('abc'));
 *         -> 'abc', 616263, 'abc'
 * mysql> SELECT HEX(255), CONV(HEX(255),16,10);
 *         -> 'FF', 255
 * </pre>
 * 
 * @author mengshi.sunmengshi 2014年4月11日 下午3:59:21
 * @since 5.1.0
 */
public class Hex extends ScalarFunction {

    @Override
    public DataType getReturnType() {
        return DataType.StringType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "HEX" };
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (ExecUtils.isNull(args[0])) {
            return null;
        }

        DataType type = DataTypeUtil.getTypeOfObject(args[0]);
        if (type == DataType.StringType) {
            String strVal = DataType.StringType.convertFrom(args[0]);
            StringBuilder sb = new StringBuilder();
            for (Byte b : strVal.getBytes()) {
                sb.append(Integer.toHexString(b & 0xff));
            }
            return sb.toString();
        } else {
            BigInteger intVal = DataType.BigIntegerType.convertFrom(args[0]);
            return intVal.toString(16).toUpperCase();
        }
    }

}
