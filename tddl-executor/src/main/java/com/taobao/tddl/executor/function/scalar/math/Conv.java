package com.taobao.tddl.executor.function.scalar.math;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Converts numbers between different number bases. Returns a string
 * representation of the number N, converted from base from_base to base
 * to_base. Returns NULL if any argument is NULL. The argument N is interpreted
 * as an integer, but may be specified as an integer or a string. The minimum
 * base is 2 and the maximum base is 36. If to_base is a negative number, N is
 * regarded as a signed number. Otherwise, N is treated as unsigned. CONV()
 * works with 64-bit precision.
 * 
 * <pre>
 * mysql> SELECT CONV('a',16,2);
 *         -> '1010'
 * mysql> SELECT CONV('6E',18,8);
 *         -> '172'
 * mysql> SELECT CONV(-17,10,-18);
 *         -> '-H'
 * mysql> SELECT CONV(10+'10'+'10'+0xa,10,10);
 *         -> '40'
 * </pre>
 * 
 * @author jianghang 2014-4-14 下午9:52:58
 * @since 5.0.7
 */
public class Conv extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (ExecUtils.isNull(arg)) {
                return null;
            }
        }

        String n = DataType.StringType.convertFrom(args[0]);
        Integer f = DataType.IntegerType.convertFrom(args[1]);
        Integer t = DataType.IntegerType.convertFrom(args[2]);
        Object result;
        Long d = Long.valueOf(n, f);
        if (t < 0) {
            result = Long.toString(d, Math.abs(t)).toUpperCase();
            if (d >= 0) {
                result = "-" + result;
            }
        } else {
            result = Long.toString(d, t).toUpperCase();
        }

        return result;
    }

    @Override
    public DataType getReturnType() {
        return DataType.StringType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "CONV" };
    }

}
