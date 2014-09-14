package com.taobao.tddl.executor.function.scalar.math;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Returns the number X, truncated to D decimal places. If D is 0, the result
 * has no decimal point or fractional part. D can be negative to cause D digits
 * left of the decimal point of the value X to become zero.
 * 
 * <pre>
 * mysql> SELECT TRUNCATE(1.223,1);
 *         -> 1.2
 * mysql> SELECT TRUNCATE(1.999,1);
 *         -> 1.9
 * mysql> SELECT TRUNCATE(1.999,0);
 *         -> 1
 * mysql> SELECT TRUNCATE(-1.999,1);
 *         -> -1.9
 * mysql> SELECT TRUNCATE(122,-2);
 *        -> 100
 * mysql> SELECT TRUNCATE(10.28*100,0);
 *        -> 1028
 * </pre>
 * 
 * All numbers are rounded toward zero.
 * 
 * @author jianghang 2014-4-14 下午10:52:31
 * @since 5.0.7
 */
public class Truncate extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        DataType type = getReturnType();
        if (ExecUtils.isNull(args[0])) {
            return null;
        }

        BigDecimal d = DataType.BigDecimalType.convertFrom(args[0]);
        int x = 0;
        if (args.length >= 2 && !ExecUtils.isNull(args[1])) {
            x = DataType.IntegerType.convertFrom(args[1]);
        }

        if (x >= 0) {
            int precision = d.precision() - d.scale() + x;
            if (precision < 0) {
                d = BigDecimal.ZERO;
            } else {
                d = d.round(new MathContext(precision, RoundingMode.DOWN));
            }
        } else {
            x = Math.abs(x);
            d = d.movePointLeft(x).setScale(0, RoundingMode.DOWN).multiply(new BigDecimal(10).pow(x));
        }
        return type.convertFrom(d);
    }

    @Override
    public DataType getReturnType() {
        return getFirstArgType();
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "TRUNCATE" };
    }

    public static void main(String args[]) {
        BigDecimal d = new BigDecimal("121.99");
        System.out.println(d.round(new MathContext(1, RoundingMode.DOWN)));
        int x = 1;
        System.out.println(d.movePointLeft(x).setScale(0, RoundingMode.DOWN).multiply(new BigDecimal(10).pow(x)));
    }
}
