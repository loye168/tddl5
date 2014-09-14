package com.taobao.tddl.executor.function.scalar.math;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Rounds the argument X to D decimal places. The rounding algorithm depends on
 * the data type of X. D defaults to 0 if not specified. D can be negative to
 * cause D digits left of the decimal point of the value X to become zero.
 * 
 * <pre>
 * mysql> SELECT ROUND(-1.23);
 *         -> -1
 * mysql> SELECT ROUND(-1.58);
 *         -> -2
 * mysql> SELECT ROUND(1.58);
 *         -> 2
 * mysql> SELECT ROUND(1.298, 1);
 *         -> 1.3
 * mysql> SELECT ROUND(1.298, 0);
 *         -> 1
 * mysql> SELECT ROUND(23.298, -1);
 *         -> 20
 * </pre>
 * 
 * see. http://dev.mysql.com/doc/refman/5.6/en/mathematical-functions.html#
 * function_round
 * 
 * @author jianghang 2014-4-14 下午10:47:45
 * @since 5.0.7
 */
public class Round extends ScalarFunction {

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
                d = d.round(new MathContext(precision, RoundingMode.HALF_UP));
            }
        } else {
            x = Math.abs(x);
            d = d.movePointLeft(x).setScale(0, RoundingMode.HALF_UP).multiply(new BigDecimal(10).pow(x));
        }
        return type.convertFrom(d);
    }

    @Override
    public DataType getReturnType() {
        return getFirstArgType();
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "ROUND" };
    }

}
