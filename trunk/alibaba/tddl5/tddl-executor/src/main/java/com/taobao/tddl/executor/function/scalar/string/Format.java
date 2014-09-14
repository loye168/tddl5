package com.taobao.tddl.executor.function.scalar.string;

import java.math.BigDecimal;
import java.text.NumberFormat;
import java.util.Locale;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * <pre>
 * FORMAT(X,D)
 * 
 * Formats the number X to a format like '#,###,###.##', rounded to D decimal places, and returns the result as a string. If D is 0, the result has no decimal point or fractional part.
 * 
 * mysql> SELECT FORMAT(12332.123456, 4);
 *         -> '12,332.1235'
 * mysql> SELECT FORMAT(12332.1,4);
 *         -> '12,332.1000'
 * mysql> SELECT FORMAT(12332.2,0);
 *         -> '12,332'
 * 
 * 
 * </pre>
 * 
 * @author mengshi.sunmengshi 2014年4月15日 下午5:50:13
 * @since 5.1.0
 */
public class Format extends ScalarFunction {

    @Override
    public DataType getReturnType() {
        return DataType.StringType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "FORMAT" };
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (ExecUtils.isNull(arg)) {
                return null;
            }
        }

        BigDecimal X = DataType.BigDecimalType.convertFrom(args[0]);

        Integer D = DataType.IntegerType.convertFrom(args[1]);

        NumberFormat nf = NumberFormat.getInstance(Locale.US);
        nf.setGroupingUsed(true);
        nf.setMaximumFractionDigits(D);
        nf.setMinimumFractionDigits(D);
        return nf.format(X);

    }

}
