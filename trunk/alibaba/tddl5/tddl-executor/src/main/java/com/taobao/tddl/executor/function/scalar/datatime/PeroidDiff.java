package com.taobao.tddl.executor.function.scalar.datatime;

import java.text.ParseException;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.exception.FunctionException;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

/**
 * Returns the number of months between periods P1 and P2. P1 and P2 should be
 * in the format YYMM or YYYYMM. Note that the period arguments P1 and P2 are
 * not date values.
 * 
 * <pre>
 * mysql> SELECT PERIOD_DIFF(200802,200703);
 *         -> 11
 * </pre>
 * 
 * @author jianghang 2014-4-17 上午10:10:23
 * @since 5.0.7
 */
public class PeroidDiff extends ScalarFunction {

    public static final String[] DATE_FORMATS = new String[] { "yyyyMM", "yyMM", "yyyy-MM", "yy-MM" };

    @SuppressWarnings("deprecation")
    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (ExecUtils.isNull(arg)) {
                return null;
            }
        }

        String value1 = DataType.StringType.convertFrom(args[0]);
        String value2 = DataType.StringType.convertFrom(args[1]);

        try {
            java.util.Date date1 = OptimizerUtils.parseDate(value1, DATE_FORMATS);
            java.util.Date date2 = OptimizerUtils.parseDate(value2, DATE_FORMATS);
            return (date1.getYear() - date2.getYear()) * 12 + (date1.getMonth() - date2.getMonth());
        } catch (ParseException e) {
            throw new FunctionException(e);
        }

    }

    @Override
    public DataType getReturnType() {
        return DataType.IntegerType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "PERIOD_DIFF" };
    }
}
