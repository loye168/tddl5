package com.taobao.tddl.executor.function.scalar.datatime;

import java.text.ParseException;
import java.util.Calendar;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.exception.FunctionException;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

/**
 * Adds N months to period P (in the format YYMM or YYYYMM). Returns a value in
 * the format YYYYMM. Note that the period argument P is not a date value.
 * 
 * <pre>
 * mysql> SELECT PERIOD_ADD(200801,2);
 *         -> 200803
 * </pre>
 * 
 * @author jianghang 2014-4-17 上午10:10:23
 * @since 5.0.7
 */
public class PeroidAdd extends ScalarFunction {

    public static final String[] DATE_FORMATS = new String[] { "yyyyMM", "yyMM", "yyyy-MM", "yy-MM" };

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (ExecUtils.isNull(arg)) {
                return null;
            }
        }

        String value = DataType.StringType.convertFrom(args[0]);
        Integer peroid = DataType.IntegerType.convertFrom(args[1]);

        try {
            java.util.Date date = OptimizerUtils.parseDate(value, DATE_FORMATS);
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            cal.add(Calendar.MONTH, peroid);

            return cal.get(Calendar.YEAR) * 100 + cal.get(Calendar.MONTH) + 1;
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
        return new String[] { "PERIOD_ADD" };
    }
}
