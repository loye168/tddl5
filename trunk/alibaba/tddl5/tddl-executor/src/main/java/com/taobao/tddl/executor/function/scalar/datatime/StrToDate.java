package com.taobao.tddl.executor.function.scalar.datatime;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * @author jianghang 2014-4-17 上午12:49:10
 * @since 5.0.7
 */
public class StrToDate extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (ExecUtils.isNull(arg)) {
                return null;
            }
        }

        String value = DataType.StringType.convertFrom(args[0]);
        String format = DataType.StringType.convertFrom(args[1]);
        SimpleDateFormat dateFormat = new SimpleDateFormat(DateFormat.convertToJavaDataFormat(format));
        java.util.Date date = null;
        try {
            date = dateFormat.parse(value);
        } catch (ParseException e) {
            return null;
        }
        DataType type = getReturnType();
        return type.convertFrom(date);
    }

    @Override
    public DataType getReturnType() {
        return DataType.TimestampType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "STR_TO_DATE" };
    }
}
