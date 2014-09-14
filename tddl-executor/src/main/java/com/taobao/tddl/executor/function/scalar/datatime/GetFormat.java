package com.taobao.tddl.executor.function.scalar.datatime;

import java.util.HashMap;
import java.util.Map;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Returns a format string. This function is useful in combination with the
 * DATE_FORMAT() and the STR_TO_DATE() functions. The possible values for the
 * first and second arguments result in several possible format strings (for the
 * specifiers used, see the table in the DATE_FORMAT() function description).
 * ISO format refers to ISO 9075, not ISO 8601.
 * 
 * @author jianghang 2014-4-17 上午11:03:40
 * @since 5.0.7
 */
public class GetFormat extends ScalarFunction {

    private static Map<String, String> formats = new HashMap<String, String>();
    static {
        formats.put("DATE_USA", "%m.%d.%Y");
        formats.put("DATE_JIS", "%Y-%m-%d");
        formats.put("DATE_ISO", "%Y-%m-%d");
        formats.put("DATE_EUR", "%d.%m.%Y");
        formats.put("DATE_INTERNAL", "%Y%m%d");
        formats.put("TIMESTAMP_USA", "%Y-%m-%d %H.%i.%s");
        formats.put("TIMESTAMP_JIS", "%Y-%m-%d %H.%i.%s");
        formats.put("TIMESTAMP_ISO", "%Y-%m-%d %H.%i.%s");
        formats.put("TIMESTAMP_EUR", "%Y-%m-%d %H.%i.%s");
        formats.put("TIMESTAMP_INTERNAL", "%Y%m%d%H%i%s");
        formats.put("TIME_USA", "%h:%i:%s %p");
        formats.put("TIME_JIS", "%H:%i:%s");
        formats.put("TIME_ISO", "%H:%i:%s");
        formats.put("TIME_EUR", "%H:%i:%s");
        formats.put("TIME_INTERNAL", "%H%i%s");
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (ExecUtils.isNull(arg)) {
                return null;
            }
        }

        String type = DataType.StringType.convertFrom(args[0]);
        String format = DataType.StringType.convertFrom(args[1]);
        return formats.get(type + "_" + format);
    }

    @Override
    public DataType getReturnType() {
        return DataType.StringType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "GET_FORMAT" };
    }
}
