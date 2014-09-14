package com.taobao.tddl.executor.function.scalar.cast;

import java.io.UnsupportedEncodingException;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * @author jianghang 2014-7-1 下午1:55:17
 * @since 5.1.7
 */
public class Convert extends ScalarFunction {

    @Override
    protected Object compute(Object[] args, ExecutionContext ec) {
        if (ExecUtils.isNull(args[0])) {
            return null;
        }

        String arg = DataType.StringType.convertFrom(args[0]);
        String charset = DataType.StringType.convertFrom(args[1]);
        try {
            return new String(arg.getBytes(), charset);
        } catch (UnsupportedEncodingException e) {
            return arg;
        }
    }

    @Override
    public DataType getReturnType() {
        return DataType.StringType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "CONVERT" };
    }

}
