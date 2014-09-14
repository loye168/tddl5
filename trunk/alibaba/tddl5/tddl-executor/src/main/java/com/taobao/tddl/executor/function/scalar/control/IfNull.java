package com.taobao.tddl.executor.function.scalar.control;

import java.util.List;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * @since 5.0.0
 */
public class IfNull extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args == null) {
            return null;
        }

        DataType type = getReturnType();
        if (ExecUtils.isNull(args[0])) {
            return type.convertFrom(args[1]);
        } else {
            return type.convertFrom(args[0]);
        }

    }

    @Override
    public DataType getReturnType() {
        List args = function.getArgs();
        Object arg0 = args.get(0);
        Object arg1 = args.get(1);
        if (ExecUtils.isNull(arg0)) {
            return getArgType(arg1);
        } else if (ExecUtils.isNull(arg1)) {
            return getArgType(arg0);
        } else {
            return getMixedStringType();
        }
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "IFNULL" };
    }
}
