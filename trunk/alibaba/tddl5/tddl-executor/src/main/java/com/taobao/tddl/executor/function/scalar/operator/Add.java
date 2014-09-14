package com.taobao.tddl.executor.function.scalar.operator;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.Calculator;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.datatype.IntervalType;

/**
 * @since 5.0.0
 */
public class Add extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        DataType type = this.getReturnType();
        Calculator cal = type.getCalculator();
        // 如果加法中有出现IntervalType类型，转到时间函数的加法处理
        for (Object arg : args) {
            if (arg instanceof IntervalType) {
                cal = DataType.TimestampType.getCalculator();
            }
        }

        if (type.getCalculator() != cal) {
            return type.convertFrom(cal.add(args[0], args[1]));
        } else {
            return cal.add(args[0], args[1]);
        }
    }

    @Override
    public DataType getReturnType() {
        Object arg0 = function.getArgs().get(0);
        Object arg1 = function.getArgs().get(1);
        // 比如出现"2012-12-10" + INTERVAL 1 DAY
        if (getArgType(arg0) instanceof IntervalType) {
            return getArgType(arg1);
        } else if (getArgType(arg1) instanceof IntervalType) {
            return getArgType(arg0);
        }

        return getMixedMathType();
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "ADD", "+" };
    }

}
