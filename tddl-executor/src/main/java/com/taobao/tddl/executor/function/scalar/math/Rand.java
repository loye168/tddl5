package com.taobao.tddl.executor.function.scalar.math;

import java.util.Random;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

public class Rand extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length > 0 && ExecUtils.isNull(args[0])) {
            Long d = DataType.LongType.convertFrom(args[0]);
            Random rand = new Random(d);
            return rand.nextDouble();
        } else {
            Random rand = new Random();
            return rand.nextDouble();
        }
    }

    @Override
    public DataType getReturnType() {
        return DataType.DoubleType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "RAND" };
    }

}
