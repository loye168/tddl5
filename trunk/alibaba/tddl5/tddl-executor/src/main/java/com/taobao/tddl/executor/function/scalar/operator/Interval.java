package com.taobao.tddl.executor.function.scalar.operator;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * INTERVAL(N,N1,N2,N3,...) <br/>
 * Returns 0 if N < N1, 1 if N < N2 and so on or -1 if N is NULL. All arguments
 * are treated as integers. It is required that N1 < N2 < N3 < ... < Nn for this
 * function to work correctly. This is because a binary search is used (very
 * fast).
 * 
 * <pre>
 * mysql> SELECT INTERVAL(23, 1, 15, 17, 30, 44, 200);
 *         -> 3
 * mysql> SELECT INTERVAL(10, 1, 10, 100, 1000);
 *         -> 2
 * mysql> SELECT INTERVAL(22, 23, 30, 44, 200);
 *         -> 0
 * </pre>
 * 
 * @author jianghang 2014-4-21 下午4:55:16
 * @since 5.0.7
 */
public class Interval extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (ExecUtils.isNull(args[0])) {
            return -1;
        }

        Long n = DataType.LongType.convertFrom(args[0]);
        for (int i = 1; i < args.length; i++) {
            if (!ExecUtils.isNull(args[i])) {
                Long p = DataType.LongType.convertFrom(args[i]);
                if (n < p) {
                    return i - 1;
                }
            }
        }

        return args.length - 1;
    }

    @Override
    public DataType getReturnType() {
        return DataType.LongType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "INTERVAL" };
    }

}
