package com.taobao.tddl.executor.function.scalar.datatime;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Extracts the time part of the time or datetime expression expr and returns it
 * as a string. This function is unsafe for statement-based replication. In
 * MySQL 5.6, a warning is logged if you use this function when binlog_format is
 * set to STATEMENT. (Bug #47995)
 * 
 * <pre>
 * mysql> SELECT TIME('2003-12-31 01:02:03');
 *         -> '01:02:03'
 * mysql> SELECT TIME('2003-12-31 01:02:03.000123');
 *         -> '01:02:03.000123'
 * </pre>
 * 
 * @author jianghang 2014-4-16 下午11:20:47
 * @since 5.0.7
 */
public class Time extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (ExecUtils.isNull(arg)) {
                return null;
            }
        }

        DataType type = getReturnType();
        return type.convertFrom(args[0]);
    }

    @Override
    public DataType getReturnType() {
        return DataType.TimeType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "TIME" };
    }
}
