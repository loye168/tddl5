package com.taobao.tddl.executor.function.scalar.datatime;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Returns the current date as a value in 'YYYY-MM-DD' or YYYYMMDD format,
 * depending on whether the function is used in a string or numeric context.
 * 
 * @author jianghang 2014-4-15 上午11:31:16
 * @since 5.0.7
 */
public class Curdate extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        DataType type = getReturnType();
        return type.convertFrom(new java.util.Date());
    }

    @Override
    public DataType getReturnType() {
        return DataType.DateType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "CURDATE", "CURRENT_DATE"};
    }
}
