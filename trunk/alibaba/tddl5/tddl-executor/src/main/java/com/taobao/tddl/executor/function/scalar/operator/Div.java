package com.taobao.tddl.executor.function.scalar.operator;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * 对应mysql的DIV函数，区别于/的出发
 * 
 * <pre>
 * Integer division. Similar to FLOOR(), but is safe with BIGINT values.
 * In MySQL 5.6, if either operand has a noninteger type, 
 * the operands are converted to DECIMAL and divided using DECIMAL arithmetic before converting the result to BIGINT. 
 * If the result exceeds BIGINT range, an error occurs.
 * 
 * mysql> SELECT 5 DIV 2;
 *         -> 2
 * </pre>
 * 
 * @author jianghang 2014-2-13 上午11:55:29
 * @since 5.0.0
 */
public class Div extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        DataType type = this.getReturnType();
        return type.getCalculator().divide(args[0], args[1]);
    }

    @Override
    public DataType getReturnType() {
        return DataType.BigDecimalType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "/" };
    }
}
