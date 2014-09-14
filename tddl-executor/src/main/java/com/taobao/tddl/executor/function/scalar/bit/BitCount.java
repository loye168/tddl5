package com.taobao.tddl.executor.function.scalar.bit;

import java.math.BigInteger;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * BIT_COUNT(N)
 * 
 * <pre>
 * Returns the number of bits that are set in the argument N.
 * 
 * mysql> SELECT BIT_COUNT(29), BIT_COUNT(b'101010');
 *         -> 4, 3
 * </pre>
 * 
 * @author jianghang 2014-4-14 下午11:22:02
 * @since 5.0.7
 */
public class BitCount extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        return computeInner(args);
    }

    private Object computeInner(Object[] args) {
        DataType type = this.getReturnType();
        Object t = type.convertFrom(args[0]);
        if (t instanceof BigInteger) {
            return ((BigInteger) t).bitCount();
        } else {
            return Long.bitCount((Long) t);
        }
    }

    @Override
    public DataType getReturnType() {
        DataType type = getFirstArgType();
        if (type == DataType.BigIntegerType || type == DataType.BigDecimalType) {
            return DataType.BigIntegerType;
        } else {
            return DataType.LongType;
        }
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "BIT_COUNT" };
    }

}
