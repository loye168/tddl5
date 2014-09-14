package com.taobao.tddl.executor.function.scalar.string;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * <pre>
 * ELT(N,str1,str2,str3,...)
 * 
 * ELT() returns the Nth element of the list of strings: str1 if N = 1, str2 if N = 2, and so on. Returns NULL if N is less than 1 or greater than the number of arguments. ELT() is the complement of FIELD().
 * 
 * mysql> SELECT ELT(1, 'ej', 'Heja', 'hej', 'foo');
 *         -> 'ej'
 * mysql> SELECT ELT(4, 'ej', 'Heja', 'hej', 'foo');
 *         -> 'foo'
 * </pre>
 * 
 * @author mengshi.sunmengshi 2014年4月11日 下午1:36:05
 * @since 5.1.0
 */
public class Elt extends ScalarFunction {

    @Override
    public DataType getReturnType() {
        return DataType.StringType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "ELT" };
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (ExecUtils.isNull(args[0])) {
            return null;
        }

        Integer index = DataType.IntegerType.convertFrom(args[0]);

        if (index < 1 || index > args.length - 1) {
            return null;
        }

        Object resEle = args[index];

        if (ExecUtils.isNull(resEle)) {
            return null;
        }

        return DataType.StringType.convertFrom(resEle);

    }

}
