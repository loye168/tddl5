package com.taobao.tddl.executor.function.scalar.string;

import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * <pre>
 * INSTR(str,substr)
 *  Returns the position of the first occurrence of substring substr in string str. This is the same as the two-argument form of LOCATE(), except that the order of the arguments is reversed.
 * 
 * mysql> SELECT INSTR('foobarbar', 'bar');
 *         -> 4
 * mysql> SELECT INSTR('xbar', 'foobar');
 *         -> 0
 * </pre>
 * 
 * @author mengshi.sunmengshi 2014年4月11日 下午4:56:34
 * @since 5.1.0
 */
public class Instr extends ScalarFunction {

    @Override
    public DataType getReturnType() {
        return DataType.IntegerType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "INSTR" };
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (ExecUtils.isNull(arg)) {
                return null;
            }
        }

        String str = DataType.StringType.convertFrom(args[0]);
        String subStr = DataType.StringType.convertFrom(args[1]);

        return TStringUtil.indexOf(str, subStr) + 1;

    }

}
