package com.taobao.tddl.executor.function.scalar.string;

import java.math.BigInteger;
import java.util.LinkedList;
import java.util.List;

import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * <pre>
 * MAKE_SET(bits,str1,str2,...)
 * 
 * Returns a set value (a string containing substrings separated by “,” characters) consisting of the strings that have the corresponding bit in bits set. str1 corresponds to bit 0, str2 to bit 1, and so on. NULL values in str1, str2, ... are not appended to the result.
 * 
 * mysql> SELECT MAKE_SET(1,'a','b','c');
 *         -> 'a'
 * mysql> SELECT MAKE_SET(1 | 4,'hello','nice','world');
 *         -> 'hello,world'
 * mysql> SELECT MAKE_SET(1 | 4,'hello','nice',NULL,'world');
 *         -> 'hello'
 * mysql> SELECT MAKE_SET(0,'a','b','c');
 *         -> ''
 * 
 * </pre>
 * 
 * @author mengshi.sunmengshi 2014年4月15日 下午6:51:11
 * @since 5.1.0
 */
public class MakeSet extends ScalarFunction {

    @Override
    public DataType getReturnType() {
        return DataType.StringType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "MAKE_SET" };
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (ExecUtils.isNull(args[0])) {
            return null;
        }
        BigInteger bitsValue = DataType.BigIntegerType.convertFrom(args[0]);
        String bitsStringReverse = TStringUtil.reverse(bitsValue.toString(2));

        List<String> sets = new LinkedList();
        for (int i = 1; i < args.length && i - 1 < bitsStringReverse.length(); i++) {

            if (ExecUtils.isNull(args[i])) {
                continue;
            }

            if (bitsStringReverse.charAt(i - 1) == '1') {
                sets.add(DataType.StringType.convertFrom(args[i]));
            }
        }

        return TStringUtil.join(sets, ",");

    }

}
