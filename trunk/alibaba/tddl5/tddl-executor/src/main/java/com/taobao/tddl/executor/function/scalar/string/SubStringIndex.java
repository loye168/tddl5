package com.taobao.tddl.executor.function.scalar.string;

import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * <pre>
 * SUBSTRING_INDEX(str,delim,count)
 * 
 * Returns the substring from string str before count occurrences of the delimiter delim. If count is positive, everything to the left of the final delimiter (counting from the left) is returned. If count is negative, everything to the right of the final delimiter (counting from the right) is returned. SUBSTRING_INDEX() performs a case-sensitive match when searching for delim.
 * 
 * mysql> SELECT SUBSTRING_INDEX('www.mysql.com', '.', 2);
 *         -> 'www.mysql'
 * mysql> SELECT SUBSTRING_INDEX('www.mysql.com', '.', -2);
 *         -> 'mysql.com'
 * 
 * </pre>
 * 
 * @author mengshi.sunmengshi 2014年4月15日 下午2:02:15
 * @since 5.1.0
 */
public class SubStringIndex extends ScalarFunction {

    @Override
    public DataType getReturnType() {
        return DataType.StringType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "SUBSTRING_INDEX" };
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (ExecUtils.isNull(args[0])) {
            return null;
        }

        String str = DataType.StringType.convertFrom(args[0]);
        String delim = DataType.StringType.convertFrom(args[1]);
        Integer count = DataType.IntegerType.convertFrom(args[2]);
        if (count == 0) {
            return "";
        } else if (count > 0) {
            Integer len = TStringUtil.ordinalIndexOf(str, delim, count);
            if (len == -1) {
                return str;
            }
            return TStringUtil.substring(str, 0, len);
        } else {
            count = -count;
            Integer pos = TStringUtil.lastOrdinalIndexOf(str, delim, count);
            return TStringUtil.substring(str, pos + 1);
        }
    }

}
