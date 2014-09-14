package com.taobao.tddl.executor.function.scalar.string;

import java.io.ByteArrayOutputStream;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * <pre>
 * UNHEX(str)
 * 
 * For a string argument str, UNHEX(str) interprets each pair of characters in the argument as a hexadecimal number and converts it to the byte represented by the number. The return value is a binary string.
 * 
 * mysql> SELECT UNHEX('4D7953514C');
 *         -> 'MySQL'
 * mysql> SELECT 0x4D7953514C;
 *         -> 'MySQL'
 * mysql> SELECT UNHEX(HEX('string'));
 *         -> 'string'
 * mysql> SELECT HEX(UNHEX('1267'));
 *         -> '1267'
 * 
 * The characters in the argument string must be legal hexadecimal digits: '0' .. '9', 'A' .. 'F', 'a' .. 'f'. If the argument contains any nonhexadecimal digits, the result is NULL:
 * 
 * mysql> SELECT UNHEX('GG');
 * +-------------+
 * | UNHEX('GG') |
 * +-------------+
 * | NULL        |
 * +-------------+
 * 
 * A NULL result can occur if the argument to UNHEX() is a BINARY column, because values are padded with 0x00 bytes when stored but those bytes are not stripped on retrieval. For example, '41' is stored into a CHAR(3) column as '41 ' and retrieved as '41' (with the trailing pad space stripped), so UNHEX() for the column value returns 'A'. By contrast '41' is stored into a BINARY(3) column as '41\0' and retrieved as '41\0' (with the trailing pad 0x00 byte not stripped). '\0' is not a legal hexadecimal digit, so UNHEX() for the column value returns NULL.
 * 
 * For a numeric argument N, the inverse of HEX(N) is not performed by UNHEX(). Use CONV(HEX(N),16,10) instead. See the description of HEX().
 * </pre>
 * 
 * @author mengshi.sunmengshi 2014年4月15日 下午2:54:57
 * @since 5.1.0
 */
public class Unhex extends ScalarFunction {

    @Override
    public DataType getReturnType() {
        return DataType.StringType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "UNHEX" };
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (ExecUtils.isNull(args[0])) {
            return null;
        }

        String byteStr = DataType.StringType.convertFrom(args[0]).toUpperCase();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (int i = 0; i < byteStr.length(); i += 2) {

            int index0 = hexString.indexOf(byteStr.charAt(i));
            if (index0 < 0) {
                return null;
            }
            if (i + 1 < byteStr.length()) {
                int index1 = hexString.indexOf(byteStr.charAt(i + 1));

                if (index1 < 0) {
                    return null;
                }

                baos.write((index0 << 4 | index1));
            } else {
                baos.write((index0));
            }
        }
        return new String(baos.toByteArray());

    }

    private static String hexString = "0123456789ABCDEF";
}
