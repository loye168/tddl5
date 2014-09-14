package com.taobao.tddl.executor.function.scalar.string;

import java.math.BigInteger;

import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * <pre>
 *  EXPORT_SET(bits,on,off[,separator[,number_of_bits]])
 * 
 * Returns a string such that for every bit set in the value bits, you get an on string and for every bit not set in the value, you get an off string. Bits in bits are examined from right to left (from low-order to high-order bits). Strings are added to the result from left to right, separated by the separator string (the default being the comma character “,”). The number of bits examined is given by number_of_bits, which has a default of 64 if not specified. number_of_bits is silently clipped to 64 if larger than 64. It is treated as an unsigned integer, so a value of –1 is effectively the same as 64.
 * 
 * mysql> SELECT EXPORT_SET(5,'Y','N',',',4);
 *         -> 'Y,N,Y,N'
 * mysql> SELECT EXPORT_SET(6,'1','0',',',10);
 *         -> '0,1,1,0,0,0,0,0,0,0'
 * </pre>
 * 
 * @author mengshi.sunmengshi 2014年4月11日 下午1:50:22
 * @since 5.1.0
 */
public class ExportSet extends ScalarFunction {

    @Override
    public DataType getReturnType() {
        return DataType.StringType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "EXPORT_SET" };
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (ExecUtils.isNull(arg)) {
                return null;
            }
        }
        BigInteger bitsValue = DataType.BigIntegerType.convertFrom(args[0]);
        String bitsStringReverse = TStringUtil.reverse(bitsValue.toString(2));

        String on = DataType.StringType.convertFrom(args[1]);
        String off = DataType.StringType.convertFrom(args[2]);
        String sep = ",";
        if (args.length >= 4) {
            sep = DataType.StringType.convertFrom(args[3]);
        }

        Integer number_of_bits = 64;

        if (args.length >= 5) {
            number_of_bits = DataType.IntegerType.convertFrom(args[4]);
        }

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < number_of_bits; i++) {
            if (i != 0) {
                sb.append(sep);
            }

            if (i < bitsStringReverse.length()) {
                if (bitsStringReverse.charAt(i) == '0') {
                    sb.append(off);
                } else {
                    sb.append(on);
                }
            } else {
                sb.append(off);
            }

        }

        return sb.toString();

    }
}
