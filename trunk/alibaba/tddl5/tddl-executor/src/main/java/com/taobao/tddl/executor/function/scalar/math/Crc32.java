package com.taobao.tddl.executor.function.scalar.math;

import java.util.zip.CRC32;
import java.util.zip.Checksum;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * Computes a cyclic redundancy check value and returns a 32-bit unsigned value.
 * The result is NULL if the argument is NULL. The argument is expected to be a
 * string and (if possible) is treated as one if it is not.
 * 
 * <pre>
 * mysql> SELECT CRC32('MySQL');
 *         -> 3259397556
 * mysql> SELECT CRC32('mysql');
 *         -> 2501908538
 * </pre>
 * 
 * @author jianghang 2014-4-14 下午10:16:56
 * @since 5.0.7
 */
public class Crc32 extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        DataType type = getReturnType();
        if (ExecUtils.isNull(args[0])) {
            return null;
        }

        String d = DataType.StringType.convertFrom(args[0]);
        // get bytes from string
        byte bytes[] = d.getBytes();
        Checksum checksum = new CRC32();
        // update the current checksum with the specified array of bytes
        checksum.update(bytes, 0, bytes.length);
        // get the current checksum value
        return type.convertFrom(checksum.getValue());
    }

    @Override
    public DataType getReturnType() {
        return DataType.LongType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "CRC32" };
    }

}
