package com.taobao.tddl.executor.function.scalar.datatime;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.datatype.DateType;

/**
 * If called with no argument, returns a Unix timestamp (seconds since
 * '1970-01-01 00:00:00' UTC) as an unsigned integer. If UNIX_TIMESTAMP() is
 * called with a date argument, it returns the value of the argument as seconds
 * since '1970-01-01 00:00:00' UTC. date may be a DATE string, a DATETIME
 * string, a TIMESTAMP, or a number in the format YYMMDD or YYYYMMDD. The server
 * interprets date as a value in the current time zone and converts it to an
 * internal value in UTC. Clients can set their time zone as described in
 * Section 10.6, “MySQL Server Time Zone Support”.
 * 
 * <pre>
 * mysql> SELECT UNIX_TIMESTAMP();
 *         -> 1196440210
 * mysql> SELECT UNIX_TIMESTAMP('2007-11-30 10:30:19');
 *         -> 1196440219
 * </pre>
 * 
 * @author jianghang 2014-4-16 下午10:30:02
 * @since 5.0.7
 */
public class UnixTimestamp extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (ExecUtils.isNull(arg)) {
                return null;
            }
        }

        if (args.length > 0) {
            java.sql.Timestamp timestamp = DateType.TimestampType.convertFrom(args[0]);
            return timestamp.getTime() / 1000;
        } else {
            return System.currentTimeMillis() / 1000;
        }
    }

    @Override
    public DataType getReturnType() {
        return DataType.LongType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "UNIX_TIMESTAMP" };
    }

}
