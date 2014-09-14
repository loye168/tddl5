package com.taobao.tddl.executor.function.scalar.cast;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.List;

import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.exception.code.ErrorCode;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * 对应mysql的cast函数
 * 
 * @author jianghang 2014-7-1 上午11:08:14
 * @since 5.1.7
 */
public class Cast extends ScalarFunction {

    public static final BigInteger BIGINT_MAX_VALUE = new BigInteger("18446744073709551616");
    public static final BigInteger LONG_MAX_VALUE   = new BigInteger("9223372036854775807");

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (ExecUtils.isNull(args[0])) {
            return null;
        }

        CastType castType = getType();
        Object obj = castType.type.convertFrom(args[0]);
        if (castType.type == DataType.BytesType) {
            if (castType.type1 != null) {
                // 如果超过byte数量，前面用0x00填充
                int length = ((byte[]) obj).length;
                byte[] bytes = new byte[castType.type1];
                int min = length > castType.type1 ? castType.type1 : length;
                System.arraycopy(obj, 0, bytes, 0, min);
                for (int i = min; i < castType.type1; i++) {
                    bytes[i] = 0;
                }
                return bytes;
            }
        } else if (castType.type == DataType.StringType) {
            if (castType.type1 != null) {
                int length = ((String) obj).length();
                int min = length > castType.type1 ? castType.type1 : length;
                return ((String) obj).substring(0, min);
            }
        } else if (castType.type == DataType.BigDecimalType) {
            if (castType.type1 != null && castType.type2 != null && castType.type1 > 0) {
                return ((BigDecimal) obj).setScale(castType.type2, BigDecimal.ROUND_HALF_UP)
                    .round(new MathContext(castType.type1));
            } else {
                return ((BigDecimal) obj).setScale(0, BigDecimal.ROUND_HALF_UP);
            }
        } else if (castType.type == DataType.BigIntegerType) {
            if (!castType.signed && BigInteger.ZERO.compareTo((BigInteger) obj) > 0) {
                return ((BigInteger) obj).add(BIGINT_MAX_VALUE);
            } else if (castType.signed && LONG_MAX_VALUE.compareTo((BigInteger) obj) < 0) {
                return ((BigInteger) obj).subtract(BIGINT_MAX_VALUE);
            }
        }

        return obj;
    }

    @Override
    public DataType getReturnType() {
        CastType castType = getType();
        return castType.type;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "CAST" };
    }

    class CastType {

        DataType type;
        Integer  type1;
        Integer  type2;
        boolean  signed = true;
    }

    /**
     * <pre>
     * BINARY[(N)]
     * CHAR[(N)]
     * DATE
     * DATETIME
     * DECIMAL[(M[,D])]
     * SIGNED [INTEGER]
     * TIME
     * UNSIGNED [INTEGER]
     * </pre>
     */
    protected CastType getType() {
        CastType castType = new CastType();
        List args = function.getArgs();
        String type = DataType.StringType.convertFrom(args.get(1));
        if (type.equalsIgnoreCase("BINARY")) {
            if (args.size() > 2) {
                castType.type1 = DataType.IntegerType.convertFrom(args.get(2));
            }
            castType.type = DataType.BytesType;
        } else if (type.equalsIgnoreCase("CHAR")) {
            if (args.size() > 2) {
                castType.type1 = DataType.IntegerType.convertFrom(args.get(2));
            }
            castType.type = DataType.StringType;
        } else if (type.equalsIgnoreCase("DATE")) {
            castType.type = DataType.DateType;
        } else if (type.equalsIgnoreCase("DATETIME")) {
            castType.type = DataType.DatetimeType;
        } else if (type.equalsIgnoreCase("TIME")) {
            castType.type = DataType.TimeType;
        } else if (type.equalsIgnoreCase("DECIMAL")) {
            castType.type = DataType.BigDecimalType;
            if (args.size() > 2) {
                castType.type1 = DataType.IntegerType.convertFrom(args.get(2));
            }
            if (args.size() > 3) {
                castType.type2 = DataType.IntegerType.convertFrom(args.get(3));
            }

            if (castType.type1 != null) {
                if (castType.type2 == null) {
                    castType.type2 = 0;
                }

                if (castType.type1 < castType.type2) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        "For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column '')");
                }
            }

        } else if (type.equalsIgnoreCase("SIGNED")) {
            castType.type = DataType.BigIntegerType;
            castType.signed = true;
        } else if (type.equalsIgnoreCase("UNSIGNED")) {
            castType.type = DataType.BigIntegerType;
            castType.signed = false;
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT, "cast type:" + type);
        }

        return castType;
    }
}
