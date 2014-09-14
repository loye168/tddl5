package com.taobao.tddl.executor.function.scalar.control;

import java.util.List;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * <pre>
 * The first version returns the result where value=compare_value. The second version returns the result for the first condition that is true. If there was no matching result value, the result after ELSE is returned, or NULL if there is no ELSE part.
 * 
 * mysql> SELECT CASE 1 WHEN 1 THEN 'one'
 *     ->     WHEN 2 THEN 'two' ELSE 'more' END;
 *         -> 'one'
 * mysql> SELECT CASE WHEN 1>0 THEN 'true' ELSE 'false' END;
 *         -> 'true'
 * mysql> SELECT CASE BINARY 'B'
 *     ->     WHEN 'a' THEN 1 WHEN 'b' THEN 2 END;
 *         -> NULL
 * </pre>
 * 
 * The return type of a CASE expression is the compatible aggregated type of all
 * return values, but also depends on the context in which it is used. If used
 * in a string context, the result is returned as a string. If used in a numeric
 * context, the result is returned as a decimal, real, or integer value.
 * 
 * @author jianghang 2014-4-15 上午11:22:01
 * @since 5.0.7
 */
public class Case extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        Boolean existComparee = DataType.BooleanType.convertFrom(args[0]);
        Boolean existElse = DataType.BooleanType.convertFrom(args[args.length - 2]);

        DataType type = null;
        Object comparee = null;
        if (existComparee) {
            type = getArgType(args[1]);
            comparee = type.convertFrom(args[1]);
        }

        int size = args.length;
        if (existElse) {
            size -= 2;
        }

        for (int i = 2; i < size; i += 2) {
            if (existComparee) {
                Object when = type.convertFrom(args[i]);
                Object then = args[i + 1];
                if (comparee.equals(when)) {
                    return then;
                }
            } else {
                // 不存在comparee
                Boolean when = DataType.BooleanType.convertFrom(args[i]);
                if (when) {
                    return args[i + 1];
                }
            }
        }

        if (existElse) {
            return args[args.length - 1];
        } else {
            return null;
        }
    }

    @Override
    public DataType getReturnType() {
        List args = function.getArgs();
        MathLevel lastMl = null;
        MathLevel ml = null;
        // 从第三个参数开始，每次跳跃两个
        for (int i = 3; i < args.size(); i += 2) {
            if (i == args.size() - 1) {
                // 判断是否存在else分支
                boolean existElse = DataType.BooleanType.convertFrom(args.get(i - 1));
                if (!existElse) {
                    return lastMl.type;
                }
            }
            DataType argType = getArgType(args.get(i));
            if (lastMl == null) {
                lastMl = getMathLevel(argType);
            } else if (argType != lastMl.type) {
                ml = getMathLevel(argType);
                if (ml.isOther()) { // 出现非数字类型，直接返回string
                    ml.type = DataType.StringType;
                    return ml.type;
                }

                if (ml.level < lastMl.level) {
                    lastMl = ml;
                }
            }
        }

        return lastMl.type;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "CASE_WHEN" };
    }
}
