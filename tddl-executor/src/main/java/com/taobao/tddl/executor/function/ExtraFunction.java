package com.taobao.tddl.executor.function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.datatype.DataTypeUtil;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IExtraFunction;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IFunction.FunctionType;
import com.taobao.tddl.optimizer.core.expression.ISelectable;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

public abstract class ExtraFunction implements IExtraFunction {

    protected static final Logger logger = LoggerFactory.getLogger(ExtraFunction.class);
    protected IFunction           function;

    @Override
    public void setFunction(IFunction function) {
        this.function = function;
    }

    /**
     * 如果可以用db的函数，那就直接使用
     * 
     * @param function
     */
    protected abstract String getDbFunction();

    protected List getReduceArgs(IFunction func) {
        String resArgs = getDbFunction();
        Object[] obs = resArgs.split(",");
        return Arrays.asList(obs);
    }

    protected List getMapArgs(IFunction func) {
        return func.getArgs();
    }

    protected Object getArgValue(Object funcArg, IRowSet kvPair, ExecutionContext ec) {
        if (funcArg instanceof IFunction) {
            if (((IFunction) funcArg).getExtraFunction().getFunctionType().equals(FunctionType.Aggregate)) {
                // aggregate function
                return ExecUtils.getValueByIColumn(kvPair, ((ISelectable) funcArg));

            } else {
                // scalar function

                return ((ScalarFunction) ((IFunction) funcArg).getExtraFunction()).scalarCalucate(kvPair, ec);
            }
        } else if (funcArg instanceof ISelectable) {// 如果是IColumn，那么应该从输入的参数中获取对应column
            if (IColumn.STAR.equals(((ISelectable) funcArg).getColumnName())) {
                return kvPair;
            } else {
                return ExecUtils.getValueByIColumn(kvPair, ((ISelectable) funcArg));
            }
        } else if (funcArg instanceof List) {
            List newArg = new ArrayList(((List) funcArg).size());

            for (Object subArg : (List) funcArg) {
                newArg.add(this.getArgValue(subArg, kvPair, ec));
            }

            return newArg;
        } else {
            return funcArg;
        }
    }

    /**
     * 返回第一个类型的DataType
     */
    protected DataType getFirstArgType() {
        return getArgType(function.getArgs().get(0));
    }

    /**
     * 返回对应类型的DataType
     */
    protected DataType getArgType(Object arg) {
        DataType type = null;
        if (arg instanceof ISelectable) {
            type = ((ISelectable) arg).getDataType();
        }
        if (type == null) {
            type = DataTypeUtil.getTypeOfObject(arg);
        }
        return type;
    }

    /**
     * 识别多个列，调整数字类型到相同level，针对出现非数字类型直接返回string
     * 
     * <pre>
     * 1. 如果所有参数的类型相同，则返回相同的类型
     * 2. 否则均返回string类型
     * 
     * 例子：
     * 1. select greast('1',2)，返回string
     * 2. select greast(1,2)，返回long
     * </pre>
     */
    protected DataType getMixedStringType() {
        // 遍历所有的then字段的类型
        List args = function.getArgs();
        MathLevel lastMl = null;
        MathLevel ml = null;
        for (int i = 0; i < args.size(); i++) {
            DataType argType = getArgType(args.get(i));
            if (lastMl == null) {
                lastMl = getMathLevel(argType);
                if (lastMl.isOther()) { // 出现非数字类型，直接返回string
                    lastMl.type = DataType.StringType;
                    return lastMl.type;
                }
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

    /**
     * 识别多个列，调整数字类型到相同level，针对出现非数字类型按照long类型处理
     * 
     * <pre>
     * 1. 如果所有参数的类型相同，则返回相同的类型
     * 2. 否则均返回string类型
     * 
     * 例子：
     * 1. select 1+1.1，返回bigdecimal
     * 2. select 1+1，返回long
     * </pre>
     */
    protected DataType getMixedMathType() {
        List args = function.getArgs();
        MathLevel lastMl = null;
        MathLevel ml = null;
        for (int i = 0; i < args.size(); i++) {
            DataType argType = getArgType(args.get(i));
            if (lastMl == null) {
                lastMl = getMathLevel(argType);
                if (lastMl.isOther()) { // 强转成数字类型
                    lastMl.level = 3;
                    lastMl.type = DataType.LongType;
                }
            } else if (argType != lastMl.type) {
                ml = getMathLevel(argType);
                if (ml.isOther()) { // 强转成数字类型
                    ml.level = 3;
                    ml.type = DataType.LongType;
                }

                if (ml.level < lastMl.level) {
                    lastMl = ml;
                }
            }
        }
        return lastMl.type;
    }

    /**
     * 识别多个列，调整数字类型到相同level，针对出现非数字类型采用firstArgType模式
     * 
     * <pre>
     * 例子：
     * 1. select 1+1.1，返回bigdecimal
     * 2. select 1+1，返回long
     * </pre>
     */
    protected DataType getMixedFirstType() {
        List args = function.getArgs();
        MathLevel lastMl = null;
        MathLevel ml = null;
        for (int i = 0; i < args.size(); i++) {
            DataType argType = getArgType(args.get(i));
            if (lastMl == null) {
                lastMl = getMathLevel(argType);
                if (lastMl.isOther()) { // 出现非数字类型，直接返回
                    return lastMl.type;
                }
            } else if (argType != lastMl.type) {
                ml = getMathLevel(argType);
                if (ml.isOther()) { // 出现非数字类型，直接返回
                    return lastMl.type;
                }

                if (ml.level < lastMl.level) {
                    lastMl = ml;
                }
            }
        }

        return lastMl.type;
    }

    public static class MathLevel {

        public static int OTHER = 100;
        public int        level = OTHER;
        public DataType   type;

        public boolean isOther() {
            return level == OTHER;
        }
    }

    protected MathLevel getMathLevel(DataType argType) {
        MathLevel ml = new MathLevel();
        if (argType == DataType.FloatType || argType == DataType.DoubleType || argType == DataType.BigDecimalType) {
            ml.level = 1;
            ml.type = DataType.BigDecimalType;
        } else if (argType == DataType.BigIntegerType) {
            ml.level = 2;
            ml.type = DataType.BigIntegerType;
        } else if (argType == DataType.LongType || argType == DataType.IntegerType || argType == DataType.ShortType
                   || argType == DataType.BooleanType) {
            ml.level = 3;
            ml.type = DataType.LongType;
        } else {
            ml.level = MathLevel.OTHER;
            ml.type = argType;
        }

        return ml;
    }
}
