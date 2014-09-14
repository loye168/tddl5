package com.taobao.tddl.executor.function.aggregate;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.AggregateFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.expression.IFunction;

/**
 * Avg函数处理比较特殊，会将AVG转化为SUM + COUNT，拿到所有库的数据后再计算AVG
 * 
 * @since 5.0.0
 */
public class Avg extends AggregateFunction {

    private Long   count = 0L;
    private Object total = null;

    @Override
    public void serverMap(Object[] args, ExecutionContext ec) {
        count++;
        Object o = args[0];

        DataType type = getSumType();
        if (o != null) {
            if (total == null) {
                total = type.convertFrom(o);
            } else {
                total = type.getCalculator().add(total, o);
            }
        }
    }

    @Override
    public void serverReduce(Object[] args, ExecutionContext ec) {
        if (args[0] == null || args[1] == null) {
            return;
        }

        count += DataType.LongType.convertFrom(args[1]);
        Object o = args[0];
        DataType type = getSumType();
        if (total == null) {
            total = type.convertFrom(o);
        } else {
            total = type.getCalculator().add(total, o);
        }
    }

    @Override
    public String getDbFunction() {
        return bulidAvgSql(function);
    }

    private String bulidAvgSql(IFunction func) {
        String colName = func.getColumnName();
        StringBuilder sb = new StringBuilder();
        if (func.getAlias() != null) {// 如果有别名，需要和FuckAvgOptimizer中保持一致
            sb.append(func.getAlias() + "1").append(",").append(func.getAlias() + "2");
        } else {
            sb.append(colName.replace("AVG", "SUM"));
            sb.append(",").append(colName.replace("AVG", "COUNT"));
        }
        return sb.toString();
    }

    @Override
    public Object getResult() {
        DataType type = this.getReturnType();
        if (total == null) {
            return type.getCalculator().divide(0L, count);
        } else {
            return type.getCalculator().divide(total, count);
        }
    }

    @Override
    public void clear() {
        this.total = null;
        this.count = 0L;
    }

    @Override
    public DataType getReturnType() {
        return getMapReturnType();
    }

    @Override
    public DataType getMapReturnType() {
        DataType type = getFirstArgType();
        if (type == DataType.BigIntegerType) {
            // 如果是大整数，返回bigDecimal
            return DataType.BigDecimalType;
        } else {
            // 尽可能都返回为BigDecimalType，double类型容易出现精度问题，会和mysql出现误差
            // [zhuoxue.yll, 2516885.8000]
            // [zhuoxue.yll, 2516885.799999999813735485076904296875]
            // return DataType.DoubleType;
            return DataType.BigDecimalType;
        }
    }

    public DataType getSumType() {
        DataType type = getFirstArgType();
        if (type == DataType.IntegerType || type == DataType.ShortType) {
            return DataType.LongType;
        } else {
            return type;
        }
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "AVG" };
    }
}
