package com.taobao.tddl.executor.function;

import java.util.List;

import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.expression.IExtraFunction;
import com.taobao.tddl.optimizer.core.expression.IFunction.FunctionType;

/**
 * 聚合函数
 * 
 * @author jianghang 2013-11-8 下午3:45:05
 * @since 5.0.0
 */
public abstract class AggregateFunction extends ExtraFunction implements IExtraFunction {

    protected Object result;

    @Override
    public void clear() {
        result = null;
    }

    @Override
    public String getDbFunction() {
        return function.getColumnName();
    }

    @Override
    public FunctionType getFunctionType() {
        return FunctionType.Aggregate;
    }

    /**
     * 获取Map函数的返回结果
     */
    public abstract DataType getMapReturnType();

    public Object getResult() {
        return result;
    }

    /**
     * 外部执行器传递ResultSet中的row记录，进行function的map计算
     * 
     * @param kvPair
     * @throws Exception
     */
    public void serverMap(IRowSet kvPair, ExecutionContext ec) throws TddlRuntimeException {
        // 当前function需要的args 有些可能是函数，也有些是其他的一些数据
        List<Object> argsArr = getMapArgs(function);
        // 函数的input参数
        Object[] inputArg = new Object[argsArr.size()];
        int index = 0;
        for (Object funcArg : argsArr) {
            inputArg[index] = getArgValue(funcArg, kvPair, ec);
            index++;
        }
        serverMap(inputArg, ec);
    }

    // }

    /**
     * 用于标记数据应该在server端进行计算。
     * 
     * <pre>
     * 因为server有分发的过程，所以这里也模拟了分发过程。 
     * 比如：
     * 1. sum会先map到所有相关的机器上
     * 2. reduce方法内做合并
     * 
     * 比较特殊的是avg
     * 1. 首先它在map的时候，需要下面的节点统计count + sum.
     * 2. reduce则是进行avg计算的地方，进行count/sum处理
     * 
     * 因为有map/reduce模型，所有带有函数计算的执行计划都被设定为： merge { query } 结构，也就是merge下面挂query的模型，
     * 
     * </pre>
     * 
     * @param args @
     */
    public abstract void serverMap(Object[] args, ExecutionContext ec);

    /**
     * 外部执行器传递ResultSet中的row记录，进行function的reduce计算
     * 
     * @param kvPair
     * @throws Exception
     */
    public void serverReduce(IRowSet kvPair, ExecutionContext ec) throws TddlRuntimeException {

        // 函数的input参数
        List<Object> reduceArgs = this.getReduceArgs(function);

        Object[] inputArg = new Object[reduceArgs.size()];
        // 目前认为所有scalar函数可下推
        for (int i = 0; i < reduceArgs.size(); i++) {
            String name = reduceArgs.get(i).toString();
            Object val = ExecUtils.getValueByTableAndName(kvPair,
                this.function.getTableName(),
                name,
                this.function.getAlias());
            inputArg[i] = val;
        }

        serverReduce(inputArg, ec);
    }

    /**
     * 用于标记数据应该在server端进行计算。
     * 
     * <pre>
     * 因为server有分发的过程，所以这里也模拟了分发过程。 
     * 比如：
     * 1. sum会先map到所有相关的机器上
     * 2. reduce方法内做合并
     * 
     * 比较特殊的是avg
     * 1. 首先它在map的时候，需要下面的节点统计count + sum.
     * 2. reduce则是进行avg计算的地方，进行count/sum处理
     * 
     * 因为有map/reduce模型，所有带有函数计算的执行计划都被设定为： merge { query } 结构，也就是merge下面挂query的模型，
     * 
     * </pre>
     * 
     * @param args @
     */
    public abstract void serverReduce(Object[] args, ExecutionContext ec);

    public void setResult(Object result) {
        this.result = result;
    }
}
